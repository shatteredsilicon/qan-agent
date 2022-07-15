/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package rdsslowlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRDS "github.com/aws/aws-sdk-go/service/rds"
	"github.com/percona/go-mysql/event"
	"github.com/percona/go-mysql/log"
	"github.com/percona/go-mysql/query"
	"github.com/shatteredsilicon/qan-agent/agent"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/qan-agent/rds"
	"github.com/shatteredsilicon/ssm/proto"
	pc "github.com/shatteredsilicon/ssm/proto/config"
)

var (
	// ErrUnknownRDSLogOutput unknown rds parameter log_output value
	ErrUnknownRDSLogOutput = errors.New("value of rds parameter log_output is unknown")
	// ErrRDSSlowlogDisabled slow log disabled error
	ErrRDSSlowlogDisabled = errors.New("slow log of rds is disabled")
)

type WorkerFactory interface {
	Make(name string, config pc.QAN, mysqlConn mysql.Connector) *Worker
}

type RealWorkerFactory struct {
	logChan chan proto.LogEntry
}

func NewRealWorkerFactory(logChan chan proto.LogEntry) *RealWorkerFactory {
	f := &RealWorkerFactory{
		logChan: logChan,
	}
	return f
}

func (f *RealWorkerFactory) Make(name string, config pc.QAN, mysqlConn mysql.Connector) *Worker {
	return NewWorker(pct.NewLogger(f.logChan, name), config, mysqlConn)
}

// --------------------------------------------------------------------------

type Job struct {
	ID             string
	RunTime        time.Duration
	ExampleQueries bool
	RetainSlowLogs int
}

func (j *Job) String() string {
	return fmt.Sprintf("%s", j.ID)
}

type Worker struct {
	logger    *pct.Logger
	config    pc.QAN
	mysqlConn mysql.Connector
	rds       *rds.Service
	// --
	ZeroRunTime bool // testing
	// --
	name            string
	status          *pct.Status
	queryChan       chan string
	fingerprintChan chan string
	errChan         chan interface{}
	doneChan        chan bool
	oldSlowLogs     map[int]string
	job             *Job
	sync            *pct.SyncChan
	running         bool
	logParser       log.LogParser
	utcOffset       time.Duration
	outlierTime     float64
	LastWritten     *int64
}

func NewWorker(logger *pct.Logger, config pc.QAN, mysqlConn mysql.Connector) *Worker {
	// By default replace numbers in words with ?
	query.ReplaceNumbersInWords = true

	// Get the UTC offset in hours for the system time zone, not the current
	// time zone, because slow log timestamps are former.
	_, utcOffset, err := mysqlConn.UTCOffset()
	if err != nil {
		logger.Warn(err.Error())
	}

	name := logger.Service()

	if err = mysqlConn.Connect(); err != nil {
		logger.Error(err.Error())
	}
	defer mysqlConn.Close()

	outlierTime, err := mysqlConn.GetGlobalVarNumeric("slow_query_log_always_write_time")
	if err != nil {
		logger.Error(err.Error())
	}
	w := &Worker{
		logger:    logger,
		config:    config,
		mysqlConn: mysqlConn,
		// --
		name:            name,
		status:          pct.NewStatus([]string{name}),
		queryChan:       make(chan string, 1),
		fingerprintChan: make(chan string, 1),
		errChan:         make(chan interface{}, 1),
		doneChan:        make(chan bool, 1),
		oldSlowLogs:     make(map[int]string),
		sync:            pct.NewSyncChan(),
		utcOffset:       utcOffset,
		outlierTime:     outlierTime.Float64,
	}
	return w
}

func (w *Worker) setupRDS() error {
	agentConfigFile := pct.Basedir.ConfigFile("agent")
	agentCfgBytes, err := agent.LoadConfig()
	if err != nil {
		w.logger.Error(err.Error())
		return err
	}
	agentConfig := &agent.AgentConfig{}
	if err := json.Unmarshal(agentCfgBytes, agentConfig); err != nil {
		w.logger.Error(fmt.Sprintf("Error decoding agent config file %s: %s\n", agentConfigFile, err.Error()))
		return err
	}

	rdsSvcDetail, err := GetRDSServiceDetail(*agentConfig, w.config)
	if err != nil {
		w.logger.Error(fmt.Sprintf("Error fetching rds service detail: %s\n", err.Error()))
		return err
	}

	creds := credentials.NewCredentials(&credentials.StaticProvider{
		Value: credentials.Value{
			AccessKeyID:     rdsSvcDetail.AWSAccessKeyID,
			SecretAccessKey: rdsSvcDetail.AWSSecretAccessKey,
		},
	})
	awsConfig := &aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   creds,
		Region:                        aws.String(rdsSvcDetail.Region),
	}
	s, err := session.NewSession(awsConfig)
	if err != nil {
		w.logger.Error(fmt.Sprintf("Error initializing aws session %s\n", err.Error()))
		return err
	}

	w.rds = rds.NewService(awsRDS.New(s), rdsSvcDetail.Instance)
	return nil
}

func (w *Worker) Setup(interval *iter.Interval) error {
	w.logger.Debug("Setup:call")
	defer w.logger.Debug("Setup:return")
	w.logger.Debug("Setup:", interval)

	if err := w.setupRDS(); err != nil {
		return err
	}

	workerRunTime := time.Duration(uint(float64(w.config.Interval)*0.9)) * time.Second // 90% of interval
	// Create new Job.
	w.job = &Job{
		ID:             fmt.Sprintf("%d", interval.Number),
		RunTime:        workerRunTime,
		ExampleQueries: boolValue(w.config.ExampleQueries),
		RetainSlowLogs: intValue(w.config.RetainSlowLogs),
	}

	w.logger.Debug("Setup:", w.job)
	return nil
}

func (w *Worker) Run() (*report.Result, error) {
	w.logger.Debug("Run:call")
	defer w.logger.Debug("Run:return")

	w.status.Update(w.name, "Starting job "+w.job.ID)
	defer w.status.Update(w.name, "Idle")

	stopped := false
	w.running = true
	defer func() {
		if stopped {
			w.sync.Done()
		}
		w.running = false
	}()

	// check if slow query log is enabled
	enabled, err := w.rds.GetParam("slow_query_log")
	if err != nil {
		return nil, err
	}
	if *enabled.ParameterValue != rds.TrueParamValue {
		return nil, ErrRDSSlowlogDisabled
	}

	slowLogFile, err := w.rds.GetParam("slow_query_log_file")
	if err != nil {
		return nil, err
	}

	// check if slow query log output to file or table
	logOutput, err := w.rds.GetParam("log_output")
	if err != nil {
		return nil, err
	}

	var result *report.Result
	switch *logOutput.ParameterValue {
	case rds.FILEParamValue:
		result, stopped, err = w.runFiles(path.Base(*slowLogFile.ParameterValue))
	default:
		return nil, ErrUnknownRDSLogOutput
	}

	return result, err
}

func (w *Worker) Stop() error {
	w.logger.Debug("Stop:call")
	defer w.logger.Debug("Stop:return")
	if w.running {
		w.sync.Stop()
		w.sync.Wait()
	}
	return nil
}

func (w *Worker) Cleanup() error {
	w.logger.Debug("Cleanup:call")
	defer w.logger.Debug("Cleanup:return")
	return nil
}

func (w *Worker) Status() map[string]string {
	return w.status.All()
}

func (w *Worker) SetConfig(config pc.QAN) {
	w.config = config
}

func (w *Worker) SetLogParser(p log.LogParser) {
	// This is just for testing, so tests can inject a parser that does
	// abnormal things like be slow, crash, etc.
	w.logParser = p
}

func (w *Worker) MakeLogParser(data []byte, opts log.Options) log.LogParser {
	if w.logParser != nil {
		p := w.logParser
		w.logParser = nil
		return p
	}
	return NewSlowLogParser(bytes.NewReader(data), opts)
}

// --------------------------------------------------------------------------

func (w *Worker) fingerprinter() {
	w.logger.Debug("fingerprinter:call")
	defer w.logger.Debug("fingerprinter:return")
	defer func() {
		if err := recover(); err != nil {
			w.errChan <- err
		}
	}()
	for {
		select {
		case q := <-w.queryChan:
			f := query.Fingerprint(q)
			w.fingerprintChan <- f
		case <-w.doneChan:
			return
		}
	}
}

func (w *Worker) runFiles(filePrefix string) (*report.Result, bool, error) {
	stopped := false

	now := time.Now().Unix()
	files, err := w.rds.GetLogFiles(w.LastWritten)
	if err != nil {
		w.logger.Error(fmt.Sprintf("fetching rds log files failed: %+v", err))
		return nil, stopped, err
	}

	// Make an event aggregate to do all the heavy lifting: fingerprint
	// queries, group, and aggregate.
	aggregator := event.NewAggregator(w.job.ExampleQueries, w.utcOffset, w.outlierTime)

	result := &report.Result{}

EVENT_LOOP:
	for _, file := range files {
		if file == nil || !strings.Contains(*file.LogFileName, filePrefix) {
			continue
		}

		data, err := w.rds.DownloadLogFile(*file.LogFileName)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == awsRDS.ErrCodeDBLogFileNotFoundFault {
					continue
				}
			}

			w.logger.Error(fmt.Sprintf("downloading rds log file %s failed: %+v", *file.LogFileName, err))
			return nil, stopped, err
		}

		// Create a slow log parser and run it.  It sends log.Event via its channel.
		// Be sure to stop it when done, else we'll leak goroutines.
		opts := log.Options{
			FilterAdminCommand: map[string]bool{
				"Binlog Dump":      true,
				"Binlog Dump GTID": true,
			},
		}
		p := w.MakeLogParser(data, opts)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					errMsg := fmt.Sprintf("Slow log parser for %s crashed: %s", w.job, err)
					w.logger.Error(errMsg)
					result.Error = errMsg
				}
			}()
			if err := p.Start(); err != nil {
				w.logger.Warn(err)
				result.Error = err.Error()
			}
		}()
		defer p.Stop()

		// Misc runtime meta data.
		progress := "Not started"
		rateType := ""
		rateLimit := uint(0)

		// Do fingerprinting in a separate Go routine so we can recover in case
		// query.Fingerprint() crashes. We don't want one bad fingerprint to stop
		// parsing the entire interval. Also, we want to log crashes and hopefully
		// fix the fingerprinter.
		go w.fingerprinter()
		defer func() { w.doneChan <- true }()

		for event := range p.EventChan() {
			w.status.Update(w.name, fmt.Sprintf("Parsing %s: %s", *file.LogFileName, progress))

			// Stop if Stop() called.
			select {
			case <-w.sync.StopChan:
				w.logger.Debug("Run:stop")
				stopped = true
				break EVENT_LOOP
			default:
			}

			// Stop if rate limits are mixed. This shouldn't happen. If it does,
			// another program or person might have reconfigured the rate limit.
			// We don't handle by design this because it's too much of an edge case.
			if event.RateType != "" {
				if rateType != "" {
					if rateType != event.RateType || rateLimit != event.RateLimit {
						errMsg := fmt.Sprintf("Slow log has mixed rate limits: %s/%d and %s/%d",
							rateType, rateLimit, event.RateType, event.RateLimit)
						w.logger.Warn(errMsg)
						result.Error = errMsg
						break EVENT_LOOP
					}
				} else {
					rateType = event.RateType
					rateLimit = event.RateLimit
				}
			}

			if event.Ts.Unix() > now || (w.LastWritten != nil && *w.LastWritten > 0 && event.Ts.Unix() <= *w.LastWritten) {
				continue
			}

			// Fingerprint the query and add it to the event aggregator. If the
			// fingerprinter crashes, start it again and skip this event.
			var fingerprint string
			w.queryChan <- event.Query
			select {
			case fingerprint = <-w.fingerprintChan:
				id := query.Id(fingerprint)
				aggregator.AddEvent(event, id, fingerprint)
			case _ = <-w.errChan:
				w.logger.Warn(fmt.Sprintf("Cannot fingerprint '%s'", event.Query))
				go w.fingerprinter()
			}
		}
	}

	if stopped {
		return nil, stopped, nil
	}
	w.LastWritten = &now

	// Finalize the global and class metrics, i.e. calculate metric stats.
	w.status.Update(w.name, "Finalizing job "+w.job.ID)
	r := aggregator.Finalize()

	// The aggregator result is a map, but we need an array of classes for
	// the query report, so convert it.
	n := len(r.Class)
	classes := make([]*event.Class, n)
	for _, class := range r.Class {
		n-- // can't classes[--n] in Go
		classes[n] = class
	}
	result.Global = r.Global
	result.Class = classes

	return result, stopped, nil
}

// boolValue returns the value of the bool pointer passed in or
// false if the pointer is nil.
func boolValue(v *bool) bool {
	if v != nil {
		return *v
	}
	return false
}

// intValue returns the value of the int pointer passed in or
// 0 if the pointer is nil.
func intValue(v *int) int {
	if v != nil {
		return *v
	}
	return 0
}
