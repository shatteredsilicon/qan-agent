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
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRDS "github.com/aws/aws-sdk-go/service/rds"
	"github.com/shatteredsilicon/qan-agent/agent"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/config"
	mysqlEvent "github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/log"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/query"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/util"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/qan-agent/rds"
	"github.com/shatteredsilicon/ssm/proto"
	"go4.org/sort"
)

const (
	safeGapToNextHour     = time.Minute
	maxFileRecords        = 24 * 30 // 30 days
	longQueryTimeMultiple = 2
	rateLmitMaximumTimes  = 2
	minimumLongQueryTime  = 0.001
	maxResultSeconds      = 5
)

var (
	// ErrUnknownRDSLogOutput unknown rds parameter log_output value
	ErrUnknownRDSLogOutput = errors.New("value of rds parameter log_output is unknown")
	// ErrRDSSlowlogDisabled slow log disabled error
	ErrRDSSlowlogDisabled = errors.New("slow log of rds is disabled")
	// ErrUnknownRDSSlowlogFile unknown rds slow log file value
	ErrUnknownRDSSlowlogFile = errors.New("slow log file of rds is unknown")
)

var (
	logParserOpts = log.Options{
		FilterAdminCommand: map[string]bool{
			"Binlog Dump":      true,
			"Binlog Dump GTID": true,
		},
	}
)

type WorkerFactory interface {
	Make(name string, config config.QAN, mysqlConn mysql.Connector) *Worker
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

func (f *RealWorkerFactory) Make(name string, config config.QAN, mysqlConn mysql.Connector) *Worker {
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

type fileRecord struct {
	marker       *string
	previousData []byte
}

type byFileName []string

func (f byFileName) Len() int      { return len(f) }
func (f byFileName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byFileName) Less(i, j int) bool {
	if len(f[i]) < len(f[j]) || f[i] < f[j] {
		return true
	}

	return false
}

type Worker struct {
	logger    *pct.Logger
	config    config.QAN
	mysqlConn mysql.Connector
	rds       *rds.Service
	// --
	ZeroRunTime bool // testing
	// --
	name                   string
	status                 *pct.Status
	queryChan              chan string
	fingerprintChan        chan string
	errChan                chan interface{}
	doneChan               chan bool
	oldSlowLogs            map[int]string
	job                    *Job
	sync                   *pct.SyncChan
	running                bool
	logParser              log.LogParser
	utcOffset              time.Duration
	outlierTime            float64
	LastWritten            *int64
	fileRecords            map[string]*fileRecord
	resultChan             chan *report.Result
	ignoreRateLimit        bool        // wether to ignore rate limit error or not
	rateLimitLongQueryTime float64     // stores the long_query_time value at the moment that rate limit happened
	rateLimitTimestamps    []time.Time // stores the timestamps when the historical rate limit happened
	lastStartTime          time.Time
}

func NewWorker(logger *pct.Logger, config config.QAN, mysqlConn mysql.Connector) *Worker {
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
		fileRecords:     make(map[string]*fileRecord),
	}
	return w
}

func (w *Worker) setupRDS() error {
	agentConfigFile := pct.Basedir.ConfigFile("agent")
	agentConfig := &agent.AgentConfig{}
	if _, err := pct.Basedir.ReadConfig("agent", agentConfig); err != nil {
		w.logger.Error(fmt.Sprintf("Error decoding agent config file %s: %s\n", agentConfigFile, err.Error()))
		return err
	}

	rdsSvcDetail, err := GetRDSServiceDetail(*agentConfig, w.config)
	if err != nil {
		w.logger.Error(fmt.Sprintf("Error fetching rds service detail: %s\n", err.Error()))
		return err
	}

	var creds *credentials.Credentials
	if rdsSvcDetail.AWSAccessKeyID != "" || rdsSvcDetail.AWSSecretAccessKey != "" {
		creds = credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     rdsSvcDetail.AWSAccessKeyID,
				SecretAccessKey: rdsSvcDetail.AWSSecretAccessKey,
			},
		})
	}
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

func (w *Worker) Setup(interval *iter.Interval, resultChan chan *report.Result) error {
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
	w.resultChan = resultChan
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
	if enabled.ParameterValue == nil || *enabled.ParameterValue != rds.TrueParamValue {
		return nil, ErrRDSSlowlogDisabled
	}

	slowLogFile, err := w.rds.GetParam("slow_query_log_file")
	if err != nil {
		return nil, err
	}
	if slowLogFile.ParameterValue == nil {
		return nil, ErrUnknownRDSSlowlogFile
	}

	// check if slow query log output to file or table
	logOutput, err := w.rds.GetParam("log_output")
	if err != nil {
		return nil, err
	}
	if logOutput.ParameterValue == nil {
		return nil, ErrUnknownRDSLogOutput
	}

	longQueryTime, err := w.rds.GetParam("long_query_time")
	if err != nil {
		return nil, err
	}
	if longQueryTime.ParameterValue == nil {
		w.logger.Warn("Got an empty long_query_time value")
		w.ignoreRateLimit = true
	} else {
		lqt, err := strconv.ParseFloat(*longQueryTime.ParameterValue, 64)
		if err != nil {
			w.logger.Warn("Got an invalid long_query_time value: ", *longQueryTime.ParameterValue)
			w.ignoreRateLimit = true
		} else {
			w.ignoreRateLimit = false
			if !w.hasRateLimitAlert() {
				w.rateLimitLongQueryTime = lqt
			} else if lqt >= w.adviceLongQueryTime() {
				w.rateLimitLongQueryTime = lqt
				w.rateLimitTimestamps = []time.Time{}
			}
		}
	}

	var result *report.Result
	switch *logOutput.ParameterValue {
	case rds.FILEParamValue:
		result, stopped, err = w.runFiles(*slowLogFile.ParameterValue)
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

func (w *Worker) SetConfig(config config.QAN) {
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
	return log.NewSlowLogParser(bytes.NewReader(data), opts)
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

func (w *Worker) cleanFileRecords(currentLogFile string) {
	if len(w.fileRecords) < maxFileRecords {
		return
	}

	filenames := make([]string, 0)
	for filename := range w.fileRecords {
		if filename == currentLogFile {
			continue
		}

		filenames = append(filenames, filename)
	}

	sort.Sort(byFileName(filenames))
	for i := 0; i < len(w.fileRecords)-maxFileRecords; i++ {
		delete(w.fileRecords, filenames[i])
	}
}

// orderFiles sorts slow query log file by filename ASC, meaning from
// the oldest to the latest, and it puts the current slow query log file
// to the end of the slice that is going to be returned
func (w *Worker) orderFiles(currentLogFile string, files []*awsRDS.DescribeDBLogFilesDetails) []*awsRDS.DescribeDBLogFilesDetails {
	sort.Sort(rds.ByFileName(files))
	if len(files) > 1 && filepath.Base(*files[0].LogFileName) == currentLogFile {
		files = append(files[1:], files[0])
	}

	return files
}

func (w *Worker) runFiles(rdsLogFilePath string) (*report.Result, bool, error) {
	currentLogFile := filepath.Base(rdsLogFilePath)
	defer w.cleanFileRecords(rdsLogFilePath)

	stopped := false

	startTime := time.Now().UTC()
	stY, stM, stD, stH := startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour()
	lastHour := time.Date(stY, stM, stD, stH, 0, 0, 0, time.UTC)
	if startTime.Sub(lastHour) < safeGapToNextHour {
		// log rotation just happened, we wait for next turn,
		// leave it to aws to make the rotation fully complete
		w.logger.Debug(fmt.Sprintf("Log parsing for current slow log file cancelled for this turn, because log file rotation just happened. startTime: %v", startTime))
		return nil, stopped, nil
	}

	if w.LastWritten == nil || *w.LastWritten == 0 {
		startTimeMilli := startTime.UnixMilli()
		w.LastWritten = &startTimeMilli
		return nil, stopped, nil
	}

	files, err := w.rds.GetSlowQueryLogFiles(w.LastWritten, &currentLogFile)
	if err != nil {
		w.logger.Error(fmt.Sprintf("fetching rds log files failed: %+v", err))
		return nil, stopped, err
	}

	if len(files) == 0 {
		startTimeMilli := startTime.UnixMilli()
		w.LastWritten = &startTimeMilli
		return nil, stopped, err
	}

	files = w.orderFiles(currentLogFile, files)
	for _, file := range files {
		filename := filepath.Base(*file.LogFileName)
		currentRecord, currentRecordExists := w.fileRecords[currentLogFile]
		_, fileRecordExists := w.fileRecords[filename]
		if !fileRecordExists && currentRecordExists {
			// A new rotated log file appears, transfer the record
			w.fileRecords[filename] = currentRecord
			w.fileRecords[currentLogFile] = nil
		}
	}

	runFingerPrinter := func() {
		defer func() {
			if r := recover(); r != nil {
				w.logger.Error("Fingerprinter recovered: ", r)
			}
		}()

		w.fingerprinter()
	}

	// Do fingerprinting in a separate Go routine so we can recover in case
	// query.Fingerprint() crashes. We don't want one bad fingerprint to stop
	// parsing the entire interval. Also, we want to log crashes and hopefully
	// fix the fingerprinter.
	go runFingerPrinter()
	defer func() { w.doneChan <- true }()

	var lastStartTime time.Time
	defer func() {
		if !lastStartTime.IsZero() {
			w.lastStartTime = lastStartTime
		}
	}()
	sendResult := func(result *report.Result, aggregator *mysqlEvent.Aggregator, lastWritten int64) {
		r := aggregator.Finalize()

		// The aggregator result is a map, but we need an array of classes for
		// the query report, so convert it.
		n := len(r.Class)
		classes := make([]*mysqlEvent.Class, n)
		for _, class := range r.Class {
			n-- // can't classes[--n] in Go
			classes[n] = class
		}
		result.Global = r.Global
		result.Class = classes
		if !w.lastStartTime.IsZero() {
			if w.lastStartTime.Unix() <= result.StartTime.Unix() {
				result.StartTime = result.StartTime.Add(time.Second)
			}
			w.lastStartTime = time.Time{}
		}
		result.EndTime = time.Unix(0, int64(time.Millisecond)*lastWritten)
		w.resultChan <- result
		lastStartTime = result.StartTime
	}

EVENT_LOOP:
	for _, file := range files {
		if file == nil {
			continue
		}

		filename := filepath.Base(*file.LogFileName)
		if record, ok := w.fileRecords[filename]; !ok || record == nil {
			zeroMarker := rds.ZeroMarker
			w.fileRecords[filename] = &fileRecord{
				previousData: []byte{},
				marker:       &zeroMarker,
			}
		}
		record := w.fileRecords[filename]
		for {
			// check if current log file got rotated or
			// is going to be rotated
			if filename == currentLogFile {
				tmpNow := time.Now().UTC()
				y, m, d, h := tmpNow.Year(), tmpNow.Month(), tmpNow.Day(), tmpNow.Hour()
				if stY != y || stM != m || stD != d || stH != h {
					// current log file must be rotated while we're parsing the old files,
					// leave it to the next turn
					w.logger.Debug(fmt.Sprintf("Log parsing for current slow log file cancelled for this turn, because log file rotation happened during the parsing. startTime: %v, currentTime: %v", startTime, tmpNow))
					break
				}
				nextHour := time.Date(y, m, d, h, 0, 0, 0, time.UTC).Add(time.Hour)
				if nextHour.Sub(tmpNow) < safeGapToNextHour {
					// current log file is going to be rotated soon, we leave it to the
					// next turn incase it got rotated while we are fetching the data
					w.logger.Debug(fmt.Sprintf("Log parsing for current slow log file cancelled for this turn, because log file rotation is going to happen. currentTime: %v, nextHourTime: %v", tmpNow, nextHour))
					break
				}
			}

			numberOfLines := rds.DefaultNumberOfLines
			dataOutput, err := w.rds.DownloadDBLogFilePortion(file.LogFileName, record.marker, &numberOfLines)
			if err != nil {
				w.logger.Error(fmt.Sprintf("downloading rds log file %s failed: %+v", *file.LogFileName, err))

				awsErr, ok := err.(awserr.Error)
				if !ok || w.ignoreRateLimit || w.hasRateLimitAlert() {
					break
				}

				// check if it's a throttling error
				messages := strings.ToLower(awsErr.Message())
				if !strings.Contains(messages, "rate exceed") && !strings.Contains(messages, "quota exceed") {
					break
				}

				now := time.Now().UTC()
				y, m, d := now.Date()
				if len(w.rateLimitTimestamps) > 0 &&
					(w.rateLimitTimestamps[len(w.rateLimitTimestamps)-1].Year() != y ||
						w.rateLimitTimestamps[len(w.rateLimitTimestamps)-1].Month() != m ||
						w.rateLimitTimestamps[len(w.rateLimitTimestamps)-1].Day() != d) {
					// re-initialize if the rate limit timestamps are not in the same day
					w.rateLimitTimestamps = []time.Time{}
				}

				w.rateLimitTimestamps = append(w.rateLimitTimestamps, now)
				break
			}

			shouldBreak := dataOutput.AdditionalDataPending == nil || !(*dataOutput.AdditionalDataPending) || dataOutput.Marker == nil

			var data bytes.Buffer
			data.WriteString(string(record.previousData))
			if dataOutput.LogFileData != nil {
				data.WriteString(*dataOutput.LogFileData)
			}

			completeLog, incompleteLog := util.SplitSlowLog(data.Bytes())
			// Test the incomplete log see if it can be parsed
			p := w.MakeLogParser([]byte(incompleteLog), logParserOpts)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						w.logger.Debug("Check if incomplete log is complete panic: ", r)
					}
				}()

				if err := p.Start(); err != nil {
					w.logger.Debug("Check if incomplete log is complete failed: ", err)
				}
			}()

			event := <-p.EventChan()
			if event != nil && len(event.Query) > 0 {
				completeLog = append(completeLog, incompleteLog...)
			} else {
				record.previousData = incompleteLog
			}
			p.Stop()

			// Create a slow log parser and run it.  It sends log.Event via its channel.
			// Be sure to stop it when done, else we'll leak goroutines.
			p = w.MakeLogParser([]byte(completeLog), logParserOpts)
			result := &report.Result{
				StartTime: time.Unix(0, int64(time.Millisecond)*(*w.LastWritten)),
			}
			go func() {
				defer func() {
					if err := recover(); err != nil {
						result.Error = fmt.Sprintf("Slow log parser for %s crashed: %s", w.job, err)
						w.logger.Error(result.Error)
					}
				}()

				if err := p.Start(); err != nil {
					w.logger.Warn(err)
					result.Error = err.Error()
				}
			}()

			// Misc runtime meta data.
			rateType := ""
			rateLimit := uint(0)

			// Make an event aggregate to do all the heavy lifting: fingerprint
			// queries, group, and aggregate.
			aggregator := mysqlEvent.NewAggregator(w.job.ExampleQueries, w.utcOffset, w.outlierTime)
			for event := range p.EventChan() {
				// Stop if Stop() called.
				select {
				case <-w.sync.StopChan:
					w.logger.Debug("Run:stop")
					stopped = true
					break EVENT_LOOP
				default:
				}

				lastWritten := event.Ts
				if event.Ts.IsZero() {
					// Have to set it to NOW()
					lastWritten = time.Now()
				}
				if lastWritten.Before(result.StartTime) {
					continue
				}

				if lastWritten.Sub(result.StartTime).Seconds() >= maxResultSeconds {
					// send a result in this case for record purpose
					sendResult(result, aggregator, *w.LastWritten)
					result = &report.Result{
						StartTime: time.Unix(0, int64(time.Millisecond)*(*w.LastWritten)),
					}
					aggregator = mysqlEvent.NewAggregator(w.job.ExampleQueries, w.utcOffset, w.outlierTime)
				}
				lastWrittenMilli := lastWritten.UnixMilli()
				w.LastWritten = &lastWrittenMilli

				if qTime, ok := event.TimeMetrics["Query_time"]; !ok || qTime == 0 {
					// ignore log entry that has 0 Query_time
					continue
				}

				if record.marker != nil {
					w.status.Update(w.name, fmt.Sprintf("Parsing rds slow log file %s, marker %s", *file.LogFileName, *record.marker))
				} else {
					w.status.Update(w.name, fmt.Sprintf("Parsing rds slow log file %s", *file.LogFileName))
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

				// ignore empty query
				if len(event.Query) == 0 || event.Query == ";" {
					continue
				}

				// Fingerprint the query and add it to the event aggregator. If the
				// fingerprinter crashes, start it again and skip this event.
				var fingerprint string
				w.queryChan <- event.Query
				select {
				case fingerprint = <-w.fingerprintChan:
					// check if query should be omitted first
					if w.config.IsQueryOmitted(fingerprint) {
						break
					}

					id := query.Id(fingerprint)
					aggregator.AddEvent(event, id, fingerprint)
				case _ = <-w.errChan:
					w.logger.Warn(fmt.Sprintf("Cannot fingerprint '%s'", event.Query))
					go runFingerPrinter()
				}
			}

			if record.marker != nil {
				w.status.Update(w.name, "Finalizing job "+w.job.ID+", file "+*file.LogFileName+", marker "+*record.marker)
			} else {
				w.status.Update(w.name, "Finalizing job "+w.job.ID+", file "+*file.LogFileName)
			}

			sendResult(result, aggregator, *w.LastWritten)

			record.previousData = []byte{}
			if dataOutput.Marker != nil {
				record.marker = dataOutput.Marker
			}

			// no more data in this file
			if shouldBreak {
				break
			}
		}
	}

	return nil, stopped, nil
}

func (w Worker) adviceLongQueryTime() float64 {
	if w.rateLimitLongQueryTime < minimumLongQueryTime {
		return minimumLongQueryTime
	} else {
		return w.rateLimitLongQueryTime * 2
	}
}

// Messages returns all messages in response to Messages command
func (w Worker) Messages() []proto.Message {
	if w.hasRateLimitAlert() {
		adviceLongQueryTime := w.adviceLongQueryTime()
		return []proto.Message{
			{
				Content: fmt.Sprintf("Throttling encountered while harvesting slow query log. Current long_query_time = %s, consider increasing to %s", strconv.FormatFloat(w.rateLimitLongQueryTime, 'f', -1, 64), strconv.FormatFloat(adviceLongQueryTime, 'f', -1, 64)),
			},
		}
	}

	return []proto.Message{}
}

func (w Worker) hasRateLimitAlert() bool {
	return len(w.rateLimitTimestamps) >= rateLmitMaximumTimes
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
