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

package slowlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/shatteredsilicon/qan-agent/mrms"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/config"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/log"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/query"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/ssm/proto"
)

type WorkerFactory interface {
	Make(name string, config analyzer.QAN, mysqlConn mysql.Connector, mrmsMonitor mrms.Monitor) *Worker
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

func (f *RealWorkerFactory) Make(name string, config analyzer.QAN, mysqlConn mysql.Connector, mrmsMonitor mrms.Monitor) *Worker {
	return NewWorker(pct.NewLogger(f.logChan, name), config, mysqlConn, mrmsMonitor)
}

// --------------------------------------------------------------------------

type Job struct {
	Id             string
	SlowLogFile    string
	StartOffset    int64
	EndOffset      int64
	ExampleQueries bool
	RetainSlowLogs int
}

func (j *Job) String() string {
	return fmt.Sprintf("%s %d-%d", j.SlowLogFile, j.StartOffset, j.EndOffset)
}

type Worker struct {
	logger    *pct.Logger
	config    analyzer.QAN
	mysqlConn mysql.Connector
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
	resultChan      chan *report.Result
	mrms            mrms.Monitor
}

func NewWorker(logger *pct.Logger, config analyzer.QAN, mysqlConn mysql.Connector, mrmsMonitor mrms.Monitor) *Worker {
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
		mrms:            mrmsMonitor,
	}
	return w
}

func (w *Worker) Setup(interval *iter.Interval, resultChan chan *report.Result) error {
	w.logger.Debug("Setup:call")
	defer w.logger.Debug("Setup:return")
	w.logger.Debug("Setup:", interval)

	// Check if slow log rotation is enabled.
	if boolValue(w.config.SlowLogRotation) {
		// Check if max slow log size was reached.
		if interval.EndOffset >= w.config.MaxSlowLogSize {
			w.logger.Info(fmt.Sprintf("Rotating slow log: %s >= %s",
				pct.Bytes(uint64(interval.EndOffset)),
				pct.Bytes(uint64(w.config.MaxSlowLogSize))))
			// Rotating slow log requires turning off the slow_query_log,
			// hence we need to switch off the slow_query_log monitoring
			w.mrms.SwitchSlowlogCheck(w.config.UUID, false)
			// Rotate slow log.
			if err := w.rotateSlowLog(interval); err != nil {
				w.logger.Error(err)
			} else {
				// Re-enable slow_query_log monitoring
				w.mrms.SwitchSlowlogCheck(w.config.UUID, true)
			}
		}
	}

	// Create new Job.
	w.job = &Job{
		Id:             fmt.Sprintf("%d", interval.Number),
		SlowLogFile:    interval.Filename,
		StartOffset:    interval.StartOffset,
		EndOffset:      interval.EndOffset,
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

	w.status.Update(w.name, "Starting job "+w.job.Id)
	defer w.status.Update(w.name, "Idle")

	stopped := false
	w.running = true
	defer func() {
		if stopped {
			w.sync.Done()
		}
		w.running = false
	}()

	// Open the slow log file. Be sure to close it else we'll leak fd.
	file, err := os.Open(w.job.SlowLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a slow log parser and run it.  It sends log.Event via its channel.
	// Be sure to stop it when done, else we'll leak goroutines.
	result := &report.Result{}
	opts := log.Options{
		StartOffset: uint64(w.job.StartOffset),
		FilterAdminCommand: map[string]bool{
			"Binlog Dump":      true,
			"Binlog Dump GTID": true,
		},
	}
	p := w.MakeLogParser(file, opts)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				errMsg := fmt.Sprintf("Slow log parser for %s crashed: %s", w.job, err)
				w.logger.Error(errMsg)
			}
		}()
		if err := p.Start(); err != nil {
			w.logger.Warn(err)
		}
	}()
	defer p.Stop()

	// Make an event aggregate to do all the heavy lifting: fingerprint
	// queries, group, and aggregate.
	aggregator := event.NewAggregator(w.job.ExampleQueries, w.utcOffset, w.outlierTime)

	// Misc runtime meta data.
	jobSize := w.job.EndOffset - w.job.StartOffset
	runtime := time.Duration(0)
	progress := "Not started"
	rateType := ""
	rateLimit := uint(0)

	sendResult := func(a *event.Aggregator, res *report.Result) {
		r := a.Finalize()

		// The aggregator result is a map, but we need an array of classes for
		// the query report, so convert it.
		classes := make([]*event.Class, 0)
		for _, cs := range r.Class {
			for _, c := range cs {
				classes = append(classes, c)
			}
		}
		res.Global = r.Global
		res.Class = classes
		res.RateLimit = rateLimit
		w.resultChan <- res
	}

	// Do fingerprinting in a separate Go routine so we can recover in case
	// query.Fingerprint() crashes. We don't want one bad fingerprint to stop
	// parsing the entire interval. Also, we want to log crashes and hopefully
	// fix the fingerprinter.
	go w.fingerprinter()
	defer func() { w.doneChan <- true }()

	t0 := time.Now().UTC()
EVENT_LOOP:
	for e := range p.EventChan() {
		runtime = time.Now().UTC().Sub(t0)
		progress = fmt.Sprintf("%.1f%% %d/%d %d %.1fs",
			float64(e.Offset)/float64(w.job.EndOffset)*100, e.Offset, w.job.EndOffset, jobSize, runtime.Seconds())
		w.status.Update(w.name, fmt.Sprintf("Parsing %s: %s", w.job.SlowLogFile, progress))

		if e.Ts.IsZero() {
			e.Ts = time.Now()
		}

		if aggregator.ShouldFinalize(e) {
			sendResult(aggregator, result)
			aggregator = event.NewAggregator(w.job.ExampleQueries, w.utcOffset, w.outlierTime)
			result = &report.Result{
				RateLimit:  rateLimit,
				StopOffset: result.StopOffset,
			}
		}

		// Stop if Stop() called.
		select {
		case <-w.sync.StopChan:
			w.logger.Debug("Run:stop")
			stopped = true
			break EVENT_LOOP
		default:
		}

		// ignore empty query
		if len(e.Query) == 0 || e.Query == ";" {
			continue
		}

		// Stop if past file end offset. This happens often because we parse
		// only a slice of the slow log, and it's growing (if MySQL is busy),
		// so typical case is, for example, parsing from offset 100 to 5000
		// but slow log is already 7000 bytes large and growing. So the first
		// event with offset > 5000 marks the end (StopOffset) of this slice.
		if int64(e.Offset) >= w.job.EndOffset {
			result.StopOffset = int64(e.Offset)
			break EVENT_LOOP
		}

		// Stop if rate limits are mixed. This shouldn't happen. If it does,
		// another program or person might have reconfigured the rate limit.
		// We don't handle by design this because it's too much of an edge case.
		if e.RateType != "" {
			if rateType != "" {
				if rateType != e.RateType || rateLimit != e.RateLimit {
					errMsg := fmt.Sprintf("Slow log has mixed rate limits: %s/%d and %s/%d",
						rateType, rateLimit, e.RateType, e.RateLimit)
					w.logger.Warn(errMsg)
					break EVENT_LOOP
				}
			} else {
				rateType = e.RateType
				rateLimit = e.RateLimit
			}
		}

		// Fingerprint the query and add it to the event aggregator. If the
		// fingerprinter crashes, start it again and skip this event.
		var fingerprint string
		w.queryChan <- e.Query
		select {
		case fingerprint = <-w.fingerprintChan:
			// check if query should be omitted first
			if w.config.IsQueryOmitted(fingerprint) {
				break
			}

			id := query.Id(fingerprint)
			aggregator.AddEvent(e, id, fingerprint)
		case _ = <-w.errChan:
			w.logger.Warn(fmt.Sprintf("Cannot fingerprint '%s'", e.Query))
			go w.fingerprinter()
		}
	}

	// If StopOffset isn't set above it means we reached the end of the slow log
	// file. This happens if MySQL isn't busy so the slow log didn't grow any,
	// or we rotated the slow log in Setup() so we're finishing the rotated slow
	// log file. So the StopOffset is the end of the file which we're already
	// at, so use SEEK_CUR.
	if result.StopOffset == 0 {
		result.StopOffset, _ = file.Seek(0, os.SEEK_CUR)
	}

	// Finalize the global and class metrics, i.e. calculate metric stats.
	w.status.Update(w.name, "Finalizing job "+w.job.Id)
	sendResult(aggregator, result)

	// Zero the runtime for testing.
	if !w.ZeroRunTime {
		result.RunTime = time.Now().UTC().Sub(t0).Seconds()
	}

	w.logger.Info(fmt.Sprintf("Parsed %s: %s", w.job, progress))
	return nil, nil
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

func (w *Worker) SetConfig(config analyzer.QAN) {
	w.config = config
}

func (w *Worker) SetLogParser(p log.LogParser) {
	// This is just for testing, so tests can inject a parser that does
	// abnormal things like be slow, crash, etc.
	w.logParser = p
}

func (w *Worker) MakeLogParser(file *os.File, opts log.Options) log.LogParser {
	if w.logParser != nil {
		p := w.logParser
		w.logParser = nil
		return p
	}
	return log.NewSlowLogParser(file, opts)
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

func (w *Worker) rotateSlowLog(interval *iter.Interval) error {
	w.logger.Debug("rotateSlowLog:call")
	defer w.logger.Debug("rotateSlowLog:return")

	w.status.Update(w.name, "Rotating slow log")
	defer w.status.Update(w.name, "Idle")

	if err := w.mysqlConn.Connect(); err != nil {
		return err
	}
	defer w.mysqlConn.Close()

	versionStr, err := w.mysqlConn.GetGlobalVarString("version")
	if err != nil {
		return err
	}

	distro := mysql.ParseDistro(versionStr.String)
	var fastRotate bool
	if vb, err := pct.AtLeastVersion(versionStr.String, "10.4"); distro != mysql.DistroMariaDB || (err == nil && !vb) {
		fastRotate = true
	}
	if !fastRotate {
		// Stop slow log so we don't move it while MySQL is using it.
		if err := w.mysqlConn.Exec(w.config.Stop); err != nil {
			return err
		}
	}

	// Move current slow log by renaming it.
	newSlowLogFile := fmt.Sprintf("%s-%d", interval.Filename, time.Now().UTC().Unix())
	renameErr := os.Rename(interval.Filename, newSlowLogFile)

	var postErr error
	// Try to reconnect if it's not connected, because if the above
	// progress takes too long, the connection might be closed by other goroutines.
	// Which implys that we need a better management of DB connections.
	// TODO: Refactor the DB connection management
	if postErr = w.mysqlConn.Connect(); postErr == nil {
		if fastRotate {
			if err := w.mysqlConn.Exec([]string{"FLUSH NO_WRITE_TO_BINLOG SLOW LOGS"}); err != nil {
				// MySQL 5.1 support.
				postErr = w.mysqlConn.Exec([]string{"FLUSH LOGS"})
			}
		} else {
			// Re-enable slow log.
			postErr = w.mysqlConn.Exec(w.config.Start)
		}
	}

	if renameErr != nil {
		return renameErr
	}

	// Modify interval so worker parses the rest of the old slow log.
	curSlowLog := interval.Filename
	interval.Filename = newSlowLogFile
	interval.EndOffset, _ = pct.FileSize(newSlowLogFile) // todo: handle err

	if postErr != nil {
		return postErr
	}

	// Purge old slow logs.
	if !config.DefaultRemoveOldSlowLogs {
		return nil
	}
	filesFound, err := filepath.Glob(fmt.Sprintf("%s-*", curSlowLog))
	if err != nil {
		return err
	}
	if len(filesFound) <= intValue(w.config.RetainSlowLogs) {
		return nil
	}
	sort.Strings(filesFound)
	for _, f := range filesFound[:len(filesFound)-intValue(w.config.RetainSlowLogs)] {
		w.status.Update(w.name, "Removing slow log "+f)
		if err := os.Remove(f); err != nil {
			w.logger.Warn(err)
			continue
		}
		w.logger.Info("Removed old slow log " + f)
	}

	return nil
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
