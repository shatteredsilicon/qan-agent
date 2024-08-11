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

package mysql

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/mrms"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/util"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/worker"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/worker/rdsslowlog"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/qan-agent/ticker"
	"github.com/shatteredsilicon/ssm/proto"
)

const MIN_SLOWLOG_ROTATION_SIZE int64 = 4096

// --------------------------------------------------------------------------

type RealAnalyzer struct {
	logger    *pct.Logger
	config    analyzer.QAN
	iter      iter.IntervalIter
	mysqlConn mysql.Connector
	mrms      mrms.Monitor
	mrmsChan  chan interface{}
	worker    worker.Worker
	clock     ticker.Manager
	spool     data.Spooler
	// --
	name                string
	mysqlConfiguredChan chan bool
	workerDoneChan      chan *iter.Interval
	status              *pct.Status
	closeChan           chan struct{}
	runWg               *sync.WaitGroup
	running             bool
	mux                 *sync.RWMutex
	start               []string
	stop                []string
}

func NewRealAnalyzer(
	logger *pct.Logger,
	config analyzer.QAN,
	it iter.IntervalIter,
	mysqlConn mysql.Connector,
	mrmsChan chan interface{},
	worker worker.Worker,
	clock ticker.Manager,
	spool data.Spooler,
) *RealAnalyzer {
	// Create what we need
	name := logger.Service()

	// Return analyzer instance
	a := &RealAnalyzer{
		logger:    logger,
		config:    config,
		iter:      it,
		mysqlConn: mysqlConn,
		mrmsChan:  mrmsChan,
		worker:    worker,
		clock:     clock,
		spool:     spool,
		// --
		name:                name,
		mysqlConfiguredChan: make(chan bool), // note: this channel can't be buffered
		workerDoneChan:      make(chan *iter.Interval, 1),
		status:              pct.NewStatus([]string{name, name + "-last-interval", name + "-next-interval"}),
		mux:                 &sync.RWMutex{},
	}
	return a
}

func (a *RealAnalyzer) String() string {
	return a.name
}

func (a *RealAnalyzer) Start() error {
	a.logger.Debug("Start:call")
	defer a.logger.Debug("Start:return")
	a.mux.Lock()
	defer a.mux.Unlock()
	if a.running {
		return nil
	}

	a.closeChan = make(chan struct{})
	a.runWg = &sync.WaitGroup{}
	a.runWg.Add(1)
	go a.run()
	a.running = true
	return nil
}

func (a *RealAnalyzer) Stop() error {
	a.logger.Debug("Stop:call")
	defer a.logger.Debug("Stop:return")
	a.mux.Lock()
	defer a.mux.Unlock()
	if !a.running {
		return nil
	}

	close(a.closeChan)
	a.runWg.Wait()
	a.running = false
	return nil
}

func (a *RealAnalyzer) Status() map[string]string {
	a.mux.RLock()
	defer a.mux.RUnlock()
	if a.running {
		a.status.Update(a.name+"-next-interval", fmt.Sprintf("%.1fs", a.clock.ETA(a.iter.TickChan())))
	} else {
		a.status.Update(a.name+"-next-interval", "")
	}
	return a.status.Merge(a.worker.Status())
}

func (a *RealAnalyzer) Config() analyzer.QAN {
	return a.config
}

func (a *RealAnalyzer) SetConfig(c analyzer.QAN) {
	a.config = c
}

func (m *RealAnalyzer) GetDefaults(uuid string) map[string]interface{} {
	return map[string]interface{}{}
}

// Messages returns all messages in response to Messages command
func (a *RealAnalyzer) Messages() []proto.Message {
	if a.worker == nil {
		return []proto.Message{}
	}

	if worker, ok := a.worker.(*rdsslowlog.Worker); ok {
		return worker.Messages()
	}

	return []proto.Message{}
}

// --------------------------------------------------------------------------

// Disable Percona Server slow log rotation and handle internally using the
// max_slowlog_size value. The slow log worker must rotate slow logs by itself
// to ensure full and proper parsing across rotations.
func (a *RealAnalyzer) TakeOverPerconaServerRotation() error {
	a.logger.Debug("TakeOverPerconaServerRotation:call")
	defer a.logger.Debug("TakeOverPerconaServerRotation:return")

	// If slow log rotation is disabled, don't take over Percona Server slow log rotation.
	if !boolValue(a.config.SlowLogRotation) {
		return nil
	}

	// max_slowlog_size: https://www.percona.com/doc/percona-server/LATEST/flexibility/slowlog_rotation.html#max_slowlog_size
	maxSlowLogSizeNullInt64, err := a.mysqlConn.GetGlobalVarInteger("max_slowlog_size")
	if err != nil {
		return err
	}
	maxSlowLogSize := maxSlowLogSizeNullInt64.Int64
	if maxSlowLogSize == 0 {
		return nil
	}

	// Slow log rotation is only activated if max_slowlog_size >= 4096.
	// https://www.percona.com/doc/percona-server/LATEST/flexibility/slowlog_rotation.html#max_slowlog_size
	if maxSlowLogSize >= MIN_SLOWLOG_ROTATION_SIZE {
		a.logger.Info("Taking over Percona Server slow log rotation, max_slowlog_size:", maxSlowLogSize)
		a.config.MaxSlowLogSize = maxSlowLogSize
		disablePSrotation := []string{
			"SET GLOBAL max_slowlog_size = 0",
		}
		if err := a.mysqlConn.Exec(disablePSrotation); err != nil {
			return err
		}
	}

	return nil
}

func (a *RealAnalyzer) setMySQLConfig() error {
	a.logger.Debug("setMySQLConfig:call")
	defer a.logger.Debug("setMySQLConfig:return")

	start, stop, err := util.GetMySQLConfig(a.config)
	if err != nil {
		return err
	}

	if a.config.CollectFrom == "slowlog" && !boolValue(a.config.SlowLogManuallyOFF) {
		// if it's slowlog harvesting and @@slow_query_log
		// is not set to OFF manually during the QAN runtime,
		// we should set slow_query_log ON automatically
		start = append(start, "SET GLOBAL slow_query_log=ON")
	}

	a.config.Start = start
	a.config.Stop = stop

	return nil
}

func (a *RealAnalyzer) checkSlowLogManuallyOFF() error {
	dbSlowLogBool, err := a.mysqlConn.GetGlobalVarBoolean("slow_query_log")
	if err != nil {
		return err
	}
	if !dbSlowLogBool.Bool || !boolValue(a.config.SlowLogManuallyOFF) {
		return nil
	}

	manuallyOFF := false
	a.config.SlowLogManuallyOFF = &manuallyOFF

	return a.writeConfig()
}

func (a *RealAnalyzer) writeConfig() error {
	configCopy := a.config
	configCopy.Start = nil
	configCopy.Stop = nil
	if err := pct.Basedir.WriteConfig(
		fmt.Sprintf("qan-%s", a.config.UUID),
		configCopy,
	); err != nil {
		return fmt.Errorf("failed to write qan config %+v to disk: %s", configCopy, err)
	}

	return nil
}

func (a *RealAnalyzer) configureMySQL(action string, tryLimit int) {
	a.logger.Debug("configureMySQL:" + action + ":call")
	defer func() {
		select {
		case a.mysqlConfiguredChan <- true:
		case <-a.closeChan:
		}
		a.logger.Debug("configureMySQL:" + action + ":stop")
	}()

	var lastErr error
	try := 0
	for (tryLimit == 0) || (try < tryLimit) {
		if lastErr != nil {
			a.logger.Warn(lastErr.Error())
			a.status.Update(a.name, lastErr.Error())
			a.mysqlConn.Close()
			lastErr = nil
		}

		// Wait after first try because something isn't working.
		try++
		if try > 1 {
			select {
			case <-a.closeChan:
				return
			case <-time.After(1 * time.Second):
			}
		}

		a.logger.Debug("configureMySQL:" + action + ":connect")

		if err := a.mysqlConn.Connect(); err != nil {
			lastErr = err
			continue
		}

		if err := a.TakeOverPerconaServerRotation(); err != nil {
			lastErr = fmt.Errorf("cannot takeover slow log rotation: %s", err)
			continue
		}

		if err := a.checkSlowLogManuallyOFF(); err != nil {
			lastErr = fmt.Errorf("cannot check slow log manually off: %s", err)
			continue
		}

		if err := a.setMySQLConfig(); err != nil {
			lastErr = fmt.Errorf("cannot detect how to configure MySQL: %s", err)
			continue
		}
		a.worker.SetConfig(a.config)

		a.logger.Debug("configureMySQL:" + action + ":exec " + action + " queries")

		var queries []string
		switch action {
		case "start":
			queries = a.config.Start
		case "stop":
			queries = a.config.Stop
		default:
			panic("Invalid action in call to qan.Analyzer.configureMySQL: " + action)
		}
		if err := a.mysqlConn.Exec(queries); err != nil {
			lastErr = fmt.Errorf("error configuring MySQL: %s", err)
			continue
		}

		// Success
		a.logger.Debug("configureMySQL:" + action + ":configured")
		a.mysqlConn.Close()
		break
	}
}

func (a *RealAnalyzer) run() {
	defer a.runWg.Done()

	a.logger.Debug("run:call")
	defer a.logger.Debug("run:return")

	mysqlConfigured := false
	go a.configureMySQL("start", 0) // try forever

	defer func() {
		a.logger.Info("Stopping")

		a.status.Update(a.name, "Stopping worker")
		a.worker.Stop()

		if mysqlConfigured {
			a.status.Update(a.name, "Stopping interval iter")
			a.iter.Stop()

			a.status.Update(a.name, "Stopping QAN on MySQL")
			a.configureMySQL("stop", 1) // try once
		}

		if err := recover(); err != nil {
			a.logger.Error("QAN crashed: ", err)
			a.status.Update(a.name, "Crashed")
		} else {
			a.status.Update(a.name, "Stopped")
			a.logger.Info("Stopped")
		}
	}()

	workerRunning := false
	lastTs := time.Time{}
	currentInterval := &iter.Interval{}
	for {
		a.logger.Debug("run:idle")
		if mysqlConfigured {
			if workerRunning {
				a.status.Update(a.name, "Running")
			} else {
				a.status.Update(a.name, "Idle")
			}
		} else {
			a.status.Update(a.name, "Configuring MySQL")
		}

		select {
		case interval := <-a.iter.IntervalChan():
			if !mysqlConfigured {
				a.logger.Debug(fmt.Sprintf("run:interval:%d:skip (mysql not configured)", interval.Number))
				continue
			}

			if workerRunning {
				a.logger.Warn(fmt.Sprintf("Skipping interval '%s' because interval '%s' is still being parsed",
					interval, currentInterval))
				continue
			}

			a.status.Update(a.name, fmt.Sprintf("Starting interval '%s'", interval))
			a.logger.Debug(fmt.Sprintf("run:interval:%s", interval))
			currentInterval = interval

			// Run the worker, timing it, make a report from its results, spool
			// the report. When done the interval is returned on workerDoneChan.
			go a.runWorker(interval)
			workerRunning = true
		case interval := <-a.workerDoneChan:
			a.logger.Debug("run:worker:done")
			a.status.Update(a.name, fmt.Sprintf("Cleaning up after interval '%s'", interval))
			workerRunning = false

			if interval.StartTime.After(lastTs) {
				t0 := interval.StartTime.Format("2006-01-02 15:04:05")
				if a.config.CollectFrom == "slowlog" || a.config.CollectFrom == "rds-slowlog" {
					t1 := interval.StopTime.Format("15:04:05 MST")
					a.status.Update(a.name+"-last-interval", fmt.Sprintf("%s to %s", t0, t1))
				} else {
					a.status.Update(a.name+"-last-interval", fmt.Sprintf("%s", t0))
				}
				lastTs = interval.StartTime
			}
		case mysqlConfigured = <-a.mysqlConfiguredChan:
			a.logger.Debug("run:mysql:configured")
			// Start the IntervalIter once MySQL has been configured.
			// This avoids no data or partial data, e.g. slow log verbosity
			// not set yet.
			a.iter.Start()

			// If the next interval is more than 1 minute in the future,
			// simulate a clock tick now to start the iter early. For example,
			// if the interval is 5m and it's currently 01:00, the next interval
			// starts in 4m and stops in 9m, so data won't be reported for about
			// 10m. Instead, tick now so start interval=01:00 and end interval
			// =05:00 and data is reported in about 6m.
			tickChan := a.iter.TickChan()
			t := a.clock.ETA(tickChan)
			if t > 60 {
				began := ticker.Began(a.config.Interval, uint(time.Now().UTC().Unix()))
				a.logger.Info("First interval began at", began)
				tickChan <- began
			} else {
				a.logger.Info(fmt.Sprintf("First interval begins in %.1f seconds", t))
			}
		case data := <-a.mrmsChan:
			// Currently there are only 2 possible data types
			// come from this channel:
			// 1 - monitored server restart (proto.Instance)
			// 2 - MySQL/MariaDB slow log changed (bool)
			var SlowLogON, isSlowLogData bool
			if data != nil {
				SlowLogON, isSlowLogData = data.(bool)
			}

			if isSlowLogData {
				slowlogOFF := !SlowLogON
				a.config.SlowLogManuallyOFF = &slowlogOFF
				if err := a.writeConfig(); err != nil {
					a.logger.Error("Failed to write qan config %+v to disk: ", err)
				} else if slowlogOFF {
					a.logger.Warn("slow_query_log turned off during runtime")
				}
			} else {
				a.logger.Debug("run:mysql:restart")
				// If MySQL is not configured, then configureMySQL() should already
				// be running, trying to configure it. Else, we need to run
				// configureMySQL again.
				if mysqlConfigured {
					mysqlConfigured = false
					a.iter.Stop()
					go a.configureMySQL("start", 0) // try forever
				}
			}
		case <-a.closeChan:
			a.logger.Debug("run:stop")
			return
		}
	}
}

func (a *RealAnalyzer) runWorker(interval *iter.Interval) {
	a.logger.Debug(fmt.Sprintf("runWorker:call:%d", interval.Number))
	defer func() {
		if err := recover(); err != nil {
			errMsg := fmt.Sprintf(a.name+"-worker crashed: '%s': %s", interval, err)
			log.Println(errMsg)
			debug.PrintStack()
			a.logger.Error(errMsg)
		}
		a.workerDoneChan <- interval
		a.logger.Debug(fmt.Sprintf("runWorker:return:%d", interval.Number))
	}()

	var resultChan chan *report.Result
	if a.config.CollectFrom == "rds-slowlog" || a.config.CollectFrom == "slowlog" {
		resultChan = make(chan *report.Result)
	}

	// Let worker do whatever it needs before it starts processing
	// the interval. This mostly makes testing easier.
	if err := a.worker.Setup(interval, resultChan); err != nil {
		a.logger.Warn(err)
		return
	}

	// Let worker do whatever it needs after processing the interval.
	// This mostly makes testing easier.
	defer func() {
		if err := a.worker.Cleanup(); err != nil {
			a.logger.Warn(err)
		}
	}()

	t0 := time.Now()
	if a.config.CollectFrom == "rds-slowlog" || a.config.CollectFrom == "slowlog" {
		go func() {
			defer func() {
				close(resultChan)

				if r := recover(); r != nil {
					a.logger.Error("a.worker.Run panic: ", r)
				}
			}()

			_, err := a.worker.Run()
			if err != nil {
				a.logger.Error("a.worker.Run error: ", err)
			}
		}()

		makeReport := func(t0, t1 time.Time, result *report.Result) {
			if t1.Sub(t0).Seconds() < 1 {
				t1 = t0.Add(time.Second)
			}
			rep := report.MakeReport(a.config.QAN, t0, t1, interval, result, a.logger)
			if err := a.spool.Write("qan", rep); err != nil {
				a.logger.Warn("Lost report:", err)
			}
		}

		startEndTime := func(t0 time.Time, t1 time.Time, result report.Result) (time.Time, time.Time) {
			if !result.StartTime.IsZero() {
				t0 = result.StartTime
			}
			if !result.EndTime.IsZero() {
				t1 = result.EndTime
			}
			return t0, t1
		}

		result := report.Result{}
		for res := range resultChan {
			if res == nil || len(res.Class) == 0 {
				continue
			}

			newResult := *res
			if res.StartTime.IsZero() || result.StartTime.IsZero() || res.StartTime.Unix() <= result.StartTime.Unix() {
				result = report.MergeResult(result, *res)
				newResult = report.Result{}
			}

			t1 := time.Now()
			t0, t1 = startEndTime(t0, t1, result)
			if result.StartTime.IsZero() && t1.Sub(t0).Seconds() < 1 {
				continue
			}

			makeReport(t0, t1, &result)

			result = newResult
			t0 = t1
		}

		if len(result.Class) > 0 {
			t1 := time.Now()
			t0, t1 = startEndTime(t0, t1, result)
			makeReport(t0, t1, &result)
		}
	} else {
		// Run the worker to process the interval.
		result, err := a.worker.Run()
		t1 := time.Now()
		if err != nil {
			a.logger.Error(err)
			return
		}

		if result == nil {
			return
		}
		result.RunTime = t1.Sub(t0).Seconds()

		// Translate the results into a report and spool.
		// NOTE: "qan" here is correct; do not use a.name.
		report := report.MakeReport(a.config.QAN, interval.StartTime, interval.StopTime, interval, result, a.logger)
		if err := a.spool.Write("qan", report); err != nil {
			a.logger.Warn("Lost report:", err)
		}
	}
}

// boolValue returns the value of the bool pointer passed in or
// false if the pointer is nil.
func boolValue(v *bool) bool {
	if v != nil {
		return *v
	}
	return false
}
