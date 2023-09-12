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

package mysql_test

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/shatteredsilicon/qan-agent/instance"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	mysqlAnalyzer "github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/config"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/worker/slowlog"
	"github.com/shatteredsilicon/qan-agent/test"
	"github.com/shatteredsilicon/qan-agent/test/mock"
	"github.com/shatteredsilicon/qan-agent/test/mock/interval_iter"
	"github.com/shatteredsilicon/qan-agent/test/mock/qan_worker"
	. "github.com/shatteredsilicon/qan-agent/test/rootdir"
	"github.com/shatteredsilicon/ssm/proto"
	pc "github.com/shatteredsilicon/ssm/proto/config"
	qp "github.com/shatteredsilicon/ssm/proto/qan"
	"github.com/stretchr/testify/require"
	. "gopkg.in/check.v1"
)

var inputDir = RootDir() + "/test/slow-logs/"

type AnalyzerTestSuite struct {
	nullmysql     *mock.NullMySQL
	iter          *interval_iter.Iter
	spool         *mock.Spooler
	clock         *mock.Clock
	api           *mock.API
	worker        *qan_worker.QanWorker
	restartChan   chan proto.Instance
	logChan       chan proto.LogEntry
	logger        *pct.Logger
	intervalChan  chan *iter.Interval
	dataChan      chan interface{}
	tmpDir        string
	configDir     string
	im            *instance.Repo
	mysqlUUID     string
	mysqlInstance proto.Instance
	config        config.QAN
}

var _ = Suite(&AnalyzerTestSuite{})

// The highest possible value max_slowlog_size can be set to (from Percona Server documentation)
const MAX_SLOW_LOG_SIZE int64 = 1073741824

func (s *AnalyzerTestSuite) SetUpSuite(t *C) {
	s.nullmysql = mock.NewNullMySQL()

	s.logChan = make(chan proto.LogEntry, 1000)
	s.logger = pct.NewLogger(s.logChan, "qan-test")

	s.intervalChan = make(chan *iter.Interval, 1)

	s.iter = interval_iter.NewIter(s.intervalChan)

	s.dataChan = make(chan interface{}, 1)
	s.spool = mock.NewSpooler(s.dataChan)

	var err error
	s.tmpDir, err = ioutil.TempDir("/tmp", "agent-test")
	t.Assert(err, IsNil)

	if err := pct.Basedir.Init(s.tmpDir); err != nil {
		t.Fatal(err)
	}
	s.configDir = pct.Basedir.Dir("config")

	links := map[string]string{
		"agents":    "/agents",
		"instances": "/instances",
	}
	s.api = mock.NewAPI("localhost", "http://localhost", "212", links)

	s.im = instance.NewRepo(pct.NewLogger(s.logChan, "analizer-test"), s.configDir, s.api)
	s.mysqlUUID = "313"
	s.mysqlInstance = proto.Instance{
		Subsystem: "mysql",
		UUID:      s.mysqlUUID,
		Name:      "db01",
		DSN:       "user:pass@tcp(localhost)/",
	}

	err = s.im.Init()
	t.Assert(err, IsNil)

	err = s.im.Add(s.mysqlInstance, true)
	t.Assert(err, IsNil)

	s.restartChan = make(chan proto.Instance, 1)
}

func (s *AnalyzerTestSuite) SetUpTest(t *C) {
	s.nullmysql.Reset()
	s.iter.Reset()
	s.spool.Reset()
	s.clock = mock.NewClock()
	if err := test.ClearDir(s.configDir, "*"); err != nil {
		t.Fatal(err)
	}
	s.worker = qan_worker.NewQanWorker()
	// Config needs to be recreated on every test since it can be modified by the test analyzers
	exampleQueries := true
	slowLogRotation := true
	s.config = config.QAN{
		QAN: pc.QAN{
			UUID:            s.mysqlUUID,
			CollectFrom:     "slowlog",
			Interval:        60,
			MaxSlowLogSize:  MAX_SLOW_LOG_SIZE,
			SlowLogRotation: &slowLogRotation,
			Start: []string{
				"-- start",
			},
			Stop: []string{
				"-- stop",
			},
			ExampleQueries: &exampleQueries,
		},
	}
}

func (s *AnalyzerTestSuite) TearDownSuite(t *C) {
	if err := os.RemoveAll(s.tmpDir); err != nil {
		t.Error(err)
	}
}

// --------------------------------------------------------------------------

func (s *AnalyzerTestSuite) TestRunMockWorker(t *C) {
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		s.config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)

	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// No interval yet, so work should not have one.
	t.Check(s.worker.Interval, IsNil)

	// Send an interval. The analyzer runs the worker with it.
	now := time.Now()
	i := &iter.Interval{
		Number:      1,
		StartTime:   now,
		StopTime:    now.Add(1 * time.Minute),
		Filename:    "slow.log",
		StartOffset: 0,
		EndOffset:   999,
	}
	s.intervalChan <- i

	if !test.WaitState(s.worker.SetupChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	t.Check(s.worker.Interval, DeepEquals, i)

	if !test.WaitState(s.worker.RunChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	if !test.WaitState(s.worker.CleanupChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	err = a.Stop()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Stopped")

	t.Check(a.String(), Equals, "qan-analyzer")
}

func (s *AnalyzerTestSuite) TestStartServiceFast(t *C) {
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	// Simulate the next tick being 3m away (mock.clock.Eta = 180) so that
	// run() sends the first tick on the tick chan, causing the first
	// interval to start immediately.
	s.clock.Eta = 180
	defer func() { s.clock.Eta = 0 }()

	config := s.config
	config.Interval = 300 // 5m
	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)
	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// run() should prime the tickChan with the 1st tick immediately.  This makes
	// the interval iter start the interval immediately.  Then run() continues
	// waiting for the iter to send an interval which happens when the real ticker
	// (the clock) sends the 2nd tick which is synced to the interval, thus ending
	// the first interval started by run() and starting the 2nd interval as normal.
	select {
	case tick := <-s.iter.TickChan():
		t.Check(tick.IsZero(), Not(Equals), true)
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for primer tick")
	}

	// Status indicates that next interval is 3m away as we faked.
	status := a.Status()
	t.Check(status["qan-analyzer-next-interval"], Equals, "180.0s")

	err = a.Stop()
	t.Assert(err, IsNil)
}

func (s *AnalyzerTestSuite) TestMySQLRestart(t *C) {
	s.nullmysql.Reset()
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		s.config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)
	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// Analyzer starts its iter when MySQL is ready.
	t.Check(s.iter.Calls(), DeepEquals, []string{"Start"})
	s.iter.Reset()

	// Simulate a MySQL restart. This causes the analyzer to re-configure MySQL
	// using the same Start queries.
	s.nullmysql.Reset()
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation
	s.nullmysql.SetCond.L.Lock()
	s.restartChan <- s.mysqlInstance
	s.nullmysql.SetCond.Wait()
	s.nullmysql.SetCond.L.Unlock()
	test.WaitStatus(1, a, "qan-analyzer", "Idle")
	expectedQueries := []string{
		"SET GLOBAL slow_query_log=OFF",
		"SET GLOBAL log_output='file'",
		"SET GLOBAL slow_query_log=ON",
		"SET time_zone='+0:00'",
	}

	t.Check(s.nullmysql.GetExec(), DeepEquals, expectedQueries)
	t.Check(a.Config().MaxSlowLogSize, Equals, MAX_SLOW_LOG_SIZE)

	// Analyzer stops and re-starts its iter on MySQL restart. We are not setting any config
	t.Check(s.iter.Calls(), DeepEquals, []string{"Stop", "Start"})

	// Enable slow log rotation by setting max_slowlog_size to a value > 4096,
	// then simulate MySQL restart.
	s.nullmysql.Reset()
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 100000)
	s.nullmysql.SetCond.L.Lock()
	s.restartChan <- s.mysqlInstance
	s.nullmysql.SetCond.Wait()
	s.nullmysql.SetCond.L.Unlock()
	test.WaitStatus(1, a, "qan-analyzer", "Idle")
	expectedQueries = []string{
		"SET GLOBAL max_slowlog_size = 0",
		"SET GLOBAL slow_query_log=OFF",
		"SET GLOBAL log_output='file'",
		"SET GLOBAL slow_query_log=ON",
		"SET time_zone='+0:00'",
	}

	t.Check(s.nullmysql.GetExec(), DeepEquals, expectedQueries)
	t.Check(a.Config().MaxSlowLogSize, Equals, int64(100000))
	err = a.Stop()
	t.Assert(err, IsNil)
}

func (s *AnalyzerTestSuite) TestRealSlowLogWorker(t *C) {
	dsn := os.Getenv("PCT_TEST_MYSQL_DSN")
	require.NotEmpty(t, dsn, "PCT_TEST_MYSQL_DSN is not set")

	realmysql := mysql.NewConnection(dsn)
	if err := realmysql.Connect(); err != nil {
		t.Fatal(err)
	}
	// Don't release all resources immediately because the worker needs the connection
	defer realmysql.Close()
	defer test.DrainRecvData(s.dataChan)

	config := s.config
	config.Start = []string{
		"SET GLOBAL slow_query_log=OFF",
		"SET GLOBAL long_query_time=0",
		"SET GLOBAL slow_query_log=ON",
	}
	config.Stop = []string{
		"SET GLOBAL slow_query_log=OFF",
		"SET GLOBAL long_query_time=10",
	}

	worker := slowlog.NewWorker(pct.NewLogger(s.logChan, "qan-worker"), config, realmysql)
	//intervalChan := make(chan *iter.Interval, 1)
	//iter := mock.NewIter(intervalChan)

	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		config,
		s.iter,
		realmysql,
		s.restartChan,
		worker,
		s.clock,
		s.spool,
	)
	err := a.Start()
	t.Assert(err, IsNil)
	if !test.WaitStatus(3, a, "qan-analyzer", "Idle") {
		t.Fatal("Timeout waiting for qan-analyzer=Idle")
	}

	now := time.Now().UTC()
	i := &iter.Interval{
		Number:      1,
		StartTime:   now,
		StopTime:    now.Add(1 * time.Minute),
		Filename:    inputDir + "slow001.log",
		StartOffset: 0,
		EndOffset:   524,
	}
	s.intervalChan <- i
	data := test.WaitData(s.dataChan)
	t.Assert(data, HasLen, 1)
	res := data[0].(*qp.Report)
	t.Check(res.Global.TotalQueries, Equals, uint(2))

	err = a.Stop()
	t.Assert(err, IsNil)
}

func (s *AnalyzerTestSuite) TestRecoverWorkerPanic(t *C) {
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		s.config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)

	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// This will cause the worker to panic when it's ran by the analyzer.
	s.worker.SetupCrashChan <- true

	// Send an interval. The analyzer runs the worker with it.
	now := time.Now()
	i := &iter.Interval{
		Number:      1,
		StartTime:   now,
		StopTime:    now.Add(1 * time.Minute),
		Filename:    "slow.log",
		StartOffset: 0,
		EndOffset:   999,
	}
	s.intervalChan <- i

	if !test.WaitState(s.worker.SetupChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	// Wait for that ^ run of the worker to fully stop and return.
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	i = &iter.Interval{
		Number:      2,
		StartTime:   now,
		StopTime:    now.Add(1 * time.Minute),
		Filename:    "slow.log",
		StartOffset: 1000,
		EndOffset:   2000,
	}
	s.intervalChan <- i

	if !test.WaitState(s.worker.SetupChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	t.Check(s.worker.Interval, DeepEquals, i)

	if !test.WaitState(s.worker.RunChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	if !test.WaitState(s.worker.CleanupChan) {
		t.Fatal("Timeout waiting for <-s.worker.SetupChan")
	}

	err = a.Stop()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Stopped")

	t.Check(a.String(), Equals, "qan-analyzer")
}

// Test that a disabled slow log rotation in Percona Server (or MySQL) does not change analizer config
func (s *AnalyzerTestSuite) TestNoSlowLogTakeOver(t *C) {
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	/*
		PS can be configured to rotate slow log, making qan break.
		Since qan cannot handle the situation where a slow log is rotated by a third party we take over Percona Server
		rotation and disable it on DB.
	*/
	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		s.config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)

	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// Disable DB rotation by setting max_slowlog_size to a value < 4096
	s.nullmysql.Reset()
	lower := mysqlAnalyzer.MIN_SLOWLOG_ROTATION_SIZE - 1
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", lower)
	// Trigger our PS slow log rotation take-over, everything should stay the same since max_slowlog_size is < 4096
	a.TakeOverPerconaServerRotation()
	t.Check(a.Config().MaxSlowLogSize, Equals, MAX_SLOW_LOG_SIZE)
	err = a.Stop()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Stopped")
	t.Check(a.String(), Equals, "qan-analyzer")
}

// Test slow log rotation take over from Percona Server
func (s *AnalyzerTestSuite) TestSlowLogTakeOver(t *C) {
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", 0) // TakeOverPerconaServerRotation

	a := mysqlAnalyzer.NewRealAnalyzer(
		pct.NewLogger(s.logChan, "qan-analyzer"),
		s.config,
		s.iter,
		s.nullmysql,
		s.restartChan,
		s.worker,
		s.clock,
		s.spool,
	)

	err := a.Start()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Idle")

	// Increase our max_slowlog_size in mocked DB
	s.nullmysql.Reset()
	greater := mysqlAnalyzer.MIN_SLOWLOG_ROTATION_SIZE + 1
	s.nullmysql.SetGlobalVarInteger("max_slowlog_size", greater)
	// Trigger slowlog rotation, takeover should succeed since max_slowlog_size >= mysqlAnalyzer.MIN_SLOWLOG_ROTATION_SIZE
	a.TakeOverPerconaServerRotation()
	expectedQueries := []string{
		"SET GLOBAL max_slowlog_size = 0",
	}

	t.Check(s.nullmysql.GetExec(), DeepEquals, expectedQueries)
	// Config should now have the configured Percona Server slow log rotation file size
	t.Check(a.Config().MaxSlowLogSize, Equals, int64(greater))

	err = a.Stop()
	t.Assert(err, IsNil)
	test.WaitStatus(1, a, "qan-analyzer", "Stopped")
	t.Check(a.String(), Equals, "qan-analyzer")
}
