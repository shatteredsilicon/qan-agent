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
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/shatteredsilicon/qan-agent/instance"
	"github.com/shatteredsilicon/qan-agent/mrms"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	mysqlAnalyzer "github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql"
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
	queryProto "github.com/shatteredsilicon/ssm/proto/query"
	"github.com/stretchr/testify/require"
	. "gopkg.in/check.v1"
)

var inputDir = RootDir() + "/test/slow-logs/"

type AnalyzerTestSuite struct {
	nullmysql     *mock.NullMySQL
	iter          *interval_iter.Iter
	spool         *mock.Spooler
	cache         *mock.Cacher
	clock         *mock.Clock
	api           *mock.API
	worker        *qan_worker.QanWorker
	mrmsChan      chan interface{}
	logChan       chan proto.LogEntry
	logger        *pct.Logger
	intervalChan  chan *iter.Interval
	dataChan      chan interface{}
	tmpDir        string
	configDir     string
	im            *instance.Repo
	mysqlUUID     string
	mysqlInstance proto.Instance
	config        analyzer.QAN
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

	s.mrmsChan = make(chan interface{}, 1)
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
	s.config = analyzer.QAN{
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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
	s.mrmsChan <- s.mysqlInstance
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
	s.mrmsChan <- s.mysqlInstance
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

	logger := pct.NewLogger(s.logChan, "qan-analyzer")
	mrmsMonitor := mrms.NewRealMonitor(logger, &mysql.RealConnectionFactory{})
	worker := slowlog.NewWorker(pct.NewLogger(s.logChan, "qan-worker"), config, realmysql, mrmsMonitor)
	//intervalChan := make(chan *iter.Interval, 1)
	//iter := mock.NewIter(intervalChan)

	a := mysqlAnalyzer.NewRealAnalyzer(
		logger,
		config,
		s.iter,
		realmysql,
		s.mrmsChan,
		worker,
		s.clock,
		s.spool,
		s.cache,
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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
		s.mrmsChan,
		s.worker,
		s.clock,
		s.spool,
		s.cache,
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

var parseQueryTests = []struct {
	query      string
	abstract   string
	tables     []queryProto.Table
	procedures []queryProto.Procedure
}{
	{
		"select c from t where id=?",
		"SELECT t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},
	{ // #1
		"select c from db.t where id=?",
		"SELECT db.t",
		[]queryProto.Table{{Db: "db", Table: "t"}},
		nil,
	},
	{ // #2
		"select c from db.t, t2 where id=?",
		"SELECT db.t t2",
		[]queryProto.Table{
			{Db: "db", Table: "t"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
	{ // #3
		"SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`",
		"SELECT film",
		[]queryProto.Table{{Db: "", Table: "film"}},
		nil,
	},
	{ // #4
		"select c from ta join tb on (ta.id=tb.id) where id=?",
		"SELECT ta tb",
		[]queryProto.Table{
			{Db: "", Table: "ta"},
			{Db: "", Table: "tb"},
		},
		nil,
	},
	{ // #5
		"select c from ta join tb on (ta.id=tb.id) join tc on (1=1) where id=?",
		"SELECT ta tb tc",
		[]queryProto.Table{
			{Db: "", Table: "ta"},
			{Db: "", Table: "tb"},
			{Db: "", Table: "tc"},
		},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// INSERT
	{ // #6
		"INSERT INTO my_table (a,b,c) VALUES (1, 2, 3)",
		"INSERT my_table",
		[]queryProto.Table{{Db: "", Table: "my_table"}},
		nil,
	},
	{ // #7
		"INSERT INTO d.t (a,b,c) VALUES (1, 2, 3)",
		"INSERT d.t",
		[]queryProto.Table{{Db: "d", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// UPDATE
	{ // #8
		"update t set foo=?",
		"UPDATE t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// DELETE
	{ // #9
		"delete from t where id in (?+)",
		"DELETE t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// Other with partial support
	{ // #10
		"show status like ?",
		"SHOW STATUS",
		nil,
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	{ // #11
		"REPLACE INTO my_table (a,b,c) VALUES (1, 2, 3)",
		"REPLACE my_table",
		[]queryProto.Table{{Db: "", Table: "my_table"}},
		nil,
	},
	{ // #12
		"OPTIMIZE TABLE `o2408`.`agent_log`",
		"OPTIMIZE o2408.agent_log",
		[]queryProto.Table{
			{Db: "o2408", Table: "agent_log"},
		},
		nil,
	},
	{ // #13
		"select c from t1 join t2 using (c) where id=?",
		"SELECT t1 t2",
		[]queryProto.Table{
			{Db: "", Table: "t1"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
	{ // #14
		"insert into data values (...)",
		"INSERT data",
		[]queryProto.Table{
			{Db: "", Table: "data"},
		},
		nil,
	},
	{ // #15
		"call\n pita(?)",
		"CALL pita",
		nil,
		[]queryProto.Procedure{
			{DB: "", Name: "pita"},
		},
	},
	{ // #16 exceeds MAX_JOIN_DEPTH
		"select c from a" +
			" join b on (1=1) join c on (1=1) join d on (1=1) join e on (1=1)" +
			" join f on (1=1) join g on (1=1) join h on (1=1) join i on (1=1)" +
			" join j on (1=1) join k on (1=1) join l on (1=1) join m on (1=1)" +
			" join n on (1=1) join o on (1=1) join p on (1=1) join q on (1=1)" +
			" join r on (1=1) join s on (1=1) join t on (1=1) join u on (1=1)" +
			" join v on (1=1) join w on (1=1) join x on (1=1) join y on (1=1)" +
			" join z on (1=1)" +
			" where id=?",
		"SELECT a b c d e f g h i j k l m n o p q r s t u v w x y z",
		[]queryProto.Table{
			{"", "a"},
			{"", "b"}, {"", "c"}, {"", "d"}, {"", "e"},
			{"", "f"}, {"", "g"}, {"", "h"}, {"", "i"},
			{"", "j"}, {"", "k"}, {"", "l"}, {"", "m"},
			{"", "n"}, {"", "o"}, {"", "p"}, {"", "q"},
			{"", "r"}, {"", "s"}, {"", "t"}, {"", "u"},
			{"", "v"}, {"", "w"}, {"", "x"}, {"", "y"},
			{"", "z"},
		},
		nil,
	},
	{ // #17
		"SELECT DISTINCT c\n FROM sbtest1\nWHERE id\nBETWEEN 1\nAND 100\nORDER BY  c\n",
		"SELECT sbtest1",
		[]queryProto.Table{{Db: "", Table: "sbtest1"}},
		nil,
	},
	{ // #18
		"SELECT DISTINCT c FROM sbtest2 WHERE id BETWEEN 1 AND 100 ORDER BY c",
		"SELECT sbtest2",
		[]queryProto.Table{{Db: "", Table: "sbtest2"}},
		nil,
	},
	// Don't remove the ; at the end of the next query.
	// There was an error in the past where a ; at the end was making the
	// parser to fail and we want to ensure it works now.
	{ // #19
		"SELECT * from `sysbenchtest`.`t6002_0`;",
		"SELECT sysbenchtest.t6002_0",
		[]queryProto.Table{{Db: "sysbenchtest", Table: "t6002_0"}},
		nil,
	},
	{ // #20
		"use zapp",
		"USE",
		nil,
		nil,
	},
	// Schema was set as default from the previous USE
	{ // #21
		"SELECT * from `t6003_0`;",
		"SELECT t6003_0",
		[]queryProto.Table{{Db: "", Table: "t6003_0"}},
		nil,
	},
	{ // #22
		"CREATE TABLE t6004 (PRIMARY KEY id int, a varchar(25)) engine=innodb",
		"CREATE TABLE t6004",
		[]queryProto.Table{{Db: "", Table: "t6004"}},
		nil,
	},
	{ // #23
		"ALTER TABLE sakila.actor ADD COLUMN newcol int",
		"ALTER TABLE sakila.actor",
		[]queryProto.Table{{Db: "sakila", Table: "actor"}},
		nil,
	},
	// Db & Table are empty because CREATE DATABASE is not yet supported by Vitess.sqlparser
	{ // #24
		"CREATE DATABASE ssm",
		"CREATE DATABASE ssm",
		nil,
		nil,
	},
	{ // #25
		"create index idx ON percona (f1)",
		"ALTER TABLE percona",
		[]queryProto.Table{{Db: "", Table: "percona"}},
		nil,
	},
	{ // #26 override the default USE
		"create index idx ON brannigan.percona (f1)",
		"ALTER TABLE brannigan.percona",
		[]queryProto.Table{{Db: "brannigan", Table: "percona"}},
		nil,
	},
	// PMM-1892. Upgraded Vitess libraries to support this query.
	// Notice that the query below is not exactly the same reported in the ticket; this
	// query has `auto_increment` between backticks because it is a reserved MySQL word
	// but MySQL accepts it anyway as a field name while Vitess doesn't.
	{
		"SELECT table_schema, table_name, column_name, `auto_increment`, " +
			"pow(2, CASE data_type WHEN 'tinyint' THEN 7 WHEN 'smallint' " +
			"THEN 15 WHEN 'mediumint' THEN 23 WHEN 'int' THEN 31 WHEN 'bigint' " +
			"THEN 63 end+(column_type LIKE '% unsigned'))-1 AS max_int FROM " +
			"information_schema.tables t JOIN information_schema.columns c " +
			"USING (table_schema,table_name) WHERE c.extra = 'auto_increment' " +
			"AND t.auto_increment IS NOT NULL",
		"SELECT information_schema.tables information_schema.columns",
		[]queryProto.Table{
			{Db: "information_schema", Table: "tables"},
			{Db: "information_schema", Table: "columns"},
		},
		nil,
	},
	{ // #28
		"SELECT @@`version`",
		"SELECT",
		nil,
		nil,
	},
	{ // #29
		"SELECT t1.*, t2.* FROM (SELECT * FROM test1) t1 JOIN (SELECT * FROM test2) t2 ON t1.id1 = t2.id2",
		"SELECT test1 test2",
		[]queryProto.Table{
			{Db: "", Table: "test1"},
			{Db: "", Table: "test2"},
		},
		nil,
	},
	{ // #30
		"SELECT t.* FROM (SELECT t1.*, t2.* FROM (SELECT * FROM test1) t1 JOIN (SELECT * FROM test2) t2 ON t1.id1 = t2.id2) t UNION SELECT t.* FROM (SELECT t3.*, t4.* FROM (SELECT * FROM test3) t3 JOIN (SELECT * FROM test4) t4 ON t3.id3 = t4.id4) t",
		"SELECT test1 test2 test3 test4",
		[]queryProto.Table{
			{Db: "", Table: "test1"},
			{Db: "", Table: "test2"},
			{Db: "", Table: "test3"},
			{Db: "", Table: "test4"},
		},
		nil,
	},
	{ // #31
		`
		-- UPDATE test users
		UPDATE test.users
		SET user_id = @USER,
			email = (
				SELECT user_email
				FROM test.wp_users
				WHERE id = @USER
			)
		WHERE wp_user_id = @USER
		`,
		"UPDATE test.users",
		[]queryProto.Table{
			{Db: "test", Table: "users"},
			{Db: "test", Table: "wp_users"},
		},
		nil,
	},
	{ // #32
		`
		-- CREATE TEMPORARY TABLE
		CREATE TEMPORARY TABLE test.tmp_t (
			PRIMARY KEY tmp_t_pkey (product_id),
			INDEX comp_key (symbol,expiration_date)
		)
			SELECT product_id, expiration_date, symbol, IF(cs = 0, 'P', 'C') as cs
			FROM test.t t
			WHERE t.ct = 7 AND t.expiration_date >= CURDATE() AND t.af = true
		`,
		"CREATE TABLE test.tmp_t",
		[]queryProto.Table{
			{Db: "test", Table: "tmp_t"},
		},
		nil,
	},
	{ // #33
		`
		SELECT *
		FROM t1
		WHERE dID IN
			(SELECT dID
			FROM t2
			WHERE uID ='12345')
				AND ` + "`enabled` = 1" + `
				AND ` + "`generated` = 1" + `
		`,
		"SELECT t1",
		[]queryProto.Table{
			{Db: "", Table: "t1"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
}

func TestParseQuery(t *testing.T) {
	t.Parallel()

	for i, test := range parseQueryTests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			abstract, tables, procedures, err := mysqlAnalyzer.ParseQuery(test.query)
			require.Nil(t, err)

			require.Equal(t, test.abstract, abstract)
			require.Equal(t, test.tables, tables)
			require.Equal(t, test.procedures, procedures)
		})
	}
}
