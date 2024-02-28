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

package perfschema

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/ssm/proto"
	pc "github.com/shatteredsilicon/ssm/proto/config"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

// A DigestRow is a row from performance_schema.events_statements_summary_by_digest.
type DigestRow struct {
	Schema                  string
	Digest                  string
	DigestText              string
	CountStar               uint
	SumTimerWait            uint64
	MinTimerWait            uint64
	AvgTimerWait            uint64
	MaxTimerWait            uint64
	SumLockTime             uint64
	SumErrors               uint64
	SumWarnings             uint64
	SumRowsAffected         uint64
	SumRowsSent             uint64
	SumRowsExamined         uint64
	SumCreatedTmpDiskTables uint64 // bool in slow log
	SumCreatedTmpTables     uint64 // bool in slow log
	SumSelectFullJoin       uint64 // bool in slow log
	SumSelectFullRangeJoin  uint64
	SumSelectRange          uint64
	SumSelectRangeCheck     uint64
	SumSelectScan           uint64 // bool in slow log
	SumSortMergePasses      uint64
	SumSortRange            uint64
	SumSortRows             uint64
	SumSortScan             uint64
	SumNoIndexUsed          uint64
	SumNoGoodIndexUsed      uint64
}

// A PreStmtRow is a row from prepared_statements_instances
type PreStmtRow struct {
	StatementID             uint64
	StatementName           string
	SQLText                 string
	TimerPrepare            uint64
	CountReprepare          uint64
	CountExecute            uint64
	SumTimerExecute         uint64
	MinTimerExecute         uint64
	AvgTimerExecute         uint64
	MaxTimerExecute         uint64
	SumLockTime             uint64
	SumErrors               uint64
	SumWarnings             uint64
	SumRowsAffected         uint64
	SumRowsSent             uint64
	SumRowsExamined         uint64
	SumCreatedTmpDiskTables uint64 // bool in slow log
	SumCreatedTmpTables     uint64 // bool in slow log
	SumSelectFullJoin       uint64 // bool in slow log
	SumSelectFullRangeJoin  uint64
	SumSelectRange          uint64
	SumSelectRangeCheck     uint64
	SumSelectScan           uint64 // bool in slow log
	SumSortMergePasses      uint64
	SumSortRange            uint64
	SumSortRows             uint64
	SumSortScan             uint64
	SumNoIndexUsed          uint64
	SumNoGoodIndexUsed      uint64
}

// ConvertToDigestRow returns data in DigestRow form
func (row *PreStmtRow) ConvertToDigestRow() *DigestRow {
	digestRow := DigestRow{}

	preStmtSQL := sha256.Sum256([]byte(fmt.Sprintf("PREPARE %s FROM %s", row.StatementName, row.SQLText)))
	digestRow.Digest = fmt.Sprintf("%x", preStmtSQL)
	digestRow.DigestText = row.SQLText
	digestRow.CountStar = uint(row.CountExecute)
	digestRow.SumTimerWait = row.SumTimerExecute
	digestRow.MinTimerWait = row.MinTimerExecute
	digestRow.AvgTimerWait = row.AvgTimerExecute
	digestRow.MaxTimerWait = row.MaxTimerExecute
	digestRow.SumLockTime = row.SumLockTime
	digestRow.SumErrors = row.SumErrors
	digestRow.SumWarnings = row.SumWarnings
	digestRow.SumRowsAffected = row.SumRowsAffected
	digestRow.SumRowsSent = row.SumRowsSent
	digestRow.SumRowsExamined = row.SumRowsExamined
	digestRow.SumCreatedTmpDiskTables = row.SumCreatedTmpDiskTables
	digestRow.SumCreatedTmpTables = row.SumCreatedTmpTables
	digestRow.SumSelectFullJoin = row.SumSelectFullJoin
	digestRow.SumSelectFullRangeJoin = row.SumSelectFullRangeJoin
	digestRow.SumSelectRange = row.SumSelectRange
	digestRow.SumSelectRangeCheck = row.SumSelectRangeCheck
	digestRow.SumSelectScan = row.SumSelectScan
	digestRow.SumSortMergePasses = row.SumSortMergePasses
	digestRow.SumSortRange = row.SumSortRange
	digestRow.SumSortRows = row.SumSortRows
	digestRow.SumSortScan = row.SumSortScan
	digestRow.SumNoIndexUsed = row.SumNoIndexUsed
	digestRow.SumNoGoodIndexUsed = row.SumNoGoodIndexUsed

	return &digestRow
}

// A Class represents a single query and its per-schema instances.
type Class struct {
	DigestText string
	Rows       map[string]*DigestRow // keyed on schema
}

// A Snapshot represents all rows from performance_schema.events_statements_summary_by_digest
// and performance_schema.prepared_statements_instances at a single time,
// grouped by digest into classes. Two consecutive Snapshots are needed to
// produce a mysqlAnalyzer.Result.
type Snapshot map[string]Class // keyed on digest (classId)

type WorkerFactory interface {
	Make(name string, mysqlConn mysql.Connector, filterOmit []string) *Worker
}

type RealWorkerFactory struct {
	logChan chan proto.LogEntry
}

type perfSchemaExample struct {
	Schema   sql.NullString
	SQLText  sql.NullString
	LastSeen time.Time
}

func NewRealWorkerFactory(logChan chan proto.LogEntry) *RealWorkerFactory {
	f := &RealWorkerFactory{
		logChan: logChan,
	}
	return f
}

func (f *RealWorkerFactory) Make(name string, mysqlConn mysql.Connector, filterOmit []string) *Worker {
	getRows := func(c chan<- *DigestRow, lastFetchSeconds float64, doneChan chan<- error) error {
		return GetDigestRows(mysqlConn, lastFetchSeconds, c, doneChan, filterOmit)
	}

	return NewWorker(pct.NewLogger(f.logChan, name), mysqlConn, getRows)
}

// GetDigestRows connects to MySQL through `mysql.Connector`,
// fetches snapshot of data from events_statements_summary_by_digest,
// delivers it over a channel, and notifies success or error through `doneChan`.
// If `lastFetchSeconds` equals `-1` then it fetches all data, not just since `lastFetchSeconds`.
func GetDigestRows(mysqlConn mysql.Connector, lastFetchSeconds float64, c chan<- *DigestRow, doneChan chan<- error, filterOmit []string) error {
	q := `
SELECT
	COALESCE(SCHEMA_NAME, ''),
	COALESCE(DIGEST, ''),
	COALESCE(DIGEST_TEXT, ''),
	COUNT_STAR,
	SUM_TIMER_WAIT,
	MIN_TIMER_WAIT,
	AVG_TIMER_WAIT,
	MAX_TIMER_WAIT,
	SUM_LOCK_TIME,
	SUM_ERRORS,
	SUM_WARNINGS,
	SUM_ROWS_AFFECTED,
	SUM_ROWS_SENT,
	SUM_ROWS_EXAMINED,
	SUM_CREATED_TMP_DISK_TABLES,
	SUM_CREATED_TMP_TABLES,
	SUM_SELECT_FULL_JOIN,
	SUM_SELECT_FULL_RANGE_JOIN,
	SUM_SELECT_RANGE,
	SUM_SELECT_RANGE_CHECK,
	SUM_SELECT_SCAN,
	SUM_SORT_MERGE_PASSES,
	SUM_SORT_RANGE,
	SUM_SORT_ROWS,
	SUM_SORT_SCAN,
	SUM_NO_INDEX_USED,
	SUM_NO_GOOD_INDEX_USED
	FROM performance_schema.events_statements_summary_by_digest
`

	if lastFetchSeconds >= 0 {
		q += fmt.Sprintf(" WHERE LAST_SEEN >= NOW() - INTERVAL %d SECOND", int64(lastFetchSeconds))
	}

	rows, err := mysqlConn.DB().Query(q)
	if err != nil {
		// This bubbles up to the analyzer which logs it as an error:
		//   0. Analyzer.Worker.Run()
		//   1. Worker.Run().getSnapShot()
		//   2. Worker.getSnapshot().getRows() (ptr to this func)
		//   3. here
		return err
	}
	go func() {
		var err error
		defer func() {
			rows.Close()
			doneChan <- err
		}()
		for rows.Next() {
			row := &DigestRow{}
			err = rows.Scan(
				&row.Schema,
				&row.Digest,
				&row.DigestText,
				&row.CountStar,
				&row.SumTimerWait,
				&row.MinTimerWait,
				&row.AvgTimerWait,
				&row.MaxTimerWait,
				&row.SumLockTime,
				&row.SumErrors,
				&row.SumWarnings,
				&row.SumRowsAffected,
				&row.SumRowsSent,
				&row.SumRowsExamined,
				&row.SumCreatedTmpDiskTables,
				&row.SumCreatedTmpTables,
				&row.SumSelectFullJoin,
				&row.SumSelectFullRangeJoin,
				&row.SumSelectRange,
				&row.SumSelectRangeCheck,
				&row.SumSelectScan,
				&row.SumSortMergePasses,
				&row.SumSortRange,
				&row.SumSortRows,
				&row.SumSortScan,
				&row.SumNoIndexUsed,
				&row.SumNoGoodIndexUsed,
			)
			if err != nil {
				return // This bubbles up too (see above).
			}

			// "The Performance Schema could produce DIGEST_TEXT values with a trailing space.
			// This no longer occurs. (Bug #26908015)"
			// https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-11.html
			row.DigestText = strings.TrimSpace(row.DigestText)

			var omit bool
			for _, omitQuery := range filterOmit {
				if strings.HasPrefix(
					strings.TrimSpace(strings.ToLower(row.DigestText)),
					strings.TrimSpace(strings.ToLower(omitQuery)),
				) {
					omit = true
					break
				}
			}
			if omit {
				continue
			}

			c <- row
		}
		if err = rows.Err(); err != nil {
			return // This bubbles up too (see above).
		}

		preStmtDoneChan := make(chan error, 1)
		if err = GetPreStmtRows(mysqlConn, c, preStmtDoneChan, filterOmit); err == nil {
			err = <-preStmtDoneChan
		}

		if errCode, ok := err.(*mysqlDriver.MySQLError); ok && errCode.Number == 1146 {
			// Ignore if it's a 'table not exists' error to
			// be compatible with older mysql/mariadb versions
			err = nil
		}
	}()
	return nil
}

// GetPreStmtRows connects to MySQL through `mysql.Connector`,
// fetches rows from prepared_stmtements_instances and converts rows into DigestRow structure,
// delivers it over a channel, and notifies success or error through `doneChan`.
func GetPreStmtRows(mysqlConn mysql.Connector, c chan<- *DigestRow, doneChan chan<- error, filterOmit []string) error {
	q := `
SELECT
	STATEMENT_ID,
	COALESCE(STATEMENT_NAME, ''),
	SQL_TEXT,
	TIMER_PREPARE,
	COUNT_REPREPARE,
	COUNT_EXECUTE,
	SUM_TIMER_EXECUTE,
	MIN_TIMER_EXECUTE,
	AVG_TIMER_EXECUTE,
	MAX_TIMER_EXECUTE,
	SUM_LOCK_TIME,
	SUM_ERRORS,
	SUM_WARNINGS,
	SUM_ROWS_AFFECTED,
	SUM_ROWS_SENT,
	SUM_ROWS_EXAMINED,
	SUM_CREATED_TMP_DISK_TABLES,
	SUM_CREATED_TMP_TABLES,
	SUM_SELECT_FULL_JOIN,
	SUM_SELECT_FULL_RANGE_JOIN,
	SUM_SELECT_RANGE,
	SUM_SELECT_RANGE_CHECK,
	SUM_SELECT_SCAN,
	SUM_SORT_MERGE_PASSES,
	SUM_SORT_RANGE,
	SUM_SORT_ROWS,
	SUM_SORT_SCAN,
	SUM_NO_INDEX_USED,
	SUM_NO_GOOD_INDEX_USED
	FROM performance_schema.prepared_statements_instances
`

	rows, err := mysqlConn.DB().Query(q)
	if err != nil {
		// This bubbles up to the analyzer which logs it as an error:
		//   0. Analyzer.Worker.Run()
		//   1. Worker.Run().getSnapShot()
		//   2. Worker.getSnapshot().getRows() (ptr to this func)
		//   3. here
		return err
	}
	go func() {
		var err error
		defer func() {
			rows.Close()
			doneChan <- err
		}()
		for rows.Next() {
			row := &PreStmtRow{}
			err = rows.Scan(
				&row.StatementID,
				&row.StatementName,
				&row.SQLText,
				&row.TimerPrepare,
				&row.CountReprepare,
				&row.CountExecute,
				&row.SumTimerExecute,
				&row.MinTimerExecute,
				&row.AvgTimerExecute,
				&row.MaxTimerExecute,
				&row.SumLockTime,
				&row.SumErrors,
				&row.SumWarnings,
				&row.SumRowsAffected,
				&row.SumRowsSent,
				&row.SumRowsExamined,
				&row.SumCreatedTmpDiskTables,
				&row.SumCreatedTmpTables,
				&row.SumSelectFullJoin,
				&row.SumSelectFullRangeJoin,
				&row.SumSelectRange,
				&row.SumSelectRangeCheck,
				&row.SumSelectScan,
				&row.SumSortMergePasses,
				&row.SumSortRange,
				&row.SumSortRows,
				&row.SumSortScan,
				&row.SumNoIndexUsed,
				&row.SumNoGoodIndexUsed,
			)
			if err != nil {
				return // This bubbles up too (see above).
			}

			var omit bool
			for _, omitQuery := range filterOmit {
				if strings.HasPrefix(
					strings.TrimSpace(strings.ToLower(row.SQLText)),
					strings.TrimSpace(strings.ToLower(omitQuery)),
				) {
					omit = true
					break
				}
			}
			if omit {
				continue
			}

			c <- row.ConvertToDigestRow()
		}
		if err = rows.Err(); err != nil {
			return // This bubbles up too (see above).
		}
	}()
	return nil
}

type GetDigestRowsFunc func(c chan<- *DigestRow, lastFetchSeconds float64, doneChan chan<- error) error

type Worker struct {
	logger    *pct.Logger
	mysqlConn mysql.Connector
	getRows   GetDigestRowsFunc
	// --
	name            string
	status          *pct.Status
	digests         *Digests
	iter            *iter.Interval
	lastErr         error
	lastRowCnt      uint
	lastFetchTime   time.Time
	lastPrepTime    float64
	collectExamples bool

	//
	lock                  sync.Mutex
	isRunning             bool
	collectExamplesTicker *time.Ticker
	queryExamples         map[string]perfSchemaExample
}

func NewWorker(logger *pct.Logger, mysqlConn mysql.Connector, getRows GetDigestRowsFunc) *Worker {
	name := logger.Service()
	w := &Worker{
		logger:    logger,
		mysqlConn: mysqlConn,
		getRows:   getRows,
		// --
		name: name,
		status: pct.NewStatus([]string{
			name,
			name + "-last",
			name + "-digests",
		}),
		digests:       NewDigests(),
		queryExamples: make(map[string]perfSchemaExample),
	}
	return w
}

func (w *Worker) Setup(interval *iter.Interval, resultChan chan *report.Result) error {
	if w.iter != nil {
		// Ensure intervals are in sequence, else reset.
		if interval.Number != w.iter.Number+1 {
			w.logger.Warn(fmt.Sprintf("Interval out of sequence: got %d, expected %d", interval.Number, w.iter.Number+1))
			w.reset()
		} else if interval.StartTime.Before(w.iter.StartTime) {
			w.logger.Warn(fmt.Sprintf("Interval reset: previous at %s, now %s", interval.StartTime, w.iter.StartTime))
			w.reset()
		}
	}
	w.iter = interval
	// Reset -last status vals.
	w.lastRowCnt = 0
	w.lastPrepTime = 0
	return nil
}

func (w *Worker) Run() (*report.Result, error) {
	w.logger.Debug("Run:call:", w.iter.Number)
	defer w.logger.Debug("Run:return:", w.iter.Number)

	defer w.status.Update(w.name, "Idle")

	w.status.Update(w.name, "Connecting to MySQL")
	if err := w.mysqlConn.Connect(); err != nil {
		w.logger.Warn(err.Error())
		w.lastErr = err
		return nil, nil // not an error to caller
	}
	defer w.mysqlConn.Close()

	var err error
	w.digests.Curr, err = w.getSnapshot()
	if err != nil {
		w.lastErr = err
		return nil, err
	}

	if len(w.digests.All) == 0 {
		return nil, nil
	}

	res, err := w.prepareResult(w.digests.All, w.digests.Curr)
	if err != nil {
		w.lastErr = err
		return nil, err
	}

	return res, nil
}

func (w *Worker) Cleanup() error {
	w.logger.Debug("Cleanup:call:", w.iter.Number)
	defer w.logger.Debug("Cleanup:return:", w.iter.Number)
	w.digests.MergeCurr()
	last := fmt.Sprintf("rows: %d, fetch: %s, prep: %s",
		w.lastRowCnt, w.lastFetchTime.Format(time.RFC3339), pct.Duration(w.lastPrepTime))
	if w.lastErr != nil {
		last += fmt.Sprintf(", error: %s", w.lastErr)
	}
	digests := fmt.Sprintf("all: %d, curr: %d", len(w.digests.All), len(w.digests.Curr))
	w.status.Update(w.name+"-last", last)
	w.status.Update(w.name+"-digest", digests)
	return nil
}

func (w *Worker) Stop() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.collectExamplesTicker != nil {
		w.collectExamplesTicker.Stop()
		w.collectExamplesTicker = nil
	}
	return nil
}

func (w *Worker) Status() map[string]string {
	return w.status.All()
}

func (w *Worker) SetConfig(config pc.QAN) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.collectExamples = *config.ExampleQueries
	if w.collectExamples && w.collectExamplesTicker == nil {
		w.collectExamplesTicker = time.NewTicker(time.Millisecond * 1000)
		go w.getQueryExamples(w.collectExamplesTicker.C)
	}
}

// --------------------------------------------------------------------------

func (w *Worker) reset() {
	w.iter = nil
	w.digests.Reset()
	w.lastErr = nil
	w.lastRowCnt = 0
	w.lastFetchTime = time.Time{}
	w.lastPrepTime = 0
}

func (w *Worker) getQueryExamples(ticker <-chan time.Time) {
	isRunning := false
	for range ticker {
		if isRunning {
			continue
		}
		w.isRunning = true

		err := w.mysqlConn.Connect()
		if err != nil {
			continue
		}

		query := "SELECT DIGEST, CURRENT_SCHEMA, SQL_TEXT FROM performance_schema.events_statements_history"
		rows, err := w.mysqlConn.DB().Query(query)
		if err != nil {
			return
		}
		for rows.Next() {
			ex := perfSchemaExample{
				LastSeen: time.Now(),
			}
			var digest sql.NullString
			err := rows.Scan(&digest, &ex.Schema, &ex.SQLText)
			if err != nil {
				continue
			}
			if !digest.Valid {
				continue
			}
			w.lock.Lock()
			classID := strings.ToUpper(digest.String[16:32])
			w.queryExamples[classID] = ex
			w.lock.Unlock()
		}
		rows.Close()
		w.isRunning = false
	}
}

func (w *Worker) getSnapshot() (Snapshot, error) {
	w.logger.Debug("getSnapshot:call:", w.iter.Number)
	defer w.logger.Debug("getSnapshot:return:", w.iter.Number)

	w.status.Update(w.name, "Processing rows")
	defer w.status.Update(w.name, "Idle")

	defer func() {
		w.lastFetchTime = time.Now().UTC()
	}()

	seconds := float64(0)
	// If it's first snapshot we should fetch it all
	if len(w.digests.All) == 0 {
		seconds = -1
	} else {
		seconds = time.Now().UTC().Sub(w.lastFetchTime).Seconds()
	}
	rowChan := make(chan *DigestRow)
	doneChan := make(chan error, 1)
	if err := w.getRows(rowChan, seconds, doneChan); err != nil {
		if err == sql.ErrNoRows {
			return Snapshot{}, nil
		}
		return Snapshot{}, err
	}

	curr := Snapshot{}
	var err error // from getRows() on doneChan
	for {
		select {
		case row := <-rowChan:
			w.lastRowCnt++
			// If events_statements_summary_by_digest is full, MySQL will start
			// setting the digest to NULL and will only compute a summary under that
			// null digest.
			// http://dev.mysql.com/doc/refman/5.6/en/statement-summary-tables.html#idm140190647360848
			// In that case, we set the digest to the string "2" (1 if for LRQ) to support
			// this summary in PCT
			classId := "2"
			if len(row.Digest) >= 32 {
				classId = strings.ToUpper(row.Digest[16:32])
			}
			if class, haveClass := curr[classId]; haveClass {
				if _, haveRow := class.Rows[row.Schema]; haveRow {
					w.logger.Error("Got class twice: ", row.Schema, row.Digest)
					continue
				}
				class.Rows[row.Schema] = row
			} else {
				// Get class digest text (fingerprint).
				digestText := row.DigestText
				if classId == "2" && digestText == "" {
					// To make explains works
					digestText = `-- performance_schema.events_statements_summary_by_digest is full`
				}

				// Create the class and init with this schema and row.
				curr[classId] = Class{
					DigestText: digestText,
					Rows: map[string]*DigestRow{
						row.Schema: row,
					},
				}
			}
		case err = <-doneChan:
			return curr, err
		}
	}
}

func (w *Worker) prepareResult(prev, curr Snapshot) (*report.Result, error) {
	w.logger.Debug("prepareResult:call:", w.iter.Number)
	defer w.logger.Debug("prepareResult:return:", w.iter.Number)

	w.status.Update(w.name, "Preparing result")
	defer w.status.Update(w.name, "Idle")

	t0 := time.Now()
	defer func() {
		w.lastPrepTime = time.Now().UTC().Sub(t0).Seconds()
	}()

	global := event.NewClass("", "", false)
	classes := []*event.Class{}

	// Compare current classes to previous.
ClassLoop:
	for classId, class := range curr {

		// If this class does not exist in prev, skip the entire class.
		prevClass, _ := prev[classId]
		/*
			if !ok {
				continue CLASS_LOOP
			}
		*/

		// This class exists in prev, so create a class aggregate of the per-schema
		// query value diffs, for rows that exist in both prev and curr.
		d := DigestRow{MinTimerWait: 0xFFFFFFFF} // class aggregate, becomes class metrics
		n := uint64(0)                           // number of query instances in prev and curr

		// Each row is an instance of the query executed in the schema.
	RowLoop:
		for schema, row := range class.Rows {
			prevRow, ok := prevClass.Rows[schema]
			if !ok {
				prevRow = &DigestRow{}
			}

			// Check if it executed during the interval.
			if row.CountStar == prevRow.CountStar {
				continue RowLoop // not executed during interval
			}

			// If current value of CountStart (number of queries)
			// is lesser than previous one, then this indicates truncate of the table.
			// In such case we should re-fetch whole data snapshot, not just it's part.
			// Reset the worker to initial state and drop this snapshot
			if row.CountStar < prevRow.CountStar {
				w.reset()
				return nil, nil
			}

			// This row executed during the interval.
			// If query 1 in db1 has prev.CountStar=50 and curr.CountStar=100,
			// and query 1 in db2 has prev.CountStar=100 and curr.CountStar=200,
			// that's +50 and +100 executions respectively, so +150 executions for
			// the class metrics.
			d.CountStar += row.CountStar - prevRow.CountStar
			d.SumTimerWait += row.SumTimerWait - prevRow.SumTimerWait
			d.SumLockTime += row.SumLockTime - prevRow.SumLockTime
			d.SumErrors += row.SumErrors - prevRow.SumErrors
			d.SumWarnings += row.SumWarnings - prevRow.SumWarnings
			d.SumRowsAffected += row.SumRowsAffected - prevRow.SumRowsAffected
			d.SumRowsSent += row.SumRowsSent - prevRow.SumRowsSent
			d.SumRowsExamined += row.SumRowsExamined - prevRow.SumRowsExamined
			d.SumCreatedTmpDiskTables += row.SumCreatedTmpDiskTables - prevRow.SumCreatedTmpDiskTables
			d.SumCreatedTmpTables += row.SumCreatedTmpTables - prevRow.SumCreatedTmpTables
			d.SumSelectFullJoin += row.SumSelectFullJoin - prevRow.SumSelectFullJoin
			d.SumSelectFullRangeJoin += row.SumSelectFullRangeJoin - prevRow.SumSelectFullRangeJoin
			d.SumSelectRange += row.SumSelectRange - prevRow.SumSelectRange
			d.SumSelectRangeCheck += row.SumSelectRangeCheck - prevRow.SumSelectRangeCheck
			d.SumSelectScan += row.SumSelectScan - prevRow.SumSelectScan
			d.SumSortMergePasses += row.SumSortMergePasses - prevRow.SumSortMergePasses
			d.SumSortRange += row.SumSortRange - prevRow.SumSortRange
			d.SumSortRows += row.SumSortRows - prevRow.SumSortRows
			d.SumSortScan += row.SumSortScan - prevRow.SumSortScan
			d.SumNoIndexUsed += row.SumNoIndexUsed - prevRow.SumNoIndexUsed
			d.SumNoGoodIndexUsed += row.SumNoGoodIndexUsed - prevRow.SumNoGoodIndexUsed

			// If it's first row for this class then set min,
			// otherwise min would be always 0.
			if d.CountStar == 0 {
				d.MinTimerWait = row.MinTimerWait
			}
			// Take the current min and max.
			if row.MinTimerWait < d.MinTimerWait {
				d.MinTimerWait = row.MinTimerWait
			}
			if row.MaxTimerWait > d.MaxTimerWait {
				d.MaxTimerWait = row.MaxTimerWait
			}

			// Add the averages, divide later.
			d.AvgTimerWait += row.AvgTimerWait
			n++
		}

		// Class was in prev, but no rows in prev were in curr, so skip the class.
		if n == 0 {
			continue ClassLoop
		}

		// Divide the total averages to yield the average of the averages.
		// Dividing by n not d.CountStar here is correct because n is the
		// number of query instances in prev and current, so it's also the
		// number of averages we added together. d.CountStar is the total
		// number of times all queries in this classes executed, which can
		// be very high.
		d.AvgTimerWait /= n

		// Create standard metric stats from the class metrics just calculated.
		stats := event.NewMetrics()

		// Time metrics are in picoseconds, so multiply by 10^-12 to convert to seconds.
		stats.TimeMetrics["Query_time"] = &qan.TimeStats{
			Sum: float64(d.SumTimerWait) * math.Pow10(-12),
			Min: event.Float64(float64(d.MinTimerWait) * math.Pow10(-12)),
			Avg: event.Float64(float64(d.AvgTimerWait) * math.Pow10(-12)),
			Max: event.Float64(float64(d.MaxTimerWait) * math.Pow10(-12)),
		}

		stats.TimeMetrics["Lock_time"] = &qan.TimeStats{
			Sum: float64(d.SumLockTime) * math.Pow10(-12),
		}

		stats.NumberMetrics["Errors"] = &qan.NumberStats{Sum: d.SumErrors}
		stats.NumberMetrics["Warnings"] = &qan.NumberStats{Sum: d.SumWarnings}
		stats.NumberMetrics["Rows_affected"] = &qan.NumberStats{Sum: d.SumRowsAffected}
		stats.NumberMetrics["Rows_sent"] = &qan.NumberStats{Sum: d.SumRowsSent}
		stats.NumberMetrics["Rows_examined"] = &qan.NumberStats{Sum: d.SumRowsExamined}
		stats.BoolMetrics["Tmp_table_on_disk"] = &qan.BoolStats{Sum: d.SumCreatedTmpDiskTables}
		stats.BoolMetrics["Tmp_table"] = &qan.BoolStats{Sum: d.SumCreatedTmpTables}
		stats.BoolMetrics["Full_join"] = &qan.BoolStats{Sum: d.SumSelectFullJoin}
		stats.NumberMetrics["Select_full_range_join"] = &qan.NumberStats{Sum: d.SumSelectFullRangeJoin}
		stats.NumberMetrics["Select_range"] = &qan.NumberStats{Sum: d.SumSelectRange}
		stats.NumberMetrics["Select_range_check"] = &qan.NumberStats{Sum: d.SumSelectRangeCheck}
		stats.BoolMetrics["Full_scan"] = &qan.BoolStats{Sum: d.SumSelectScan}
		stats.NumberMetrics["Merge_passes"] = &qan.NumberStats{Sum: d.SumSortMergePasses}
		stats.NumberMetrics["Sort_range"] = &qan.NumberStats{Sum: d.SumSortRange}
		stats.NumberMetrics["Sort_rows"] = &qan.NumberStats{Sum: d.SumSortRows}
		stats.NumberMetrics["Sort_scan"] = &qan.NumberStats{Sum: d.SumSortScan}
		stats.NumberMetrics["No_index_used"] = &qan.NumberStats{Sum: d.SumNoIndexUsed}
		stats.NumberMetrics["No_good_index_used"] = &qan.NumberStats{Sum: d.SumNoGoodIndexUsed}

		// Create and save the pre-aggregated class.  Using only last 16 digits
		// of checksum is historical: pt-query-digest does the same:
		// my $checksum = uc substr(md5_hex($val), -16);
		// 0 as tzDiff (last param) because we are not saving examples
		ex, ok := w.queryExamples[classId]
		class := event.NewClass(classId, class.DigestText, ok)
		if ok {
			class.Example = &qan.Example{
				QueryTime: float64(ex.LastSeen.Unix()),
				Db:        ex.Schema.String,
				Query:     ex.SQLText.String,
			}
		}
		class.TotalQueries = d.CountStar
		class.Metrics = stats
		class.Class.Metrics = &qan.Metrics{
			TimeMetrics:   class.Metrics.TimeMetrics,
			NumberMetrics: class.Metrics.NumberMetrics,
			BoolMetrics:   class.Metrics.BoolMetrics,
		}
		classes = append(classes, class)

		// Add the class to the global metrics.
		global.AddClass(class)
	}

	// Each row/class was unique, so update the global counts.
	nClasses := uint64(len(classes))
	if nClasses == 0 {
		return nil, nil
	}

	result := &report.Result{
		Global: global,
		Class:  classes,
	}

	return result, nil
}
