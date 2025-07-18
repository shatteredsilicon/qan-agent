package collector

import (
	"context"
	"database/sql"
	"time"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/query"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/aggregator"
	"github.com/shatteredsilicon/ssm/proto/config"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

type exampleRow struct {
	Query      string
	QueryStart time.Time
}

// A statmentRow is a row from pg_stat_statements
type statmentRow struct {
	Datname           string
	Query             string
	QueryIDs          []int64
	Calls             uint
	TotalExecTime     float64
	MinExecTime       float64
	MaxExecTime       float64
	MeanExecTime      float64
	Rows              uint64
	SharedBlksHit     uint64
	SharedBlksRead    uint64
	SharedBlksDirtied uint64
	SharedBlksWritten uint64
}

type statement struct {
	query string
	rows  map[string]statmentRow // keyed on schema
}

type TableCollector struct {
	config        config.QAN
	logger        *pct.Logger
	db            *sql.DB
	spooler       data.Spooler
	statements    map[string]statement // keyed on classId
	examples      map[int64]exampleRow
	exampleTicker *time.Ticker
}

func NewTableCollector(config config.QAN, logger *pct.Logger, db *sql.DB, spooler data.Spooler) *TableCollector {
	return &TableCollector{
		config:        config,
		db:            db,
		logger:        logger,
		spooler:       spooler,
		statements:    make(map[string]statement),
		examples:      make(map[int64]exampleRow),
		exampleTicker: time.NewTicker(time.Millisecond * 1000),
	}
}

func (c *TableCollector) Prepare() {
	go func() {
		for range c.exampleTicker.C {
			rows, err := c.db.Query(`
				SELECT query_id, query, query_start
				FROM pg_stat_activity
			`)
			if err != nil {
				c.logger.Error(err)
				continue
			}
			defer rows.Close()

			for rows.Next() {
				var id sql.NullInt64
				var query string
				var ts sql.NullTime
				if err = rows.Scan(&id, &query, &ts); err != nil {
					c.logger.Error(err)
					break
				}
				if id.Valid && ts.Valid {
					c.examples[id.Int64] = exampleRow{
						Query:      query,
						QueryStart: ts.Time,
					}
				}
			}
		}
	}()
}

func (c *TableCollector) Stop() {
	c.exampleTicker.Stop()
}

func (c *TableCollector) Start(ctx context.Context) {
	startTime := time.Now()

	var statStatementEnable bool
	if err := c.db.QueryRowContext(ctx, "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements' and installed_version is not null").Scan(&statStatementEnable); err != nil && err != sql.ErrNoRows {
		c.logger.Error(err)
		return
	}

	if !statStatementEnable {
		c.logger.Error("pg_stat_statements is not enable")
		return
	}

	statments, err := c.getStatements()
	if err != nil {
		c.logger.Error(err)
		return
	}

	defer func() {
		if len(c.statements) == 0 {
			c.statements = statments
			return
		}

		for id, s := range statments {
			if _, ok := c.statements[id]; !ok {
				c.statements[id] = s
				continue
			}

			for schema, row := range s.rows {
				c.statements[id].rows[schema] = row
			}
		}
	}()

	if len(c.statements) == 0 {
		return
	}

	globalClass := aggregator.NewClass("", "", *c.config.ExampleQueries)
	classes := make([]*aggregator.Class, 0)
	for id, s := range statments {
		preStatement := c.statements[id]
		var totalRow statmentRow
		var example *qan.Example
		count := 0
		for _, row := range s.rows {
			preRow := preStatement.rows[row.Datname]
			if preRow.Calls == row.Calls {
				continue
			}
			if row.Calls < preRow.Calls {
				c.reset()
				return
			}
			if totalRow.Calls == 0 || row.MinExecTime < totalRow.MinExecTime {
				totalRow.MinExecTime = row.MinExecTime
			}
			totalRow.Calls += row.Calls - preRow.Calls
			totalRow.TotalExecTime += row.TotalExecTime - preRow.TotalExecTime
			if row.MaxExecTime > totalRow.MaxExecTime {
				totalRow.MaxExecTime = row.MaxExecTime
			}
			totalRow.MeanExecTime += row.MeanExecTime
			for _, queryID := range row.QueryIDs {
				if ex, ok := c.examples[queryID]; ok {
					if example == nil || float64(ex.QueryStart.Unix()) > example.QueryTime {
						example = &qan.Example{
							Db:        row.Datname,
							QueryTime: float64(ex.QueryStart.Unix()),
							Query:     ex.Query,
						}
					}
					break
				}
			}
			count += 1
		}
		if count == 0 {
			continue
		}
		totalRow.MeanExecTime = totalRow.MeanExecTime / float64(len(s.rows))

		stats := aggregator.NewMetrics()
		stats.TimeMetrics["Query_time"] = &qan.TimeStats{
			Sum: totalRow.TotalExecTime,
			Min: &totalRow.MinExecTime,
			Avg: &totalRow.MeanExecTime,
			Max: &totalRow.MaxExecTime,
		}
		stats.NumberMetrics["Shared_blks_hit"] = &qan.NumberStats{
			Sum: totalRow.SharedBlksHit,
		}
		stats.NumberMetrics["Shared_blks_read"] = &qan.NumberStats{
			Sum: totalRow.SharedBlksRead,
		}

		class := aggregator.NewClass(id, s.query, *c.config.ExampleQueries)
		if example != nil {
			class.Example = example
		}
		class.TotalQueries = totalRow.Calls
		class.Metrics = stats
		class.Class.Metrics = stats.Metrics
		classes = append(classes, class)
		globalClass.AddClass(class)
	}

	report := aggregator.NewAggregator(*c.config.ExampleQueries).MakeReport(
		c.config, startTime, time.Now(), classes, globalClass,
	)
	if err := c.spooler.Write("qan", report); err != nil {
		c.logger.Warn("Lost report: ", err)
	}
}

func (c *TableCollector) reset() {
	c.statements = make(map[string]statement)
}

func (c *TableCollector) getStatements() (map[string]statement, error) {
	rows, err := c.db.Query(`
		SELECT
			queryid,
			pg_database.datname,
			query,
			calls,
			total_exec_time,
			min_exec_time,
			max_exec_time,
			mean_exec_time,
			rows,
			shared_blks_hit,
			shared_blks_read,
			shared_blks_dirtied,
			shared_blks_written
		FROM pg_stat_statements
		JOIN pg_database ON pg_stat_statements.dbid = pg_database.oid
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	statments := make(map[string]statement)
	for rows.Next() {
		var row statmentRow
		var queryID int64
		err = rows.Scan(
			&queryID,
			&row.Datname,
			&row.Query,
			&row.Calls,
			&row.TotalExecTime,
			&row.MinExecTime,
			&row.MaxExecTime,
			&row.MeanExecTime,
			&row.Rows,
			&row.SharedBlksHit,
			&row.SharedBlksRead,
			&row.SharedBlksDirtied,
			&row.SharedBlksWritten,
		)
		if err != nil {
			return nil, err
		}
		row.QueryIDs = []int64{queryID}

		id := query.Id(row.Query)
		if s, exist := statments[id]; exist {
			if oldRow, ok := s.rows[row.Datname]; ok {
				oldRow.QueryIDs = append(oldRow.QueryIDs, row.QueryIDs...)
				s.rows[row.Datname] = oldRow
				continue
			}
			s.rows[row.Datname] = row
		} else {
			statments[id] = statement{
				query: row.Query,
				rows: map[string]statmentRow{
					row.Datname: row,
				},
			}
		}
	}

	return statments, nil
}
