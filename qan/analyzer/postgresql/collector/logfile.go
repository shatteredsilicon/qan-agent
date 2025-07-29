package collector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/aggregator"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/logparser"
	"github.com/shatteredsilicon/ssm/proto/config"
)

var (
	parseRoutines = runtime.NumCPU()
)

type nullBool sql.NullBool

// Scan implements the [Scanner] interface.
func (n *nullBool) Scan(value any) error {
	err := (*sql.NullBool)(n).Scan(value)
	if err == nil {
		return nil
	}

	switch s := value.(type) {
	case string:
		if s == "ON" || s == "on" {
			n.Bool, n.Valid = true, true
		} else if s == "OFF" || s == "off" {
			n.Bool, n.Valid = false, true
		} else {
			return err
		}
	default:
		return err
	}

	return nil
}

func (n nullBool) Value() (driver.Value, error) {
	return sql.NullBool(n).Value()
}

type logFileRecord struct {
	pos  int64
	info os.FileInfo
}

type LogFileCollector struct {
	config              config.QAN
	logger              *pct.Logger
	db                  *sql.DB
	spooler             data.Spooler
	files               map[string]logFileRecord
	modTimeOfLatestFile time.Time
}

func NewLogFileCollector(config config.QAN, logger *pct.Logger, db *sql.DB, spooler data.Spooler) *LogFileCollector {
	return &LogFileCollector{
		config:  config,
		db:      db,
		logger:  logger,
		spooler: spooler,
		files:   make(map[string]logFileRecord),
	}
}

func (c *LogFileCollector) Prepare() error { return nil }
func (c *LogFileCollector) Stop()          {}

func (c *LogFileCollector) Start(ctx context.Context) {
	var loggingEnable nullBool
	var logDestination, dataDir, logFilename, logDir string
	if err := c.db.QueryRowContext(ctx, "SELECT current_setting('logging_collector'),  current_setting('log_destination'), current_setting('data_directory'), current_setting('log_filename'), current_setting('log_directory')").Scan(&loggingEnable, &logDestination, &dataDir, &logFilename, &logDir); err != nil {
		c.logger.Error(err)
		return
	}

	if !loggingEnable.Valid || !loggingEnable.Bool {
		c.logger.Error("logging_collector parameter is not set")
		return
	}

	logParserFunc := logparser.GetLogParserFuncs(logDestination)
	if logParserFunc == nil {
		c.logger.Error("Not supported log_destination")
		return
	}

	logParser := logParserFunc()
	logRoutineChan := make(chan struct{}, parseRoutines)
	logEventChan := make(chan logparser.Event, parseRoutines)
	var wg sync.WaitGroup

	if !path.IsAbs(logDir) {
		logDir = path.Join(dataDir, logDir)
	}

	entries, err := os.ReadDir(logDir)
	if err != nil {
		c.logger.Error(err)
		return
	}

	stopC := make(chan struct{})
	go func() {
		ag := aggregator.NewAggregator(*c.config.ExampleQueries)
		startTime := time.Now()
		for {
			select {
			case e := <-logEventChan:
				if q := strings.TrimSpace(e.Query); q == "" || q == ";" {
					continue
				}
				if ag.ShouldFinalize(e) {
					report := ag.Finalize(c.config, startTime, time.Now())
					ag = aggregator.NewAggregator(true)
					startTime = time.Now()
					if err := c.spooler.Write("qan", report); err != nil {
						c.logger.Warn("Lost report: ", err)
					}
					continue
				}
				ag.AddEvent(e)
			case <-stopC:
			case <-ctx.Done():
				report := ag.Finalize(c.config, startTime, time.Now())
				if len(report.Class) == 0 {
					return
				}
				if err := c.spooler.Write("qan", report); err != nil {
					c.logger.Warn("Lost report: ", err)
				}
				return
			}
		}
	}()

	files := make(map[string]logFileRecord)
	var modTimeOfLatestFile time.Time
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !logParser.IsFileAcceptable(entry) {
			continue
		}

		filename := entry.Name()
		filepath := path.Join(logDir, filename)
		record, recordExists := c.files[filename]

		logFileInfo, err := os.Stat(filepath)
		if err != nil {
			c.logger.Error("failed to get file info of", filepath, ":", err)
			if recordExists {
				files[filename] = record
			}
			continue
		}

		if c.modTimeOfLatestFile.IsZero() {
			files[filename] = logFileRecord{
				info: logFileInfo,
				pos:  logFileInfo.Size(),
			}
			if logFileInfo.ModTime().After(modTimeOfLatestFile) {
				modTimeOfLatestFile = logFileInfo.ModTime()
			}
			continue
		}

		if logFileInfo.ModTime().Before(c.modTimeOfLatestFile) {
			continue
		}

		logFile, err := os.Open(filepath)
		if err != nil {
			c.logger.Error("failed to open file", filepath, ":", err)
			if recordExists {
				files[filename] = record
			}
			continue
		}

		if recordExists && os.SameFile(record.info, logFileInfo) {
			if logFileInfo.Size() <= record.pos {
				continue
			}

			if _, err := logFile.Seek(record.pos, io.SeekStart); err != nil {
				c.logger.Error("failed to seek file", filepath, "at pos", record.pos, ":", err)
				files[filename] = record
				continue
			}
		}

		logRoutineChan <- struct{}{}
		wg.Add(1)
		go func(ctx context.Context, f *os.File, fi os.FileInfo, p logparser.LogParser, ch chan logparser.Event) {
			defer func() {
				<-logRoutineChan
				wg.Done()
				f.Close()
			}()

			if err := p.Parse(ctx, f, ch); err != nil {
				c.logger.Error("failed to parse file", fi.Name(), ":", err)
				return
			}

			filePos, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				c.logger.Error("failed to get seek pos of file", fi.Name(), ":", err)
				filePos = fi.Size()
			}

			files[fi.Name()] = logFileRecord{
				info: fi,
				pos:  filePos,
			}

			if fi.ModTime().After(modTimeOfLatestFile) {
				modTimeOfLatestFile = fi.ModTime()
			}
		}(ctx, logFile, logFileInfo, logParser, logEventChan)
		logParser = logParserFunc()
	}
	wg.Wait()

	c.files = files
	if !modTimeOfLatestFile.IsZero() {
		c.modTimeOfLatestFile = modTimeOfLatestFile
	}

	stopC <- struct{}{}
}
