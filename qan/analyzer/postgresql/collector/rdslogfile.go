package collector

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRDS "github.com/aws/aws-sdk-go/service/rds"

	"github.com/shatteredsilicon/qan-agent/agent"
	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/aggregator"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/logparser"
	"github.com/shatteredsilicon/qan-agent/rds"
	"github.com/shatteredsilicon/ssm/proto"
	"github.com/shatteredsilicon/ssm/proto/config"
)

const (
	longQueryTimeMultiple          = 2
	rateLmitMaximumTimes           = 2
	minimumLogMinDurationStatement = 1
	defaultThrottlingRetention     = 7 * 24 * time.Hour // 7 days
)

type RDSLogFileCollector struct {
	config                           config.QAN
	logger                           *pct.Logger
	spooler                          data.Spooler
	records                          map[string]*rdsLogFileRecord
	rds                              *rds.Service
	lastRDSLogWritten                *int64
	ignoreRateLimit                  bool        // wether to ignore rate limit error or not
	rateLimitLogMinDurationStatement int64       // stores the long_query_time value at the moment that rate limit happened
	rateLimitTimestamps              []time.Time // stores the timestamps when the historical rate limit happened
}

func NewRDSLogFileCollector(config config.QAN, logger *pct.Logger, spooler data.Spooler) *RDSLogFileCollector {
	return &RDSLogFileCollector{
		config:  config,
		logger:  logger,
		spooler: spooler,
		records: make(map[string]*rdsLogFileRecord),
	}
}

func (c *RDSLogFileCollector) Prepare() error {
	return c.setupRDS()
}

func (c *RDSLogFileCollector) Stop() {}

func (c *RDSLogFileCollector) Start(ctx context.Context) {
	loggingEnable, err := c.rds.GetParam("logging_collector")
	if err != nil {
		c.logger.Error("failed to check logging_collector parameter:", err)
		return
	}
	if loggingEnable.ParameterValue == nil || *loggingEnable.ParameterValue != rds.TrueParamValue {
		c.logger.Error("logging_collector parameter is not set")
		return
	}

	logDestination, err := c.rds.GetParam("log_destination")
	if err != nil {
		c.logger.Error("failed to check log_destination parameter:", err)
		return
	}
	if logDestination.ParameterValue == nil {
		c.logger.Error("Not supported log_destination")
		return
	}

	logParserFunc := logparser.GetLogParserFuncs(*logDestination.ParameterValue)
	if logParserFunc == nil {
		c.logger.Error("Not supported log_destination")
		return
	}

	logMinDurationStatement, err := c.rds.GetParam("log_min_duration_statement")
	if err != nil {
		return
	}
	if logMinDurationStatement.ParameterValue == nil {
		c.logger.Warn("Got an empty log_min_duration_statement value")
		c.ignoreRateLimit = true
	} else {
		lmds, err := strconv.ParseInt(*logMinDurationStatement.ParameterValue, 10, 64)
		if err != nil {
			c.logger.Warn("Got an invalid log_min_duration_statement value: ", *logMinDurationStatement.ParameterValue)
			c.ignoreRateLimit = true
		} else {
			c.ignoreRateLimit = false
			if !c.hasRateLimitAlert() {
				c.rateLimitLogMinDurationStatement = lmds
			} else if lmds >= c.adviceLogMinDurationStatement() || c.throttlingStateExpired() {
				c.rateLimitLogMinDurationStatement = lmds
				c.rateLimitTimestamps = []time.Time{}
			}
		}
	}

	startTime := time.Now().UTC()
	if c.lastRDSLogWritten == nil || *c.lastRDSLogWritten == 0 {
		c.logger.Debug("Log parsing cancelled for first turn")
		startTimeMilli := startTime.UnixMilli()
		c.lastRDSLogWritten = &startTimeMilli
		return
	}

	logParser := logParserFunc()
	logRoutineChan := make(chan struct{}, parseRoutines)
	logEventChan := make(chan *logparser.Event, parseRoutines)
	var wg sync.WaitGroup

	startupRDSLogWritten := c.lastRDSLogWritten
	files, err := c.rds.GetLogFiles(startupRDSLogWritten, nil)
	if err != nil {
		c.logger.Error(fmt.Sprintf("fetching rds log files failed: %+v", err))
		return
	}

	if len(files) == 0 {
		c.logger.Debug("Log parsing cancelled for this turn, because there are no log files to process")
		startTimeMilli := startTime.UnixMilli()
		c.lastRDSLogWritten = &startTimeMilli
		return
	}

	stopC := make(chan struct{})
	go func() {
		ag := aggregator.NewAggregator(*c.config.ExampleQueries)
		startTime := time.Now()
		for {
			select {
			case e := <-logEventChan:
				lastLogWritten := e.LogEntry.LogTime.UnixMilli()
				if !e.LogEntry.LogTime.IsZero() && lastLogWritten < *startupRDSLogWritten {
					// log entry before the time we are concerned with, ignore it
					continue
				}
				if lastLogWritten > *c.lastRDSLogWritten {
					c.lastRDSLogWritten = &lastLogWritten
				}

				if q := strings.TrimSpace(e.Query); q == "" || q == ";" {
					continue
				}
				e.AttemptToResolveParams()
				if ag.ShouldFinalize(e) {
					report := ag.Finalize(c.config, startTime, time.Now())
					ag = aggregator.NewAggregator(true)
					startTime = time.Now()
					if err := c.spooler.Write("qan", report); err != nil {
						c.logger.Warn("Lost report: ", err)
					}
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

	records := make(map[string]*rdsLogFileRecord)
	for _, file := range files {
		c.logger.Debug(fmt.Sprintf("Start prasing log file %s", *file.LogFileName))

		filename := filepath.Base(*file.LogFileName)
		if !logParser.IsFileAcceptable(filename) {
			c.logger.Debug(fmt.Sprintf("Log file %s is not acceptable", *file.LogFileName))
			continue
		}

		record, recordExists := c.records[filename]
		if !recordExists || record == nil {
			zeroMarker := rds.ZeroMarker
			record = &rdsLogFileRecord{
				previousData: []byte{},
				marker:       &zeroMarker,
			}
		}
		records[filename] = record

		logRoutineChan <- struct{}{}
		wg.Add(1)
		go func(ctx context.Context, f *awsRDS.DescribeDBLogFilesDetails, r *rdsLogFileRecord, p logparser.LogParser, ch chan *logparser.Event) {
			defer func() {
				<-logRoutineChan
				wg.Done()
			}()

			for {
				numberOfLines := rds.DefaultNumberOfLines
				dataOutput, err := c.rds.DownloadDBLogFilePortion(f.LogFileName, r.marker, &numberOfLines)
				if err != nil {
					c.logger.Error(fmt.Sprintf("downloading rds log file %s failed: %+v", *file.LogFileName, err))

					awsErr, ok := err.(awserr.Error)
					if !ok || c.ignoreRateLimit || c.hasRateLimitAlert() {
						return
					}

					// check if it's a throttling error
					messages := strings.ToLower(awsErr.Message())
					if !strings.Contains(messages, "rate exceed") && !strings.Contains(messages, "quota exceed") {
						return
					}

					now := time.Now().UTC()
					c.rateLimitTimestamps = append(c.rateLimitTimestamps, now)
					return
				}

				shouldBreak := dataOutput.AdditionalDataPending == nil || !(*dataOutput.AdditionalDataPending) || dataOutput.Marker == nil

				data := bytes.NewBuffer(r.previousData)
				if dataOutput.LogFileData != nil {
					data.WriteString(*dataOutput.LogFileData)
				}
				dataStr := data.String()

				var completeLog string
				var incompleteLog []byte
				if shouldBreak {
					completeLog = dataStr
				} else {
					// Leave last line to next batch in case it's not a complete log
					lastNewLinePos := strings.LastIndex(dataStr, "\n")
					completeLog = dataStr[:lastNewLinePos]
					if lastNewLinePos < len(dataStr)-1 {
						incompleteLog = []byte(dataStr[lastNewLinePos+1:])
					}
				}

				if err := p.Parse(ctx, bytes.NewReader([]byte(completeLog)), ch); err != nil {
					c.logger.Error("failed to parse file", *f.LogFileName, ":", err)
					return
				}

				if dataOutput.Marker != nil {
					r.marker = dataOutput.Marker
				}
				r.previousData = incompleteLog

				if shouldBreak {
					break
				}
			}
		}(ctx, file, record, logParser, logEventChan)
		logParser = logParserFunc()
	}
	wg.Wait()

	c.records = records
	stopC <- struct{}{}
}

func (c *RDSLogFileCollector) setupRDS() error {
	agentConfigFile := pct.Basedir.ConfigFile("agent")
	agentConfig := &agent.AgentConfig{}
	if _, err := pct.Basedir.ReadConfig("agent", agentConfig); err != nil {
		c.logger.Error(fmt.Sprintf("Error decoding agent config file %s: %s\n", agentConfigFile, err.Error()))
		return err
	}

	rdsSvcDetail, err := rds.GetRDSServiceDetail(*agentConfig, c.config)
	if err != nil {
		c.logger.Error(fmt.Sprintf("Error fetching rds service detail: %s\n", err.Error()))
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
		c.logger.Error(fmt.Sprintf("Error initializing aws session %s\n", err.Error()))
		return err
	}

	c.rds = rds.NewService(awsRDS.New(s), rdsSvcDetail.Instance)
	return nil
}

func (c *RDSLogFileCollector) hasRateLimitAlert() bool {
	return len(c.rateLimitTimestamps) >= rateLmitMaximumTimes
}

func (c *RDSLogFileCollector) adviceLogMinDurationStatement() int64 {
	if c.rateLimitLogMinDurationStatement < minimumLogMinDurationStatement {
		return minimumLogMinDurationStatement
	} else {
		return c.rateLimitLogMinDurationStatement * 2
	}
}

func (c *RDSLogFileCollector) throttlingStateExpired() bool {
	if len(c.rateLimitTimestamps) == 0 {
		return false
	}

	throttlingRetention := defaultThrottlingRetention
	if os.Getenv("THROTTLING_RETENTION") != "" {
		retention, _ := time.ParseDuration(os.Getenv("THROTTLING_RETENTION"))
		if retention > 0 {
			throttlingRetention = retention
		}
	}

	return time.Now().UTC().Sub(c.rateLimitTimestamps[0]) >= throttlingRetention
}

func (c *RDSLogFileCollector) throttlingCount() (past1Day uint, past7Days uint) {
	now := time.Now().UTC()
	for i := range c.rateLimitTimestamps {
		hours := now.UTC().Sub(c.rateLimitTimestamps[i]).Hours()
		if hours < 24 {
			past1Day += 1
		}
		if hours < 7*24 {
			past7Days += 1
		}
	}
	return
}

// Messages returns all messages in response to Messages command
func (c *RDSLogFileCollector) Messages() []proto.Message {
	if c.hasRateLimitAlert() && !c.throttlingStateExpired() {
		adviceLogMinDurationStatement := c.adviceLogMinDurationStatement()
		timesP1D, timesP7Ds := c.throttlingCount()
		return []proto.Message{
			{
				Content: fmt.Sprintf("Throttling encountered while harvesting slow query log, %d times past 1 day, %d times past 7 days. Current log_min_duration_statement = %d, consider increasing to %d", timesP1D, timesP7Ds, c.rateLimitLogMinDurationStatement, adviceLogMinDurationStatement),
			},
		}
	}

	return []proto.Message{}
}

type rdsLogFileRecord struct {
	marker       *string
	previousData []byte
}
