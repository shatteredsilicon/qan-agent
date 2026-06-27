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

package report

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

// slowlog|perf schema --> Result --> qan.Report --> data.Spooler

// Data for an interval from slow log or performance schema (pfs) parser,
// passed to MakeReport() which transforms into a qan.Report{}.
type Result struct {
	Global     *event.Class   // metrics for all data
	Class      []*event.Class // per-class metrics
	RateLimit  uint           // Percona Server rate limit
	RunTime    float64        // seconds parsing data, hopefully < interval
	StopOffset int64          // slow log offset where parsing stopped, should be <= end offset
	StartTime  time.Time
	EndTime    time.Time
}

type ByQueryTime []*event.Class

func (a ByQueryTime) Len() int      { return len(a) }
func (a ByQueryTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByQueryTime) Less(i, j int) bool {
	// todo: will panic if struct is incorrect
	// descending order
	return a[i].Metrics.TimeMetrics["Query_time"].Sum > a[j].Metrics.TimeMetrics["Query_time"].Sum
}

func MakeReport(
	config analyzer.QAN,
	startTime, endTime time.Time,
	interval *iter.Interval,
	result *Result,
	logger *pct.Logger,
	prefetchMetadataHandler func(*event.Class) error,
) *qan.Report {
	// Sort classes by Query_time_sum, descending.
	sort.Sort(ByQueryTime(result.Class))

	// Make qan.Report from Result and other metadata (e.g. Interval).
	report := &qan.Report{
		UUID:    config.UUID,
		StartTs: startTime,
		EndTs:   endTime,
		RunTime: result.RunTime,
		Global:  result.Global.Class,
		Class:   make([]*qan.Class, len(result.Class)),
	}

	processedI := 0
	for ; processedI < len(result.Class) && (config.ReportLimit == 0 || processedI < int(config.ReportLimit)); processedI++ {
		if err := prefetchMetadataHandler(result.Class[processedI]); err != nil {
			logger.Error("got an error when prefetching metadata:", err)
		}

		report.Class[processedI] = result.Class[processedI].Class
		if logger != nil && report.Class[processedI] != nil && report.Class[processedI].Fingerprint != "" && report.Class[processedI].Example != nil && report.Class[processedI].Example.Query == "" {
			classBytes, _ := json.Marshal(*report.Class[processedI])
			logger.Debug("MakeReport got an non-empty fingerprint and empty query example class: %s", string(classBytes))
		}
	}
	report.Class = report.Class[:processedI]

	if config.ReportLimit > 0 && processedI >= int(config.ReportLimit) && len(result.Class) > processedI { // LRQ
		// Low-ranking Queries
		lrq := event.NewClass("lrq", "/* low-ranking queries */", false)

		// Set timestamps of lrq query class to a proper 'zero' time,
		// so it fits database's NO_ZERO_DATE restriction or
		// something like that.
		lrq.StartAt = time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC)
		lrq.EndAt = time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC)
		lrq.Example.Ts = lrq.StartAt.UTC().Format(time.DateTime)

		for _, class := range result.Class[processedI:] {
			lrq.AddClass(class)
		}
		report.Class = append(report.Class, lrq.Class)
	}

	if interval != nil {
		size, err := pct.FileSize(interval.Filename)
		if err != nil {
			size = 0
		}

		// slow log data
		report.SlowLogFile = interval.Filename
		report.SlowLogFileSize = size
		report.StartOffset = interval.StartOffset
		report.EndOffset = interval.EndOffset
		report.StopOffset = result.StopOffset
		report.RateLimit = result.RateLimit
	}

	return report
}
