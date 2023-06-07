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
	"strings"
	"time"

	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	pc "github.com/shatteredsilicon/ssm/proto/config"
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
	Error      string         `json:",omitempty"`
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

func MakeReport(config pc.QAN, startTime, endTime time.Time, interval *iter.Interval, result *Result, logger *pct.Logger) *qan.Report {
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
	for i := range result.Class {
		report.Class[i] = result.Class[i].Class
		if logger != nil && report.Class[i] != nil && report.Class[i].Fingerprint != "" && report.Class[i].Example != nil && report.Class[i].Example.Query == "" {
			classBytes, _ := json.Marshal(*report.Class[i])
			logger.Debug("MakeReport got an non-empty fingerprint and empty query example class: %s", string(classBytes))
		}
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

	// Return all query classes if there's no limit or number of classes is
	// less than the limit.
	n := len(result.Class)
	if config.ReportLimit == 0 || n <= int(config.ReportLimit) {
		return report // all classes, no LRQ
	}

	// Top queries
	report.Class = make([]*qan.Class, config.ReportLimit)
	for i := range result.Class[0:config.ReportLimit] {
		report.Class[i] = result.Class[i].Class
	}

	// Low-ranking Queries
	lrq := event.NewClass("lrq", "/* low-ranking queries */", false)
	for _, class := range result.Class[config.ReportLimit:n] {
		lrq.AddClass(class)
	}
	report.Class = append(report.Class, lrq.Class)

	return report // top classes, the rest as LRQ
}

// MergeResult merges srcResult into destResult
func MergeResult(destResult, srcResult Result) Result {
	if destResult.Global == nil {
		destResult.Global = srcResult.Global
	} else if srcResult.Global != nil {
		destResult.Global.AddClass(srcResult.Global)
	}

	if destResult.Class == nil || len(destResult.Class) == 0 {
		destResult.Class = srcResult.Class
	} else {
		classes := make(map[string]*event.Class)
		for _, class := range destResult.Class {
			classes[class.Id] = class
		}

		for _, class := range srcResult.Class {
			if _, ok := classes[class.Id]; ok {
				classes[class.Id].AddClass(class)
			} else {
				classes[class.Id] = class
				destResult.Class = append(destResult.Class, class)
			}
		}
	}

	destResult.RunTime += srcResult.RunTime
	if srcResult.StopOffset > destResult.StopOffset {
		destResult.StopOffset = srcResult.StopOffset
	}
	if len(srcResult.Error) > 0 {
		if len(destResult.Error) > 0 {
			destResult.Error = strings.Join([]string{destResult.Error, srcResult.Error}, " | ")
		} else {
			destResult.Error = srcResult.Error
		}
	}

	if destResult.StartTime.IsZero() {
		destResult.StartTime = srcResult.StartTime
	}
	if destResult.EndTime.Before(srcResult.EndTime) {
		destResult.EndTime = srcResult.EndTime
	}

	return destResult
}
