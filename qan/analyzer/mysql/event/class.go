/*
Copyright (c) 2019, Percona LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package event

import (
	"encoding/json"

	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/log"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

const (
	// MaxExampleBytes defines to how many bytes truncate a query.
	MaxExampleBytes = 2 * 1024 * 10

	// TruncatedExampleSuffix is added to truncated query.
	TruncatedExampleSuffix = "..."
)

type Class struct {
	*qan.Class
	Metrics *Metrics
}

// NewClass returns a new Class for the class ID and fingerprint.
// If sample is true, the query with the greatest Query_time is saved.
func NewClass(id, fingerprint string, sample bool) *Class {
	m := NewMetrics()
	class := &Class{
		Class: &qan.Class{
			Id:           id,
			Fingerprint:  fingerprint,
			Metrics:      m.Metrics,
			TotalQueries: 0,
			Example:      &qan.Example{},
			Sample:       sample,
			UserSources:  make([]qan.UserSource, 0),
		},
		Metrics: m,
	}
	return class
}

// AddEvent adds an event to the query class.
func (c *Class) AddEvent(e *log.Event, outlier bool) {
	if outlier {
		c.Outliers++
	} else {
		c.TotalQueries++
	}

	c.Metrics.AddEvent(e, outlier)

	// Save last db seen for this query. This helps ensure the sample query
	// has a db.
	if e.Db != "" {
		c.LastDb = e.Db
	}
	if c.Sample {
		if n, ok := e.TimeMetrics["Query_time"]; ok {
			if float64(n) > c.Example.QueryTime {
				c.Example.QueryTime = float64(n)
				c.Example.Size = len(e.Query)
				if e.Db != "" {
					c.Example.Db = e.Db
				} else {
					c.Example.Db = c.LastDb
				}
				if len(e.Query) > MaxExampleBytes {
					c.Example.Query = e.Query[0:MaxExampleBytes-len(TruncatedExampleSuffix)] + TruncatedExampleSuffix
				} else {
					c.Example.Query = e.Query
				}
				if len(e.ExplainRows) > 0 {
					explainBytes, _ := json.Marshal(e.ExplainRows)
					c.Example.Explain = string(explainBytes)
				}
				if !e.Ts.IsZero() {
					// todo use time.RFC3339Nano instead
					c.Example.Ts = e.Ts.UTC().Format("2006-01-02 15:04:05")
				}
			}
		}
	}

	if e.Host != "" {
		merged := false
		for i := len(c.UserSources) - 1; i >= 0; i-- {
			// Try to merge user@host rows that are in the same unix timestamp,
			// this list is in timestamp ASC order, so we loop this list backward,
			// and exit the loop once two unix timestamp don't match
			if c.UserSources[i].Ts.Unix() != e.Ts.Unix() {
				break
			}
			if c.UserSources[i].User == e.User && c.UserSources[i].Host == e.Host {
				c.UserSources[i].Count++
				merged = true
				break
			}
		}
		if !merged {
			c.UserSources = append(c.UserSources, qan.UserSource{
				Ts:    e.Ts,
				User:  e.User,
				Host:  e.Host,
				Count: 1,
			})
		}
	}
}

// AddClass adds a Class to the current class. This is used with pre-aggregated classes.
func (c *Class) AddClass(newClass *Class) {
	c.UniqueQueries++
	c.TotalQueries += newClass.TotalQueries
	c.UserSources = append(c.UserSources, newClass.UserSources...)

	for newMetric, newStats := range newClass.Metrics.TimeMetrics {
		stats, ok := c.Metrics.TimeMetrics[newMetric]
		if !ok {
			m := *newStats
			c.Metrics.TimeMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
			stats.Avg = Float64(stats.Sum / float64(c.TotalQueries))
			if Float64Value(newStats.Min) < Float64Value(stats.Min) || stats.Min == nil {
				stats.Min = newStats.Min
			}
			if Float64Value(newStats.Max) > Float64Value(stats.Max) || stats.Max == nil {
				stats.Max = newStats.Max
			}
		}
	}

	for newMetric, newStats := range newClass.Metrics.NumberMetrics {
		stats, ok := c.Metrics.NumberMetrics[newMetric]
		if !ok {
			m := *newStats
			c.Metrics.NumberMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
			stats.Avg = Uint64(stats.Sum / uint64(c.TotalQueries))
			if Uint64Value(newStats.Min) < Uint64Value(stats.Min) || stats.Min == nil {
				stats.Min = newStats.Min
			}
			if Uint64Value(newStats.Max) > Uint64Value(stats.Max) || stats.Max == nil {
				stats.Max = newStats.Max
			}
		}
	}

	for newMetric, newStats := range newClass.Metrics.BoolMetrics {
		stats, ok := c.Metrics.BoolMetrics[newMetric]
		if !ok {
			m := *newStats
			c.Metrics.BoolMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
		}
	}
}

// Finalize calculates all metric statistics. Call this function when done
// adding events to the class.
func (c *Class) Finalize(rateLimit uint) {
	if rateLimit == 0 {
		rateLimit = 1
	}
	c.TotalQueries = (c.TotalQueries * rateLimit) + c.Outliers
	c.Metrics.Finalize(rateLimit, c.TotalQueries)
	if c.Example.QueryTime == 0 {
		c.Example = nil
	}
}
