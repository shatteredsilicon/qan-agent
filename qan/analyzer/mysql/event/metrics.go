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
	"sort"

	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/log"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

// Metrics encapsulate the metrics of an event like Query_time and Rows_sent.
type Metrics struct {
	*qan.Metrics
}

// NewMetrics returns a pointer to an initialized Metrics structure.
func NewMetrics() *Metrics {
	m := &Metrics{
		Metrics: &qan.Metrics{
			TimeMetrics:   make(map[string]*qan.TimeStats),
			NumberMetrics: make(map[string]*qan.NumberStats),
			BoolMetrics:   make(map[string]*qan.BoolStats),
		},
	}
	return m
}

// AddEvent saves all the metrics of the event.
func (m *Metrics) AddEvent(e *log.Event, outlier bool) {

	for metric, val := range e.TimeMetrics {
		stats, seenMetric := m.TimeMetrics[metric]
		if !seenMetric {
			m.TimeMetrics[metric] = &qan.TimeStats{
				Vals: []float64{},
			}
			stats = m.TimeMetrics[metric]
		}
		if outlier {
			stats.OutlierSum += val
		} else {
			stats.Sum += val
		}
		stats.Vals = append(stats.Vals, float64(val))
	}

	for metric, val := range e.NumberMetrics {
		stats, seenMetric := m.NumberMetrics[metric]
		if !seenMetric {
			m.NumberMetrics[metric] = &qan.NumberStats{
				Vals: []uint64{},
			}
			stats = m.NumberMetrics[metric]
		}
		if outlier {
			stats.OutlierSum += val
		} else {
			stats.Sum += val
		}
		stats.Vals = append(stats.Vals, val)
	}

	for metric, val := range e.BoolMetrics {
		stats, seenMetric := m.BoolMetrics[metric]
		if !seenMetric {
			stats = &qan.BoolStats{}
			m.BoolMetrics[metric] = stats
		}
		if val {
			if outlier {
				stats.OutlierSum++
			} else {
				stats.Sum++
			}
		}
	}
}

type byUint64 []uint64

func (a byUint64) Len() int      { return len(a) }
func (a byUint64) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byUint64) Less(i, j int) bool {
	return a[i] < a[j] // ascending order
}

// Finalize calculates the statistics of the added metrics. Call this function
// when done adding events.
func (m *Metrics) Finalize(rateLimit uint, totalQueries uint) {
	if rateLimit == 0 {
		rateLimit = 1
	}

	for _, s := range m.TimeMetrics {
		sort.Float64s(s.Vals)
		cnt := len(s.Vals)

		s.Min = Float64(s.Vals[0])
		s.Med = Float64(s.Vals[(50*cnt)/100]) // median = 50th percentile
		s.P95 = Float64(s.Vals[(95*cnt)/100])
		s.Max = Float64(s.Vals[cnt-1])
		s.Sum = (s.Sum * float64(rateLimit)) + s.OutlierSum
		s.Avg = Float64(s.Sum / float64(totalQueries))
	}

	for _, s := range m.NumberMetrics {
		sort.Sort(byUint64(s.Vals))
		cnt := len(s.Vals)

		s.Min = Uint64(s.Vals[0])
		s.Med = Uint64(s.Vals[(50*cnt)/100]) // median = 50th percentile
		s.P95 = Uint64(s.Vals[(95*cnt)/100])
		s.Max = Uint64(s.Vals[cnt-1])
		s.Sum = (s.Sum * uint64(rateLimit)) + s.OutlierSum
		s.Avg = Uint64(s.Sum / uint64(totalQueries))
	}

	for _, s := range m.BoolMetrics {
		s.Sum = (s.Sum * uint64(rateLimit)) + s.OutlierSum
	}
}

// Float64 returns a pointer to the float64 value passed in.
func Float64(v float64) *float64 {
	return &v
}

// Float64Value returns the value of the float64 pointer passed in or
// 0 if the pointer is nil.
func Float64Value(v *float64) float64 {
	if v != nil {
		return *v
	}
	return 0
}

// Uint64 returns a pointer to the uint64 value passed in.
func Uint64(v uint64) *uint64 {
	return &v
}

// Uint64Value returns the value of the uint64 pointer passed in or
// 0 if the pointer is nil.
func Uint64Value(v *uint64) uint64 {
	if v != nil {
		return *v
	}
	return 0
}
