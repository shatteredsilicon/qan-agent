package aggregator

import (
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/logparser"
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
	*Metrics
}

// NewClass returns a new Class for the class ID and fingerprint.
// If sample is true, the query with the greatest Query_time is saved.
func NewClass(id, fingerprint string, sample bool) *Class {
	metrics := NewMetrics()
	class := &Class{
		Class: &qan.Class{
			Id:           id,
			Fingerprint:  fingerprint,
			Metrics:      metrics.Metrics,
			TotalQueries: 0,
			Example:      &qan.Example{},
			Sample:       sample,
			UserSources:  make([]qan.UserSource, 0),
		},
		Metrics: metrics,
	}
	return class
}

// AddEvent adds an event to the query class.
func (c *Class) AddEvent(e *logparser.Event) {
	c.TotalQueries++
	c.Metrics.AddEvent(e)

	if c.Sample {
		if n, ok := e.TimeMetrics["Query_time"]; ok {
			if float64(n) >= c.Example.QueryTime { // if two log event have same Query_time, use the later one
				c.Example.QueryTime = float64(n)
				c.Example.Size = len(e.Query)
				c.Example.Db = e.LogEntry.DatabaseName
				if len(e.Query) > MaxExampleBytes {
					c.Example.Query = e.Query[0:MaxExampleBytes-len(TruncatedExampleSuffix)] + TruncatedExampleSuffix
				} else {
					c.Example.Query = e.Query
				}
				if !e.LogEntry.LogTime.IsZero() && e.LogEntry.LogTime.Unix() > 0 {
					// todo use time.RFC3339Nano instead
					c.Example.Ts = e.LogEntry.LogTime.UTC().Format("2006-01-02 15:04:05")
				}
			}
		}
	}

	if !e.LogEntry.LogTime.IsZero() && e.LogEntry.LogTime.Unix() > 0 && (c.StartAt.IsZero() || e.LogEntry.LogTime.Before(c.StartAt)) {
		c.StartAt = e.LogEntry.LogTime.Time
	}
	if !e.LogEntry.LogTime.IsZero() && e.LogEntry.LogTime.Unix() > 0 && e.LogEntry.LogTime.After(c.EndAt) {
		c.EndAt = e.LogEntry.LogTime.Time
	}
}

// AddClass adds a Class to the current class. This is used with pre-aggregated classes.
func (c *Class) AddClass(newClass *Class) {
	c.UniqueQueries++
	c.TotalQueries += newClass.TotalQueries
	c.UserSources = append(c.UserSources, newClass.UserSources...)

	for newMetric, newStats := range newClass.TimeMetrics {
		stats, ok := c.TimeMetrics[newMetric]
		if !ok {
			m := *newStats
			c.TimeMetrics[newMetric] = &m
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

	for newMetric, newStats := range newClass.NumberMetrics {
		stats, ok := c.NumberMetrics[newMetric]
		if !ok {
			m := *newStats
			c.NumberMetrics[newMetric] = &m
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

	for newMetric, newStats := range newClass.BoolMetrics {
		stats, ok := c.BoolMetrics[newMetric]
		if !ok {
			m := *newStats
			c.BoolMetrics[newMetric] = &m
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
	c.Metrics.Finalize(c.TotalQueries)
	if c.Example.QueryTime == 0 {
		c.Example = nil
	}
}
