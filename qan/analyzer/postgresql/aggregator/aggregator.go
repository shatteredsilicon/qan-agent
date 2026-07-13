package aggregator

import (
	"sort"
	"time"

	"github.com/shatteredsilicon/qan-agent/qan/analyzer/event"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/logparser"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
	"github.com/shatteredsilicon/ssm/proto/config"
	"github.com/shatteredsilicon/ssm/proto/qan"
)

const (
	defaultTsLength = 60
	maxEventSize    = 1024 * 1024
)

// An Aggregator groups events by class ID. When there are no more events,
// a call to Finalize computes all metric statistics and returns a Result.
// Aggregator is supposed to groups events that happened in a same unix second only.
type Aggregator struct {
	samples bool
	// --
	global    *Class
	classes   map[int64]map[string]*Class
	eventSize int64
}

// NewAggregator returns a new Aggregator.
// outlierTime is https://www.percona.com/doc/percona-server/5.5/diagnostics/slow_extended_55.html#slow_query_log_always_write_time
func NewAggregator(samples bool) *Aggregator {
	globalMetrics := NewMetrics()
	a := &Aggregator{
		samples: samples,
		// --
		global: &Class{
			Class: &qan.Class{
				Metrics: globalMetrics.Metrics,
			},
			Metrics: globalMetrics,
		},
		classes: make(map[int64]map[string]*Class),
	}
	return a
}

// AddEvent adds the event to the aggregator, automatically creating new classes
// as needed.
func (a *Aggregator) AddEvent(e *logparser.Event) {
	classes, ok := a.classes[e.LogEntry.LogTime.Unix()]
	if !ok {
		classes = make(map[string]*Class)
		a.classes[e.LogEntry.LogTime.Unix()] = classes
	}

	// We don't need to deal with User@Host for
	// global class, so make a copy and set Host
	// to empty
	a.global.AddEvent(e)

	class, ok := classes[e.ID]
	if !ok {
		class = NewClass(e.ID, e.Fingerprint, a.samples)
		classes[e.ID] = class
	}
	class.AddEvent(e)

	a.eventSize++
}

type byQueryTime []*Class

func (a byQueryTime) Len() int      { return len(a) }
func (a byQueryTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byQueryTime) Less(i, j int) bool {
	// todo: will panic if struct is incorrect
	// descending order
	return a[i].Metrics.TimeMetrics["Query_time"].Sum > a[j].Metrics.TimeMetrics["Query_time"].Sum
}

// Finalize calculates all metric statistics and returns a Result.
// Call this function when done adding events to the aggregator.
func (a *Aggregator) Finalize(config config.QAN, startTime, endTime time.Time) *report.Result {
	a.global.UniqueQueries = uint(len(a.classes))
	cls := make([]*event.Class, 0)
	for unixTs, classes := range a.classes {
		for _, class := range classes {
			class.StartAt = time.Unix(unixTs, 0)
			class.EndAt = class.StartAt
			class.UniqueQueries = 1
			if class.Example != nil && class.Example.Ts != "" {
				if t, err := time.Parse("2006-01-02 15:04:05", class.Example.Ts); err != nil {
					class.Example.Ts = ""
				} else {
					class.Example.Ts = t.Format("2006-01-02 15:04:05")
				}
			}
			cls = append(cls, &event.Class{
				Class: class.Class,
				Metrics: &event.Metrics{
					Metrics: class.Metrics.Metrics,
				},
			})
		}
	}

	return &report.Result{
		RunTime: float64(endTime.Sub(startTime)),
		Global: &event.Class{
			Class: a.global.Class,
			Metrics: &event.Metrics{
				Metrics: a.global.Metrics.Metrics,
			},
		},
		Class: cls,
	}
}

// ShouldFinalize checks whether it should finialize before
// adding a new event
func (a *Aggregator) ShouldFinalize(event *logparser.Event) bool {
	return a.classes[event.LogEntry.LogTime.Unix()] == nil && len(a.classes) >= defaultTsLength || a.eventSize >= maxEventSize
}

func (a *Aggregator) MakeReport(config config.QAN, startTime, endTime time.Time, classes []*Class, globalClass *Class) *qan.Report {
	// Sort classes by Query_time_sum, descending.
	sort.Sort(byQueryTime(classes))

	// Make qan.Report from Result and other metadata (e.g. Interval).
	report := &qan.Report{
		UUID:    config.UUID,
		StartTs: startTime,
		EndTs:   endTime,
		RunTime: float64(endTime.Sub(startTime)),
		Global:  globalClass.Class,
		Class:   make([]*qan.Class, len(classes)),
	}
	for i := range classes {
		report.Class[i] = classes[i].Class
	}

	// Return all query classes if there's no limit or number of classes is
	// less than the limit.
	n := len(classes)
	if config.ReportLimit == 0 || n <= int(config.ReportLimit) {
		return report // all classes, no LRQ
	}

	// Top queries
	report.Class = make([]*qan.Class, config.ReportLimit)
	for i := range classes[0:config.ReportLimit] {
		report.Class[i] = classes[i].Class
	}

	// Low-ranking Queries
	lrq := NewClass("lrq", "/* low-ranking queries */", false)

	// Set timestamps of lrq query class to a proper 'zero' time,
	// so it fits database's NO_ZERO_DATE restriction or
	// something like that.
	lrq.StartAt = time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC)
	lrq.EndAt = time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC)
	lrq.Example.Ts = lrq.StartAt.UTC().Format(time.DateTime)

	for _, class := range classes[config.ReportLimit:n] {
		lrq.AddClass(class)
	}
	report.Class = append(report.Class, lrq.Class)

	return report // top classes, the rest as LRQ
}
