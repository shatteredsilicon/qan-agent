package worker

import (
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/config"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/iter"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/report"
)

// A Worker gets queries, aggregates them, and returns a Result. Workers are ran
// by Analyzers. When ran, MySQL is presumed to be configured and ready.
type Worker interface {
	Setup(*iter.Interval, chan *report.Result) error
	Run() (*report.Result, error)
	Stop() error
	Cleanup() error
	Status() map[string]string
	SetConfig(config.QAN)
}
