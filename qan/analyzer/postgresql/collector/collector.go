package collector

import "context"

type Collector interface {
	Prepare() error
	Start(context.Context)
	Stop()
}
