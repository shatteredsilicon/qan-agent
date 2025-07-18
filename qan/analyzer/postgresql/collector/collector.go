package collector

import "context"

type Collector interface {
	Prepare()
	Start(context.Context)
	Stop()
}
