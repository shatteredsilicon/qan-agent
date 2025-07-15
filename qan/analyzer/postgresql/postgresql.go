package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/shatteredsilicon/ssm/proto"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/postgresql/collector"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql"
)

const (
	DefaultInterval       = 60 // in seconds
	DefaultExampleQueries = true
	DefaultCollectFrom    = "logfile"
)

func New(ctx context.Context, protoInstance proto.Instance) analyzer.Analyzer {
	// Get available services from ctx
	services, _ := ctx.Value("services").(map[string]interface{})

	// Get services we need
	logger, _ := services["logger"].(*pct.Logger)
	spool, _ := services["spool"].(data.Spooler)

	// return initialized MongoAnalyzer
	return &PGAnalyzer{
		protoInstance: protoInstance,
		spool:         spool,
		logger:        logger,
		stopChan:      make(chan struct{}),
	}
}

// PGAnalyzer
type PGAnalyzer struct {
	protoInstance proto.Instance
	logger        *pct.Logger
	spool         data.Spooler
	collector     collector.Collector
	config        analyzer.QAN
	db            *sql.DB
	sync.RWMutex       // Lock() to protect internal consistency of the service
	running       bool // Is this service running?
	stopChan      chan struct{}
}

// SetConfig sets the config
func (a *PGAnalyzer) SetConfig(setConfig analyzer.QAN) {
	a.config = setConfig
	if a.config.ExampleQueries == nil {
		v := DefaultExampleQueries
		a.config.ExampleQueries = &v
	}
	if a.config.Interval == 0 {
		a.config.Interval = DefaultInterval
	}
	if a.config.CollectFrom == "" {
		a.config.CollectFrom = DefaultCollectFrom
	}
}

// Config returns analyzer running configuration
func (a *PGAnalyzer) Config() analyzer.QAN {
	return a.config
}

// Start starts analyzer but doesn't wait until it exits
func (a *PGAnalyzer) Start() error {
	a.logger.Debug("Start:call")
	defer a.logger.Debug("Start:return")

	a.Lock()
	defer a.Unlock()
	if a.running {
		return nil
	}

	dsn := postgresql.FixDSN(a.protoInstance.DSN)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	switch a.config.CollectFrom {
	case "logfile":
		a.collector = collector.NewLogFileCollector(a.config.QAN, a.logger, db, a.spool)
	case "table":
		a.collector = collector.NewTableCollector(a.config.QAN, a.logger, db, a.spool)
	default:
		return errors.New("unspported CollectFrom option")
	}

	a.db = db
	a.collector.Prepare()

	a.running = true
	go a.run()
	return nil
}

// Status returns list of statuses
func (a *PGAnalyzer) Status() map[string]string {
	a.RLock()
	defer a.RUnlock()

	statuses := map[string]string{}
	service := a.logger.Service()

	if !a.running {
		statuses[service] = "Not running"
		return statuses
	}

	statuses[service] = "Running"
	return statuses
}

// Stop stops running analyzer, waits until it stops
func (a *PGAnalyzer) Stop() error {
	a.Lock()
	defer a.Unlock()
	if !a.running {
		return nil
	}

	a.stopChan <- struct{}{}
	a.db.Close()
	a.running = false
	return nil
}

func (a *PGAnalyzer) GetDefaults(uuid string) map[string]interface{} {
	internal := a.config.Interval
	if internal <= 0 {
		internal = DefaultInterval
	}

	exampleQueries := a.config.ExampleQueries
	if exampleQueries == nil {
		eq := DefaultExampleQueries
		exampleQueries = &eq
	}

	return map[string]interface{}{
		"Interval":       internal,
		"ExampleQueries": exampleQueries,
		"CollectFrom":    a.config.CollectFrom,
	}
}

func (*PGAnalyzer) Messages() []proto.Message {
	return []proto.Message{}
}

// String returns human readable identification of Analyzer
func (*PGAnalyzer) String() string {
	return ""
}

func (a *PGAnalyzer) run() {
	a.logger.Debug("run:call")
	defer a.logger.Debug("run:return")

	ticker := time.NewTicker(time.Duration(a.config.Interval) * time.Second)
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithCancel(context.TODO())
			a.collector.Start(ctx)
			cancel()
		case <-a.stopChan:
			a.collector.Stop()
			return
		}
	}
}
