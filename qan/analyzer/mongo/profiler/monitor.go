package profiler

import (
	"fmt"
	"sync"

	pc "github.com/shatteredsilicon/ssm/proto/config"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mongo/profiler/aggregator"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mongo/profiler/collector"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mongo/profiler/parser"
)

func NewMonitor(
	mongoClient *mongo.Client,
	dbName string,
	aggregator *aggregator.Aggregator,
	logger *pct.Logger,
	spool data.Spooler,
	config pc.QAN,
) *monitor {
	return &monitor{
		mongoClient: mongoClient,
		dbName:      dbName,
		aggregator:  aggregator,
		logger:      logger,
		spool:       spool,
		config:      config,
	}
}

type monitor struct {
	// dependencies
	mongoClient *mongo.Client
	dbName      string
	aggregator  *aggregator.Aggregator
	spool       data.Spooler
	logger      *pct.Logger
	config      pc.QAN

	// internal services
	services []services

	// state
	sync.RWMutex      // Lock() to protect internal consistency of the service
	running      bool // Is this service running?
}

func (self *monitor) Start() error {
	self.Lock()
	defer self.Unlock()

	if self.running {
		return nil
	}

	defer func() {
		// if we failed to start
		if !self.running {
			// be sure that any started internal service is shutdown
			for _, s := range self.services {
				s.Stop()
			}
			self.services = nil
		}
	}()

	// create collector and start it
	c := collector.New(self.mongoClient, self.dbName)
	docsChan, err := c.Start()
	if err != nil {
		return err
	}
	self.services = append(self.services, c)

	// create parser and start it
	p := parser.New(docsChan, self.aggregator)
	err = p.Start()
	if err != nil {
		return err
	}
	self.services = append(self.services, p)

	self.running = true
	return nil
}

func (self *monitor) Stop() {
	self.Lock()
	defer self.Unlock()

	if !self.running {
		return
	}

	// stop internal services
	for _, s := range self.services {
		s.Stop()
	}

	self.running = false
}

// Status returns list of statuses
func (self *monitor) Status() map[string]string {
	self.RLock()
	defer self.RUnlock()

	statuses := &sync.Map{}

	wg := &sync.WaitGroup{}
	wg.Add(len(self.services))
	for _, s := range self.services {
		go func(s services) {
			defer wg.Done()
			for k, v := range s.Status() {
				key := fmt.Sprintf("%s-%s", s.Name(), k)
				statuses.Store(key, v)
			}
		}(s)
	}
	wg.Wait()

	statusesMap := map[string]string{}
	statuses.Range(func(key, value interface{}) bool {
		statusesMap[key.(string)] = value.(string)
		return true
	})

	return statusesMap
}

type services interface {
	Status() map[string]string
	Stop()
	Name() string
}
