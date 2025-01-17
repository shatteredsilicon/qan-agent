package sender

import (
	"sync"

	"github.com/shatteredsilicon/ssm/proto/qan"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mongo/status"
)

func New(reportChan <-chan *qan.Report, spool data.Spooler, logger *pct.Logger) *Sender {
	return &Sender{
		reportChan: reportChan,
		spool:      spool,
		logger:     logger,
	}
}

type Sender struct {
	// dependencies
	reportChan <-chan *qan.Report
	spool      data.Spooler
	logger     *pct.Logger

	// stats
	status *status.Status

	// state
	sync.RWMutex                 // Lock() to protect internal consistency of the service
	running      bool            // Is this service running?
	doneChan     chan struct{}   // close(doneChan) to notify goroutines that they should shutdown
	wg           *sync.WaitGroup // Wait() for goroutines to stop after being notified they should shutdown
}

// Start starts but doesn't wait until it exits
func (self *Sender) Start() error {
	self.Lock()
	defer self.Unlock()
	if self.running {
		return nil
	}

	// create new channels over which we will communicate to...
	// ... inside goroutine to close it
	self.doneChan = make(chan struct{})

	// set stats
	stats := &stats{}
	self.status = status.New(stats)

	// start a goroutine and Add() it to WaitGroup
	// so we could later Wait() for it to finish
	self.wg = &sync.WaitGroup{}
	self.wg.Add(1)
	go start(
		self.wg,
		self.reportChan,
		self.spool,
		self.logger,
		self.doneChan,
		stats,
	)

	self.running = true
	return nil
}

// Stop stops running
func (self *Sender) Stop() {
	self.Lock()
	defer self.Unlock()
	if !self.running {
		return
	}
	self.running = false

	// notify goroutine to close
	close(self.doneChan)

	// wait for goroutines to exit
	self.wg.Wait()
	return
}

func (self *Sender) Status() map[string]string {
	self.RLock()
	defer self.RUnlock()
	if !self.running {
		return nil
	}

	return self.status.Map()
}

func (self *Sender) Name() string {
	return "sender"
}

func start(
	wg *sync.WaitGroup,
	reportChan <-chan *qan.Report,
	spool data.Spooler,
	logger *pct.Logger,
	doneChan <-chan struct{},
	stats *stats,
) {
	// signal WaitGroup when goroutine finished
	defer wg.Done()

	for {

		select {
		case report, ok := <-reportChan:
			stats.In.Add(1)
			// if channel got closed we should exit as there is nothing we can listen to
			if !ok {
				return
			}

			// check if we should shutdown
			select {
			case <-doneChan:
				return
			default:
				// just continue if not
			}

			// sent report
			if err := spool.Write("qan", report); err != nil {
				stats.ErrIter.Add(1)
				logger.Warn("Lost report:", err)
				continue
			}
			stats.Out.Add(1)
		case <-doneChan:
			return
		}
	}

}
