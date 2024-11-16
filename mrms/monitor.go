/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package mrms

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/percona/go-mysql/dsn"
	"github.com/shatteredsilicon/qan-agent/mrms/checker"
	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/ssm/proto"
)

const MONITOR_NAME = "mrm-monitor"

type Checker interface {
	Check() (bool, error)
	SlowLogChanged() (*bool, error)
}

type instance struct {
	instance           proto.Instance
	checker            Checker
	listeners          map[chan interface{}]bool
	slowlogCheckPaused bool
	sync.RWMutex
}

type Monitor interface {
	Start(interval time.Duration) error
	Stop() error
	Status() map[string]string
	Add(proto.Instance) chan interface{}
	Remove(string, chan interface{})
	ListenerCount(uuid string) uint
	Check()
	SwitchSlowlogCheck(uuid string, on bool)
}

type RealMonitor struct {
	logger           *pct.Logger
	mysqlConnFactory mysql.ConnectionFactory
	// --
	instances map[string]*instance
	sync.RWMutex
	// --
	status *pct.Status
	sync   *pct.SyncChan
}

func NewRealMonitor(logger *pct.Logger, mysqlConnFactory mysql.ConnectionFactory) *RealMonitor {
	instances := map[string]*instance{
		"": {
			listeners: map[chan interface{}]bool{},
		},
	}
	m := &RealMonitor{
		logger:           logger,
		mysqlConnFactory: mysqlConnFactory,
		// --
		instances: instances,
		status:    pct.NewStatus([]string{MONITOR_NAME}),
		sync:      pct.NewSyncChan(),
	}
	return m
}

/////////////////////////////////////////////////////////////////////////////
// Interface
/////////////////////////////////////////////////////////////////////////////

func (m *RealMonitor) Start(interval time.Duration) error {
	m.logger.Debug("Start:call")
	defer m.logger.Debug("Start:return")
	go m.run(interval)
	return nil
}

func (m *RealMonitor) Stop() error {
	m.logger.Debug("Stop:call")
	defer m.logger.Debug("Stop:return")
	m.sync.Stop()
	m.sync.Wait()
	return nil
}

func (m *RealMonitor) Status() map[string]string {
	return m.status.All()
}

func (m *RealMonitor) Add(in proto.Instance) chan interface{} {
	m.logger.Debug("Add:call:" + dsn.HidePassword(in.DSN))
	defer m.logger.Debug("Add:return:" + dsn.HidePassword(in.DSN))

	m.Lock()
	defer m.Unlock()

	i, ok := m.instances[in.UUID]
	if !ok {
		m.logger.Debug("add:" + in.Subsystem + "-" + in.UUID)
		c := checker.NewMySQL(
			pct.NewLogger(m.logger.LogChan(), "mrms-check-mysql-"+in.Name),
			m.mysqlConnFactory.Make(in.DSN),
		)
		i = &instance{
			instance:  in,
			checker:   c,
			listeners: map[chan interface{}]bool{},
		}
		m.instances[in.UUID] = i
	}

	mrmsChan := make(chan interface{}, 1)
	if in.UUID != "" {
		i.listeners[mrmsChan] = true
	} else {
		m.instances[""].listeners[mrmsChan] = true // global
	}

	return mrmsChan
}

func (m *RealMonitor) Remove(uuid string, c chan interface{}) {
	m.logger.Debug("Remove:call:" + uuid)
	defer m.logger.Debug("Remove:return:" + uuid)

	m.Lock()
	defer m.Unlock()

	delete(m.instances[""].listeners, c) // global

	i, ok := m.instances[uuid]
	if !ok {
		return
	}

	delete(i.listeners, c)
	if len(i.listeners) == 0 {
		delete(m.instances, uuid)
	}
}

func (m *RealMonitor) Check() {
	m.logger.Debug("Check:call")
	defer m.logger.Debug("Check:return")

	m.status.Update(MONITOR_NAME, "Checking")

	m.RLock()
	defer m.RUnlock()

	for uuid, in := range m.instances {
		if uuid == "" {
			continue // global
		}
		m.logger.Debug("check:" + uuid)

		slowlogChangedTo, err := in.checker.SlowLogChanged()
		in.RLock()
		if err != nil {
			m.logger.Warn(err)
		} else if slowlogChangedTo != nil && !in.slowlogCheckPaused {
			for c := range in.listeners { // only this instance
				select {
				case c <- *slowlogChangedTo:
				default:
					m.logger.Warn("Listener not ready")
				}
			}
		}
		in.RUnlock()

		restarted, err := in.checker.Check()
		if err != nil {
			m.logger.Warn(err)
			continue
		}
		if !restarted {
			continue
		}
		m.logger.Info(fmt.Sprintf("%s instance %s restarted", in.instance.Subsystem, in.instance.UUID))
		for c := range in.listeners { // only this instance
			select {
			case c <- in.instance:
			default:
				m.logger.Warn("Listener not ready")
			}
		}
		for c := range m.instances[""].listeners { // global
			select {
			case c <- in.instance:
			default:
				m.logger.Warn("Global listener not ready")
			}
		}
	}
}

func (m *RealMonitor) ListenerCount(uuid string) uint {
	m.logger.Debug("ListenerCount:call")
	defer m.logger.Debug("ListenerCount:return")
	m.RLock()
	defer m.RUnlock()
	i, ok := m.instances[uuid]
	if !ok {
		return 0
	}
	return uint(len(i.listeners))
}

func (m *RealMonitor) SwitchSlowlogCheck(uuid string, on bool) {
	m.logger.Debug("SwitchSlowlogCheck:call:" + uuid)
	defer m.logger.Debug("SwitchSlowlogCheck:return:" + uuid)

	i := m.instances[uuid]
	if i == nil {
		return
	}

	i.Lock()
	defer i.Unlock()

	m.logger.Debug("SwitchSlowlogCheck: slow_query_log monitoring for", uuid, "switched to", on)
	i.slowlogCheckPaused = !on
}

/////////////////////////////////////////////////////////////////////////////
// Implementation
/////////////////////////////////////////////////////////////////////////////

func (m *RealMonitor) run(interval time.Duration) {
	m.logger.Debug("run:call")
	defer m.logger.Debug("run:return")

	defer func() {
		if err := recover(); err != nil {
			errMsg := fmt.Sprintf("Restart monitor crashed: %s", err)
			log.Println(errMsg)
			debug.PrintStack()
			m.logger.Error(errMsg)
		}
		m.status.Update(MONITOR_NAME, "Stopped")
		m.sync.Done()
	}()

	for {
		m.Check()
		m.status.Update(MONITOR_NAME, "Idle")
		select {
		case <-time.After(interval):
		case <-m.sync.StopChan:
			return
		}
	}
}
