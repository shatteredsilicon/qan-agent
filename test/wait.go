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

package test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/ssm/proto"
)

var Ts, _ = time.Parse("2006-01-02 15:04:05", "2013-12-30 18:36:00")

func WaitCmd(replyChan chan *proto.Cmd) []proto.Cmd {
	var buf []proto.Cmd
	var haveData bool = true
	for haveData {
		select {
		case cmd := <-replyChan:
			buf = append(buf, *cmd)
		case <-time.After(100 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitReply(replyChan chan *proto.Reply) []proto.Reply {
	var buf []proto.Reply
	var haveData bool = true
	for haveData {
		select {
		case reply := <-replyChan:
			buf = append(buf, *reply)
		case <-time.After(100 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitReplyCmd(replyChan chan *proto.Reply, replyCmd string) []proto.Reply {
	var buf []proto.Reply
	var haveData bool = true
	for haveData {
		select {
		case reply := <-replyChan:
			if reply.Cmd == replyCmd {
				buf = append(buf, *reply)
				haveData = false
			}
		case <-time.After(3000 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitStatusReply(replyChan chan *proto.Reply) map[string]string {
	select {
	case reply := <-replyChan:
		status := make(map[string]string)
		if err := json.Unmarshal(reply.Data, &status); err != nil {
			log.Println(err)
		}
		return status
	case <-time.After(250 * time.Millisecond):
	}
	return nil
}

func WaitData(recvDataChan chan interface{}) []interface{} {
	var buf []interface{}
	var haveData bool = true
	for haveData {
		select {
		case data := <-recvDataChan:
			buf = append(buf, data)
		case <-time.After(500 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitBytes(dataChan chan []byte) [][]byte {
	var buf [][]byte
	var haveData bool = true
	for haveData {
		select {
		case data := <-dataChan:
			buf = append(buf, data)
		case <-time.After(100 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitLog(recvDataChan chan interface{}, want int) []proto.LogEntry {
	got := 0
	buf := []proto.LogEntry{}
	timeout := time.After(1 * time.Second)
	for got < want {
		select {
		case data := <-recvDataChan:
			logEntry := *data.(*proto.LogEntry)
			logEntry.Ts = Ts
			buf = append(buf, logEntry)
			got++
		case <-timeout:
			return buf
		}
	}

	return buf
}

func WaitTrace(traceChan chan string) []string {
	var buf []string
	var haveData bool = true
	for haveData {
		select {
		case msg := <-traceChan:
			buf = append(buf, msg)
		case <-time.After(10 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitErr(errChan chan error) []error {
	var buf []error
	var haveData bool = true
	for haveData {
		select {
		case err := <-errChan:
			buf = append(buf, err)
		case <-time.After(10 * time.Millisecond):
			haveData = false
		}
	}
	return buf
}

func WaitFiles(dir string, n int) []os.FileInfo {
	for i := 0; i < 5; i++ {
		files, _ := ioutil.ReadDir(dir)
		nFiles := len(files)
		if nFiles >= n {
			return files
		}
		time.Sleep(100 * time.Millisecond)
	}
	files, _ := ioutil.ReadDir(dir)
	return files
}

func WaitStatus(timeout int, r pct.StatusReporter, proc string, state string) bool {
	waitTimeout := time.After(time.Duration(timeout) * time.Second)
	for {
		status := r.Status()
		if s, ok := status[proc]; ok {
			if s == state {
				return true
			}
		}

		select {
		case <-waitTimeout:
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func WaitStatusPrefix(timeout int, r pct.StatusReporter, proc string, state string) bool {
	waitTimeout := time.After(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-waitTimeout:
			return false
		case <-time.After(100 * time.Millisecond):
			status := r.Status()
			if s, ok := status[proc]; !ok {
				log.Fatalf("StatusReporter does not have %s: %+v\n", proc, status)
			} else {
				if strings.HasPrefix(s, state) {
					return true
				}
			}
		}
	}
}

func WaitState(c chan bool) bool {
	select {
	case state := <-c:
		return state
	case <-time.After(1 * time.Second):
		return false
	}
}
