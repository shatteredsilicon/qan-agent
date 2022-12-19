/*
Copyright (c) 2019, Percona LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package log

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	l "log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/shatteredsilicon/ssm/proto"
)

// Regular expressions to match important lines in slow log.
var (
	timeRe    = regexp.MustCompile(`Time: (\S+\s{1,2}\S+)`)
	timeNewRe = regexp.MustCompile(`Time:\s+(\d{4}-\d{2}-\d{2}\S+)`)
	userRe    = regexp.MustCompile(`User@Host: ([^\[]+|\[[^[]+\]).*?@ (\S*) \[(.*)\]`)
	schema    = regexp.MustCompile(`Schema: +(.*?) +Last_errno:`)
	headerRe  = regexp.MustCompile(`^#\s+[A-Z]`)
	metricsRe = regexp.MustCompile(`(\w+): (\S+|\z)`)
	adminRe   = regexp.MustCompile(`command: (.+)`)
	setRe     = regexp.MustCompile(`^SET (last_insert_id|insert_id|timestamp)\s*=\s*(.*?)\s*;`)
	useRe     = regexp.MustCompile(`^(?i)use `)
)

// SlowLogParser parses a MySQL slow log. It implements the LogParser interface.
type SlowLogParser struct {
	reader io.ReadSeeker
	opt    Options
	// --
	stopChan        chan bool
	eventChan       chan *Event
	inHeader        bool
	inQuery         bool
	inExplain       bool
	explaiinColumns []string
	headerLines     uint
	queryLines      uint64
	bytesRead       uint64
	lineOffset      uint64
	endOffset       uint64
	stopped         bool
	event           *Event
}

// NewSlowLogParser returns a new SlowLogParser that reads from the open file.
func NewSlowLogParser(r io.ReadSeeker, opt Options) *SlowLogParser {
	if opt.DefaultLocation == nil {
		// Old MySQL format assumes time is taken from SYSTEM.
		opt.DefaultLocation = time.Local
	}

	p := &SlowLogParser{
		reader: r,
		opt:    opt,
		// --
		stopChan:    make(chan bool, 1),
		eventChan:   make(chan *Event),
		inHeader:    false,
		inQuery:     false,
		headerLines: 0,
		queryLines:  0,
		lineOffset:  0,
		bytesRead:   opt.StartOffset,
		event:       NewEvent(),
	}
	return p
}

// EventChan returns the unbuffered event channel on which the caller can
// receive events.
func (p *SlowLogParser) EventChan() <-chan *Event {
	return p.eventChan
}

// Stop stops the parser before parsing the next event or while blocked on
// sending the current event to the event channel.
func (p *SlowLogParser) Stop() {
	if p.opt.Debug {
		l.Println("stopping")
	}
	p.stopChan <- true
	return
}

// Start starts the parser. Events are sent to the unbuffered event channel.
// Parsing stops on EOF, error, or call to Stop. The event channel is closed
// when parsing stops. The file is not closed.
func (p *SlowLogParser) Start() error {
	if p.opt.Debug {
		l.SetFlags(l.Ltime | l.Lmicroseconds)
		fmt.Println()
		l.Println("start parsing")
	}

	// Seek to the offset, if any.
	// @todo error if start off > file size
	if p.opt.StartOffset > 0 {
		if _, err := p.reader.Seek(int64(p.opt.StartOffset), os.SEEK_SET); err != nil {
			return err
		}
	}

	defer close(p.eventChan)

	r := bufio.NewReader(p.reader)

SCANNER_LOOP:
	for !p.stopped {
		select {
		case <-p.stopChan:
			p.stopped = true
			break SCANNER_LOOP
		default:
		}

		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			break SCANNER_LOOP
		}

		lineLen := uint64(len(line))
		p.bytesRead += lineLen
		p.lineOffset = p.bytesRead - lineLen
		if p.opt.Debug {
			fmt.Println()
			l.Printf("+%d line: %s", p.lineOffset, line)
		}

		// Filter out meta lines:
		//   /usr/local/bin/mysqld, Version: 5.6.15-62.0-tokudb-7.1.0-tokudb-log (binary). started with:
		//   Tcp port: 3306  Unix socket: /var/lib/mysql/mysql.sock
		//   Time                 Id Command    Argument
		if lineLen >= 20 && ((line[0] == '/' && line[lineLen-6:lineLen] == "with:\n") ||
			(line[0:4] == "Time" && unicode.IsSpace(rune(line[5]))) ||
			(line[0:4] == "Tcp ") ||
			(line[0:4] == "TCP ")) {
			if p.opt.Debug {
				l.Println("meta")
			}
			p.inExplain = false
			continue
		}

		// PMM-1834: Filter out empty comments and MariaDB explain:
		if line == "#\n" {
			p.inExplain = false
			continue
		}

		if strings.HasPrefix(line, "# explain:") {
			p.parseExplain(line)
			continue
		}

		p.inExplain = false
		// Remove \n.
		line = line[0 : lineLen-1]

		if p.inHeader {
			p.parseHeader(line)
		} else if p.inQuery {
			p.parseQuery(line)
		} else if headerRe.MatchString(line) {
			p.inHeader = true
			p.inQuery = false
			p.parseHeader(line)
		}
	}

	if !p.stopped && p.queryLines > 0 {
		p.endOffset = p.bytesRead
		p.sendEvent(false, false)
	}

	if p.opt.Debug {
		l.Printf("\ndone")
	}
	return nil
}

// --------------------------------------------------------------------------

func (p *SlowLogParser) parseHeader(line string) {
	if p.opt.Debug {
		l.Println("header")
	}

	if !headerRe.MatchString(line) {
		p.inHeader = false
		p.inQuery = true
		p.parseQuery(line)
		return
	}

	if p.headerLines == 0 {
		p.event.Offset = p.lineOffset
	}
	p.headerLines++

	if strings.HasPrefix(line, "# Time") {
		if p.opt.Debug {
			l.Println("time")
		}
		m := timeRe.FindStringSubmatch(line)
		if len(m) == 2 {
			p.event.Ts, _ = time.ParseInLocation("060102 15:04:05", m[1], p.opt.DefaultLocation)
		} else {
			m = timeNewRe.FindStringSubmatch(line)
			if len(m) == 2 {
				p.event.Ts, _ = time.ParseInLocation(time.RFC3339Nano, m[1], p.opt.DefaultLocation)
			} else {
				return
			}
		}
		if userRe.MatchString(line) {
			if p.opt.Debug {
				l.Println("user (bad format)")
			}
			m := userRe.FindStringSubmatch(line)
			p.event.User = m[1]
			if m[2] != "" {
				p.event.Host = m[2]
			} else {
				p.event.Host = m[3]
			}
		}
	} else if strings.HasPrefix(line, "# User") {
		if p.opt.Debug {
			l.Println("user")
		}
		m := userRe.FindStringSubmatch(line)
		if len(m) < 3 {
			return
		}
		p.event.User = m[1]
		if m[2] != "" {
			p.event.Host = m[2]
		} else {
			p.event.Host = m[3]
		}
	} else if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
	} else {
		if p.opt.Debug {
			l.Println("metrics")
		}
		submatch := schema.FindStringSubmatch(line)
		if len(submatch) == 2 {
			p.event.Db = submatch[1]
		}

		m := metricsRe.FindAllStringSubmatch(line, -1)
		for _, smv := range m {
			// [String, Metric, Value], e.g. ["Query_time: 2", "Query_time", "2"]
			if strings.HasSuffix(smv[1], "_time") || strings.HasSuffix(smv[1], "_wait") {
				// microsecond value
				val, _ := strconv.ParseFloat(smv[2], 64)
				p.event.TimeMetrics[smv[1]] = val
			} else if smv[2] == "Yes" || smv[2] == "No" {
				// boolean value
				if smv[2] == "Yes" {
					p.event.BoolMetrics[smv[1]] = true
				} else {
					p.event.BoolMetrics[smv[1]] = false
				}
			} else if smv[1] == "Schema" {
				p.event.Db = smv[2]
			} else if smv[1] == "Log_slow_rate_type" {
				p.event.RateType = smv[2]
			} else if smv[1] == "Log_slow_rate_limit" {
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.RateLimit = uint(val)
			} else {
				// integer value
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.NumberMetrics[smv[1]] = val
			}
		}
	}
}

func (p *SlowLogParser) parseQuery(line string) {
	if p.opt.Debug {
		l.Println("query")
	}

	if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
		return
	} else if headerRe.MatchString(line) {
		if p.opt.Debug {
			l.Println("next event")
		}
		p.inHeader = true
		p.inQuery = false
		p.endOffset = p.lineOffset
		p.sendEvent(true, false)
		p.parseHeader(line)
		return
	}

	isUse := useRe.FindString(line)
	if p.queryLines == 0 && isUse != "" {
		if p.opt.Debug {
			l.Println("use db")
		}
		db := strings.TrimPrefix(line, isUse)
		db = strings.TrimRight(db, ";")
		db = strings.Trim(db, "`")
		p.event.Db = db
		// Set the 'use' as the query itself.
		// In case we are on a group of lines like in test 23, lines 6~8, the
		// query will be replaced by the real query "select field...."
		// In case we are on a group of lines like in test23, lines 27~28, the
		// query will be "use dbnameb" since the user executed a use command
		p.event.Query = line
	} else if m := setRe.FindAllStringSubmatch(line, -1); len(m) > 0 {
		var name, val string
		if len(m[0]) > 2 {
			name = m[0][1]
			val = m[0][2]
		}

		if p.opt.Debug {
			l.Println("set var", name)
		}

		switch name {
		// @todo: use other params that are being set
		case "timestamp":
			t, err := strconv.ParseInt(val, 10, 64)
			if err == nil && p.event.Ts.IsZero() {
				p.event.Ts = time.Unix(t, 0)
			}
		}

	} else {
		if p.opt.Debug {
			l.Println("query")
		}
		if p.queryLines > 0 {
			p.event.Query += "\n" + line
		} else {
			p.event.Query = line
		}
		p.queryLines++
	}
}

func (p *SlowLogParser) parseAdmin(line string) {
	if p.opt.Debug {
		l.Println("admin")
	}
	p.event.Admin = true
	m := adminRe.FindStringSubmatch(line)
	p.event.Query = m[1]
	p.event.Query = strings.TrimSuffix(p.event.Query, ";") // makes FilterAdminCommand work

	// admin commands should be the last line of the event.
	if filtered := p.opt.FilterAdminCommand[p.event.Query]; !filtered {
		if p.opt.Debug {
			l.Println("not filtered")
		}
		p.endOffset = p.bytesRead
		p.sendEvent(false, false)
	} else {
		p.inHeader = false
		p.inQuery = false
	}
}

func (p *SlowLogParser) parseExplain(line string) {
	splitStrs := strings.SplitN(line, ":", 2)
	if len(splitStrs) != 2 {
		return
	}

	strs := strings.Fields(splitStrs[1])
	if !p.inExplain { // first line, parse it as the column headers
		p.inExplain = true
		p.explaiinColumns = strs
		p.event.ExplainRows = make([]proto.ExplainRow, 0)
		return
	}

	row := proto.ExplainRow{}
	t := reflect.TypeOf(row)
	for columnIndex, str := range strs {
		if len(p.explaiinColumns) <= columnIndex {
			break
		}

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Tag.Get("slowlog") != p.explaiinColumns[columnIndex] {
				continue
			}

			if field.Type.ConvertibleTo(reflect.TypeOf(proto.NullInt64{})) {
				val := sql.NullInt64{}
				if strings.ToUpper(str) != "NULL" {
					val.Valid = true
					val.Int64, _ = strconv.ParseInt(str, 10, 64)
				}
				reflect.ValueOf(&row).Elem().Field(i).Set(reflect.ValueOf(proto.NullInt64{NullInt64: val}))
			} else if field.Type.ConvertibleTo(reflect.TypeOf(proto.NullFloat64{})) {
				val := sql.NullFloat64{}
				if strings.ToUpper(str) != "NULL" {
					val.Valid = true
					val.Float64, _ = strconv.ParseFloat(str, 64)
				}
				reflect.ValueOf(&row).Elem().Field(i).Set(reflect.ValueOf(proto.NullFloat64{NullFloat64: val}))
			} else if field.Type.ConvertibleTo(reflect.TypeOf(proto.NullString{})) {
				val := sql.NullString{}
				if str != "NULL" {
					val.Valid = true
					val.String = str
				}
				reflect.ValueOf(&row).Elem().Field(i).Set(reflect.ValueOf(proto.NullString{NullString: val}))
			}
		}
	}
	p.event.ExplainRows = append(p.event.ExplainRows, row)
}

func (p *SlowLogParser) sendEvent(inHeader bool, inQuery bool) {
	if p.opt.Debug {
		l.Println("send event")
	}

	p.event.OffsetEnd = p.endOffset

	// Make a new event and reset our metadata.
	defer func() {
		p.event = NewEvent()
		p.headerLines = 0
		p.queryLines = 0
		p.inHeader = inHeader
		p.inQuery = inQuery
		p.inExplain = false
		p.event.ExplainRows = nil
	}()

	if _, ok := p.event.TimeMetrics["Query_time"]; !ok {
		if p.headerLines == 0 {
			l.Panicf("No Query_time in event at %d: %#v", p.lineOffset, p.event)
		}
		// Started parsing in header after Query_time.  Throw away event.
		return
	}

	// Clean up the event.
	p.event.Db = strings.TrimSuffix(p.event.Db, ";\n")
	p.event.Query = strings.TrimSuffix(p.event.Query, ";")

	// Send the event.  This will block.
	select {
	case p.eventChan <- p.event:
	case <-p.stopChan:
		p.stopped = true
	}
}