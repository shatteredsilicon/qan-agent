package logparser

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/query"
)

type CSVLogParser struct {
}

func NewCSVLogParser() LogParser {
	return &CSVLogParser{}
}

func (p *CSVLogParser) Parse(ctx context.Context, f io.Reader, c chan<- *Event) error {
	reader := csv.NewReader(f)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		e, err := p.parseLine(line)
		if err != nil {
			return err
		}

		match := statementRe.FindAllStringSubmatch(e.Message, -1)
		if len(match) == 0 {
			continue
		}

		q := match[0][3]
		if len(match[0][4]) > 0 {
			q = match[0][4]
		}
		fingerprint, err := pg_query.Normalize(q)
		if err != nil {
			return err
		}
		id := query.Id(fingerprint)
		queryTime, _ := strconv.ParseFloat(match[0][1], 64)

		c <- &Event{
			ID:          id,
			Fingerprint: fingerprint,
			Query:       q,
			LogEntry:    *e,
			TimeMetrics: map[string]float64{
				"Query_time": queryTime / 1000,
			},
		}
	}
	return nil
}

func (p *CSVLogParser) IsFileAcceptable(filename string) bool {
	return strings.HasSuffix(filename, ".csv")
}

func (p *CSVLogParser) SplitLog(content []byte) ([]byte, []byte) {
	for i := len(content) - 1; i >= 0; i-- {
		if content[i] != '\n' || len(content)-i-1 < len(logTimeLayout) {
			continue
		}

		_, err := time.Parse(logTimeLayout, string(content[i+1:i+1+len(logTimeLayout)]))
		if err != nil {
			continue
		}

		if i > 0 && content[i-1] == '"' {
			return content[:i], content[i+1:]
		}

		commaIdx := i - 1
		for ; commaIdx >= 0; commaIdx-- {
			if content[commaIdx] == ',' {
				break
			}
			if !unicode.IsDigit(rune(content[commaIdx])) && content[commaIdx] != '-' {
				break
			}
		}
		if commaIdx == -1 {
			return nil, content
		}
		if commaIdx != ',' {
			continue
		}

		return content[:i], content[i+1:]
	}

	return nil, content
}

func (p *CSVLogParser) parseLine(fields []string) (*LogEntry, error) {
	var e LogEntry

	if len(fields) < reflect.ValueOf(e).NumField() {
		return nil, errors.New("unmatched field amount from csv log file")
	}

	e.LogTime.Time, _ = time.Parse(logTimeLayout, fields[0])
	e.UserName = fields[1]
	e.DatabaseName = fields[2]
	e.ProcessID, _ = strconv.Atoi(fields[3])
	e.ConnectionFrom = fields[4]
	e.SessionID = fields[5]
	e.SessionLineNum, _ = strconv.ParseInt(fields[6], 10, 64)
	e.CommandTag = fields[7]
	e.SessionStartTime.Time, _ = time.Parse(logTimeLayout, fields[8])
	e.VirtualTransactionID = fields[9]
	e.TransactionID, _ = strconv.ParseInt(fields[10], 10, 64)
	e.ErrorSeverity = fields[11]
	e.SQLStateCode = fields[12]
	e.Message = fields[13]
	e.Detail = fields[14]
	e.Hint = fields[15]
	e.InternalQuery = fields[16]
	e.InternalQueryPos, _ = strconv.Atoi(fields[17])
	e.Context = fields[18]
	e.Query = fields[19]
	e.QueryPos, _ = strconv.Atoi(fields[20])
	e.Location = fields[21]
	e.ApplicationName = fields[22]
	e.BackendType = fields[23]

	return &e, nil
}
