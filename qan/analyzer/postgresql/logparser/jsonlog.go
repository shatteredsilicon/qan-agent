package logparser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql/query"
)

type JSONLogEntry struct {
	LogEntry
	FuncName    string `json:"func_name"`
	FileName    string `json:"file_name"`
	FileLineNum int    `json:"file_line_num"`
}

type JSONLogParser struct {
}

func NewJSONLogParser() LogParser {
	return &JSONLogParser{}
}

func (p *JSONLogParser) Parse(ctx context.Context, f *os.File, c chan<- Event) error {
	reader := bufio.NewScanner(f)
	for reader.Scan() {
		var e JSONLogEntry
		if err := json.Unmarshal(reader.Bytes(), &e); err != nil {
			return err
		}

		match := statementRe.FindAllStringSubmatch(e.Message, -1)
		if len(match) == 0 {
			continue
		}

		q := match[0][2]
		fingerprint, err := pg_query.Normalize(q)
		if err != nil {
			return err
		}
		id := query.Id(fingerprint)
		queryTime, _ := strconv.ParseFloat(match[0][1], 64)

		if e.Location == "" {
			if e.FuncName != "" && e.FileName != "" {
				e.Location = fmt.Sprintf("%s, %s:%d", e.FuncName, e.FileName, e.FileLineNum)
			} else if e.FileName != "" {
				e.Location = fmt.Sprintf("%s:%d", e.FileName, e.FileLineNum)
			}
		}

		c <- Event{
			ID:          id,
			Fingerprint: fingerprint,
			Query:       q,
			LogEntry:    e.LogEntry,
			TimeMetrics: map[string]float64{
				"Query_time": queryTime / 1000,
			},
		}
	}
	return nil
}

func (p *JSONLogParser) IsFileAcceptable(entry os.DirEntry) bool {
	return !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json")
}
