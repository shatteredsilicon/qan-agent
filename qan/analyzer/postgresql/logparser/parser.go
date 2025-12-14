package logparser

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/util"
)

const (
	logTimeLayout = "2006-01-02 15:04:05.999 MST"
)

var (
	statementRe = regexp.MustCompile(`(?s)^\s*duration:\s*([0-9\.]+)\s*ms\s+(statement:\s*(.*)|execute\s+[^:]+:\s*(.*))`)
	parameterRe = regexp.MustCompile(`(?si)^\s*parameters:\s*(\$\d+\s*=.*)`)
)

type LogParser interface {
	Parse(context.Context, io.Reader, chan<- *Event) error
	IsFileAcceptable(string) bool
}

type LogParserFunc func() LogParser

func GetLogParserFuncs(logDestination string) LogParserFunc {
	for _, ld := range strings.Split(logDestination, ",") {
		switch ld {
		case "csvlog":
			return NewCSVLogParser
		case "jsonlog":
			return NewJSONLogParser
		}
	}
	return nil
}

type LogTime struct {
	time.Time
}

func (lt LogTime) MarshalJSON() ([]byte, error) {
	return []byte(lt.Format(logTimeLayout)), nil
}

func (lt *LogTime) UnmarshalJSON(data []byte) error {
	t, err := time.Parse(logTimeLayout, strings.Trim(string(data), "\""))
	if err != nil {
		return err
	}
	lt.Time = t
	return nil
}

type LogEntry struct {
	LogTime              LogTime `json:"timestamp"`
	UserName             string  `json:"user"`
	DatabaseName         string  `json:"dbname"`
	ProcessID            int     `json:"pid"`
	ConnectionFrom       string  `json:"remote_host"`
	SessionID            string  `json:"session_id"`
	SessionLineNum       int64   `json:"line_num"`
	CommandTag           string  `json:"ps"`
	SessionStartTime     LogTime `json:"session_start"`
	VirtualTransactionID string  `json:"vxid"`
	TransactionID        int64   `json:"txid"`
	ErrorSeverity        string  `json:"error_severity"`
	SQLStateCode         string  `json:"state_code"`
	Message              string  `json:"message"`
	Detail               string  `json:"detail"`
	Hint                 string  `json:"hint"`
	InternalQuery        string  `json:"internal_query"`
	InternalQueryPos     int     `json:"internal_position"`
	Context              string  `json:"context"`
	Query                string  `json:"statement"`
	QueryPos             int     `json:"cursor_position"`
	Location             string  `json:"location"`
	ApplicationName      string  `json:"application_name"`
	BackendType          string  `json:"backend_type"`
}

type Event struct {
	ID            string
	Fingerprint   string
	Query         string
	LogEntry      LogEntry
	TimeMetrics   map[string]float64 // *_time and *_wait metrics
	NumberMetrics map[string]uint64  // most metrics
	BoolMetrics   map[string]bool    // yes/no metrics
}

type paramRef struct {
	number   int
	location int
}

type paramRefArr []paramRef

func (arr paramRefArr) Len() int           { return len(arr) }
func (arr paramRefArr) Swap(i, j int)      { arr[i], arr[j] = arr[j], arr[i] }
func (arr paramRefArr) Less(i, j int) bool { return arr[i].location > arr[j].location }

func (e *Event) AttemptToResolveParams() {
	match := parameterRe.FindAllStringSubmatch(e.LogEntry.Detail, -1)
	if len(match) == 0 {
		return
	}

	params := map[int]string{}
	for _, item := range util.Split(match[0][1], ',') {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		if len(key) < 2 || key[0] != '$' {
			continue
		}

		number, err := strconv.Atoi(key[1:])
		if err != nil {
			continue
		}
		params[number] = strings.TrimSpace(kv[1])
	}

	if len(params) == 0 {
		return
	}

	parseTreeJSON, err := pg_query.ParseToJSON(e.Query)
	if err != nil {
		return
	}

	parseTree := map[string]interface{}{}
	if err = json.Unmarshal([]byte(parseTreeJSON), &parseTree); err != nil {
		return
	}

	paramObjects := util.GetJSONObjectsByKey(parseTree, "ParamRef")

	var paramRefs paramRefArr
	for _, obj := range paramObjects {
		param, ok := obj.(map[string]interface{})
		if !ok || param == nil {
			continue
		}

		var ref paramRef

		number, ok := param["number"].(float64)
		if !ok {
			continue
		}
		ref.number = int(number)

		if _, ok := params[ref.number]; !ok {
			continue
		}

		location, ok := param["location"].(float64)
		if !ok {
			continue
		}
		ref.location = int(location)

		if len(e.Query) < ref.location {
			continue
		}

		paramRefs = append(paramRefs, ref)
	}

	sort.Sort(paramRefs)
	for _, ref := range paramRefs {
		numberLen := 1 + int(math.Log10(float64(ref.number)))
		e.Query = e.Query[:ref.location] + params[ref.number] + e.Query[ref.location+1+numberLen:]
	}
}
