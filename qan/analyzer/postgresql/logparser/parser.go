package logparser

import (
	"context"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	logTimeLayout = "2006-01-02 15:04:05.999 MST"
)

var (
	statementRe = regexp.MustCompile(`(?s)^\s*duration:\s*([0-9\.]+)\s*ms\s+statement:\s*(.*)`)
)

type LogParser interface {
	Parse(context.Context, *os.File, chan<- Event) error
	IsFileAcceptable(os.DirEntry) bool
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
