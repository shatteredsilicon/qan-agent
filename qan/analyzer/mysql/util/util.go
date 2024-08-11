package util

import (
	"fmt"
	"regexp"

	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
)

var logHeaderRe = regexp.MustCompile(`^#\s*[A-Z]`)

func GetMySQLConfig(config analyzer.QAN) ([]string, []string, error) {
	switch config.CollectFrom {
	case "slowlog":
		return makeSlowLogConfig()
	case "rds-slowlog":
		return makeRDSSlowLogConfig()
	case "perfschema":
		return makePerfSchemaConfig()
	default:
		return nil, nil, fmt.Errorf("invalid CollectFrom: '%s'; expected 'slowlog' or 'perfschema'", config.CollectFrom)
	}
}

func makeSlowLogConfig() ([]string, []string, error) {
	on := []string{
		"SET time_zone='+0:00'",
	}
	off := []string{
		"SET GLOBAL slow_query_log=OFF",
	}

	return on, off, nil
}

func makeRDSSlowLogConfig() ([]string, []string, error) {
	return []string{"SET time_zone='+0:00'"}, []string{}, nil
}

func makePerfSchemaConfig() ([]string, []string, error) {
	return []string{"SET time_zone='+0:00'"}, []string{}, nil
}

// SplitSlowLog splits a incomplete slow log into 2 parts
func SplitSlowLog(log []byte) (completeLog, incompleteLog []byte) {
	lineEnd := len(log)
	var inHeader bool
	for i := len(log) - 1; i >= 0; i-- {
		if log[i] != '\n' {
			continue
		}

		if i == lineEnd-1 {
			continue
		}

		line := log[i+1 : lineEnd]
		lineLen := uint64(len(line))
		if lineLen >= 20 && (line[0] == '/' && string(line[lineLen-6:lineLen]) == "with:\n") {
			return log[0 : i+1], log[i+1:]
		} else if logHeaderRe.Match(line) {
			inHeader = true
		} else if inHeader {
			return log[0:lineEnd], log[lineEnd:]
		}

		lineEnd = i + 1
	}

	return []byte{}, log
}
