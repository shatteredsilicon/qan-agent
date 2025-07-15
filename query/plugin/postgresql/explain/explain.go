package explain

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/shatteredsilicon/ssm/proto"
	"vitess.io/vitess/go/vt/sqlparser"
)

func Explain(db *sql.DB, query string, convert, ignoreClassic bool) (*proto.ExplainResult, error) {
	explainResult, err := explain(db, query, ignoreClassic)
	if err != nil {
		return nil, err
	}
	return explainResult, nil
}

// --------------------------------------------------------------------------

func explain(db *sql.DB, query string, ignoreClassic bool) (*proto.ExplainResult, error) {
	explain := &proto.ExplainResult{}
	var originErr error
	explain.TEXT, explain.JSON, originErr = realExplain(db, query, ignoreClassic)
	if originErr == nil {
		return explain, nil
	}

	// First try failed, see if this is a query that we can
	// adjust to make EXPLAIN works
	s, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return nil, originErr
	}

	var newQuery string
	switch s.(type) {
	case *sqlparser.Delete: // DELETE statement, try changing it to SELECT statement
		if indexes := regexp.MustCompile(`(?i)\sFROM\s`).FindIndex([]byte(query)); len(indexes) > 0 {
			newQuery = fmt.Sprintf("SELECT * %s", query[indexes[0]:])
		}
	case *sqlparser.Insert:
		switch s.(*sqlparser.Insert).Rows.(type) {
		case *sqlparser.Select: // INSERT INTO ... SELECT statement, try explaining the SELECT part only
			newQuery = sqlparser.String(s.(*sqlparser.Insert).Rows)
		}
	}

	if newQuery == "" {
		return nil, originErr
	}

	explain.TEXT, explain.JSON, err = realExplain(db, newQuery, ignoreClassic)
	if err != nil {
		return nil, err
	}

	return explain, nil
}

func realExplain(db *sql.DB, query string, ignoreText bool) (text, json string, err error) {
	if !ignoreText {
		text, err = textExplain(db, query)
		if err != nil {
			return "", "", err
		}
	}

	json, err = jsonExplain(db, query)
	return
}

func textExplain(db *sql.DB, query string) (explain string, err error) {
	if strings.TrimSpace(query) == "" {
		return "", fmt.Errorf("cannot run EXPLAIN on an empty query example")
	}

	rows, err := db.Query(fmt.Sprintf("EXPLAIN (FORMAT TEXT) %s", query))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	explainRows := []string{}
	for rows.Next() {
		var row string
		err = rows.Scan(&row)
		if err != nil {
			return "", err
		}
		explainRows = append(explainRows, row)
	}

	return strings.Join(explainRows, "\n"), err
}

func jsonExplain(db *sql.DB, query string) (string, error) {
	var explain string
	err := db.QueryRow(fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", query)).Scan(&explain)
	if err != nil {
		return "", err
	}

	return explain, nil
}
