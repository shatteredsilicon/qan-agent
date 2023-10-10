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

package explain

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/ssm/proto"
	"vitess.io/vitess/go/vt/sqlparser"
)

func Explain(c mysql.Connector, db, query string, convert, ignoreClassic bool) (*proto.ExplainResult, error) {
	if db != "" && !strings.HasPrefix(db, "`") {
		db = "`" + db + "`"
	}
	explainResult, err := explain(c, db, query, ignoreClassic)
	if err != nil {
		// MySQL 5.5 returns syntax error because it doesn't support non-SELECT EXPLAIN.
		// MySQL 5.6 non-SELECT EXPLAIN requires privs for the SQL statement.
		errCode := mysql.MySQLErrorCode(err)
		if convert && (errCode == mysql.ER_SYNTAX_ERROR || errCode == mysql.ER_USER_DENIED) && isDMLQuery(query) {
			query = dmlToSelect(query)
			if query == "" {
				return nil, fmt.Errorf("cannot convert query to SELECT")
			}
			explainResult, err = explain(c, db, query, ignoreClassic) // query converted to SELECT
		}
		if err != nil {
			return nil, err
		}
	}
	return explainResult, nil
}

// --------------------------------------------------------------------------

func explain(c mysql.Connector, db, query string, ignoreClassic bool) (*proto.ExplainResult, error) {
	// Transaction because we need to ensure USE and EXPLAIN are run in one connection
	tx, err := c.DB().Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// If the query has a default db, use it; else, all tables need to be db-qualified
	// or EXPLAIN will throw an error.
	if db != "" {
		_, err := tx.Exec(fmt.Sprintf("USE %s", db))
		if err != nil {
			return nil, err
		}
	}

	explain := &proto.ExplainResult{}
	var originErr error
	explain.Classic, explain.JSON, originErr = realExplain(c, tx, query, ignoreClassic)
	if originErr == nil {
		return explain, nil
	}

	// First try failed, see if this is a query that we can
	// adjust to make EXPLAIN works
	s, err := sqlparser.Parse(query)
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

	explain.Classic, explain.JSON, err = realExplain(c, tx, newQuery, ignoreClassic)
	if err != nil {
		return nil, err
	}

	return explain, nil
}

func realExplain(c mysql.Connector, tx *sql.Tx, query string, ignoreClassic bool) (classic []*proto.ExplainRow, json string, err error) {
	if !ignoreClassic {
		classic, err = classicExplain(c, tx, query)
		if err != nil {
			return nil, "", err
		}
	}

	json, err = jsonExplain(c, tx, query)
	return classic, json, err
}

func classicExplain(c mysql.Connector, tx *sql.Tx, query string) (classicExplain []*proto.ExplainRow, err error) {
	// Partitions are introduced since MySQL 5.1
	// We can simply run EXPLAIN /*!50100 PARTITIONS*/ to get this column when it's available
	// without prior check for MySQL version.
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("cannot run EXPLAIN on an empty query example")
	}
	rows, err := tx.Query(fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Go rows.Scan() expects exact number of columns
	// so when number of columns is undefined then the easiest way to
	// overcome this problem is to count received number of columns
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	nCols := len(columns)

	for rows.Next() {
		explainRow := &proto.ExplainRow{}
		switch nCols {
		case 10:
			err = rows.Scan(
				&explainRow.Id,
				&explainRow.SelectType,
				&explainRow.Table,
				&explainRow.Type,
				&explainRow.PossibleKeys,
				&explainRow.Key,
				&explainRow.KeyLen,
				&explainRow.Ref,
				&explainRow.Rows,
				&explainRow.Extra,
			)
		case 11: // MySQL 5.1 with "partitions"
			err = rows.Scan(
				&explainRow.Id,
				&explainRow.SelectType,
				&explainRow.Table,
				&explainRow.Partitions, // here
				&explainRow.Type,
				&explainRow.PossibleKeys,
				&explainRow.Key,
				&explainRow.KeyLen,
				&explainRow.Ref,
				&explainRow.Rows,
				&explainRow.Extra,
			)
		case 12: // MySQL 5.7 with "filtered"
			err = rows.Scan(
				&explainRow.Id,
				&explainRow.SelectType,
				&explainRow.Table,
				&explainRow.Partitions,
				&explainRow.Type,
				&explainRow.PossibleKeys,
				&explainRow.Key,
				&explainRow.KeyLen,
				&explainRow.Ref,
				&explainRow.Rows,
				&explainRow.Filtered, // here
				&explainRow.Extra,
			)
		}
		if err != nil {
			return nil, err
		}
		classicExplain = append(classicExplain, explainRow)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return classicExplain, nil
}

func jsonExplain(c mysql.Connector, tx *sql.Tx, query string) (string, error) {
	// EXPLAIN in JSON format is introduced since MySQL 5.6.5 and MariaDB 10.1.2
	// https://mariadb.com/kb/en/mariadb/explain-format-json/
	ok, err := c.VersionConstraint(">= 5.6.5, < 10.0.0 || >= 10.1.2")
	if !ok || err != nil {
		return "", err
	}

	explain := ""
	err = tx.QueryRow(fmt.Sprintf("EXPLAIN FORMAT=JSON %s", query)).Scan(&explain)
	if err != nil {
		return "", err
	}

	return explain, nil
}
