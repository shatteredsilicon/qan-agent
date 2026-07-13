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
	"strconv"
	"strings"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/ssm/proto"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	unsupportedRegex = regexp.MustCompile(`^(?i)\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|GRANT|REVOKE|OPTIMIZE|ANALYZE|EXPLAIN|REPAIR|CALL)`)
)

func Explain(c mysql.Connector, db, query string, ignoreClassic bool) (*proto.ExplainResult, error) {
	if unsupportedRegex.Match([]byte(query)) {
		return nil, nil
	}

	if db != "" && !strings.HasPrefix(db, "`") {
		db = "`" + db + "`"
	}

	query = tryConvertToExplainable(query)
	return explain(c, db, query, ignoreClassic)
}

func tryConvertToExplainable(query string) string {
	s, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return query
	}

	switch node := s.(type) {
	case *sqlparser.Delete: // DELETE statement, try changing it to SELECT statement
		var newQuery string
		if len(node.Targets) < 2 {
			newQuery = fmt.Sprintf("SELECT * FROM %s", sqlparser.SliceString(s.(*sqlparser.Delete).TableExprs))
		} else {
			newQuery = fmt.Sprintf("SELECT 1 FROM %s", sqlparser.SliceString(s.(*sqlparser.Delete).TableExprs))
		}
		if node.Where != nil {
			newQuery += sqlparser.CanonicalString(node.Where)
		}
		return newQuery
	case *sqlparser.Insert:
		switch node.Rows.(type) {
		case *sqlparser.Select: // INSERT INTO ... SELECT statement, try explaining the SELECT part only
			return sqlparser.String(s.(*sqlparser.Insert).Rows)
		}
	}

	return query
}

// --------------------------------------------------------------------------

func explain(c mysql.Connector, db, query string, ignoreClass bool) (*proto.ExplainResult, error) {
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

	result := &proto.ExplainResult{}

	if !ignoreClass {
		result.Classic, err = classicExplain(tx, query)
		if err != nil {
			return nil, err
		}
	}

	result.JSON, err = jsonExplain(c, tx, query)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func classicExplain(tx *sql.Tx, query string) (classicExplain []*proto.ExplainRow, err error) {
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
		var rowsStr proto.NullString
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
				&rowsStr,
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
				&rowsStr,
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
				&rowsStr,
				&explainRow.Filtered, // here
				&explainRow.Extra,
			)
		}
		if err != nil {
			return nil, err
		}
		if rowsStr.Valid {
			if m := regexp.MustCompile(`^(\d+)\s*\((\d+)%?\)`).FindStringSubmatch(rowsStr.String); len(m) == 3 {
				explainRow.Rows.Int64, _ = strconv.ParseInt(m[1], 10, 64)
				explainRow.Rows.Valid = true
				if !explainRow.Filtered.Valid {
					explainRow.Filtered.Float64, _ = strconv.ParseFloat(m[2], 64)
					explainRow.Filtered.Valid = true
				}
			} else {
				explainRow.Rows.Int64, err = strconv.ParseInt(rowsStr.String, 10, 64)
				if err != nil {
					return nil, err
				}
				explainRow.Rows.Valid = true
			}
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
