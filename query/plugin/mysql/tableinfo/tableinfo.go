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

package tableinfo

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/util"
	"github.com/shatteredsilicon/ssm/proto"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	MAX_JOIN_DEPTH = 100
)

func TableInfo(c mysql.Connector, tables *proto.TableInfoQuery) (proto.TableInfoResult, error) {
	res := make(proto.TableInfoResult)

	createList := append([]proto.Table{}, tables.Create...)
	indexList := append([]proto.Table{}, tables.Index...)
	statusList := append([]proto.Table{}, tables.Status...)

	if len(tables.Create) > 0 {
		for i := 0; i < len(createList); i++ {
			t := createList[i]

			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &proto.TableInfo{}
				tableInfo = res[dbTable]
			}

			db := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			tableType, def, err := showCreate(c, util.Ident(db, table))
			if err != nil {
				if tableInfo.Errors == nil {
					tableInfo.Errors = []string{}
				}
				tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("SHOW CREATE TABLE %s: %s", t.Table, err))
				continue
			}
			tableInfo.Create = def
			tableInfo.Type = tableType

			if tableType != proto.TypeDBView {
				continue
			}

			// try to get the underlying tables or
			// views of current view
			s, err := sqlparser.Parse(def)
			if err != nil {
				continue
			}

			cv, ok := s.(*sqlparser.CreateView)
			if !ok {
				continue
			}

			tables := getTablesFromSelectStmt(cv.Select, 0)
			for ti := range tables {
				if tables[ti].Db == "" {
					tables[ti].Db = t.Db
				}
			}
			createList = append(createList, tables...)
			indexList = append(indexList, tables...)
			statusList = append(statusList, tables...)
		}
	}

	if len(tables.Index) > 0 {
		for _, t := range indexList {
			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &proto.TableInfo{}
				tableInfo = res[dbTable]
			}
			if tableInfo.Type == proto.TypeDBView {
				continue
			}

			db := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			indexes, err := showIndex(c, util.Ident(db, table))
			if err != nil {
				if tableInfo.Errors == nil {
					tableInfo.Errors = []string{}
				}
				tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("SHOW INDEX FROM %s.%s: %s", t.Db, t.Table, err))
				continue
			}
			tableInfo.Index = indexes
		}
	}

	if len(tables.Status) > 0 {
		for _, t := range statusList {
			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &proto.TableInfo{}
				tableInfo = res[dbTable]
			}
			if tableInfo.Type == proto.TypeDBView {
				continue
			}

			// SHOW TABLE STATUS does not accept db.tbl so pass them separately.
			db := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			status, err := showStatus(c, util.Ident(db, ""), table)
			if err != nil {
				if tableInfo.Errors == nil {
					tableInfo.Errors = []string{}
				}
				tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("SHOW TABLE STATUS FROM %s WHERE Name='%s': %s", t.Db, t.Table, err))
				continue
			}
			tableInfo.Status = status
		}
	}

	return res, nil
}

// --------------------------------------------------------------------------

func showCreate(c mysql.Connector, dbTable string) (proto.DBObjectType, string, error) {
	// Result from SHOW CREATE TABLE includes two columns, "Table" and
	// "Create Table", we ignore the first one as we need only "Create Table".
	var name, def, c3, c4 string
	var tableType proto.DBObjectType

	rows, err := c.DB().Query("SHOW CREATE TABLE " + dbTable)
	if err != nil {
		return tableType, "", err
	}
	defer rows.Close()

	if rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return tableType, "", err
		}
		if len(columns) == 0 {
			return tableType, "", fmt.Errorf("unexpected 'SHOW CREATE TABLE' of table %s", dbTable)
		}

		if strings.ToLower(columns[0]) == "view" {
			tableType = proto.TypeDBView
			err = rows.Scan(&name, &def, &c3, &c4)
		} else {
			tableType = proto.TypeDBTable
			err = rows.Scan(&name, &def)
		}
	}

	return tableType, def, err
}

func showIndex(c mysql.Connector, dbTable string) (map[string][]proto.ShowIndexRow, error) {
	rows, err := c.DB().Query("SHOW INDEX FROM " + dbTable)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	defer rows.Close()
	if err == sql.ErrNoRows {
		err = fmt.Errorf("table %s doesn't exist", dbTable)
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	indexes := map[string][]proto.ShowIndexRow{} // keyed on KeyName
	prevKeyName := ""
	for rows.Next() {
		indexRow := proto.ShowIndexRow{}
		dest := []interface{}{
			&indexRow.Table,
			&indexRow.NonUnique,
			&indexRow.KeyName,
			&indexRow.SeqInIndex,
			&indexRow.ColumnName,
			&indexRow.Collation,
			&indexRow.Cardinality,
			&indexRow.SubPart,
			&indexRow.Packed,
			&indexRow.Null,
			&indexRow.IndexType,
			&indexRow.Comment,
			&indexRow.IndexComment,
			&indexRow.Visible,
		}

		// Cut dest to number of columns.
		// Some columns are not available at earlier versions of MySQL.
		if len(columns) < len(dest) {
			dest = dest[:len(columns)]
		}

		// Append dest to number of columns
		appendLen := len(columns) - len(dest)
		for i := 0; i < appendLen; i++ {
			col := new(interface{})
			dest = append(dest, col)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, err
		}
		if indexRow.KeyName != prevKeyName {
			indexes[indexRow.KeyName] = []proto.ShowIndexRow{}
			prevKeyName = indexRow.KeyName
		}
		indexes[indexRow.KeyName] = append(indexes[indexRow.KeyName], indexRow)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

func showStatus(c mysql.Connector, db, table string) (*proto.ShowTableStatus, error) {
	status := proto.ShowTableStatus{}
	rows, err := c.DB().Query(fmt.Sprintf("SHOW TABLE STATUS FROM %s WHERE Name='%s'", db, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		cols := []interface{}{
			&status.Name,
			&status.Engine,
			&status.Version,
			&status.RowFormat,
			&status.Rows,
			&status.AvgRowLength,
			&status.DataLength,
			&status.MaxDataLength,
			&status.IndexLength,
			&status.DataFree,
			&status.AutoIncrement,
			&status.CreateTime,
			&status.UpdateTime,
			&status.CheckTime,
			&status.Collation,
			&status.Checksum,
			&status.CreateOptions,
			&status.Comment,
		}

		sqlCols, err := rows.Columns()
		if err != nil {
			if err == sql.ErrNoRows {
				err = fmt.Errorf("table %s.%s doesn't exist", db, table)
			}
			return nil, err
		}

		appendLen := len(sqlCols) - len(cols)
		for i := 0; i < appendLen; i++ {
			col := new(interface{})
			cols = append(cols, col)
		}

		err = rows.Scan(cols...)
		if err != nil {
			if err == sql.ErrNoRows {
				err = fmt.Errorf("table %s.%s doesn't exist", db, table)
			}
			return nil, err
		}
	}

	return &status, nil
}

func getTablesFromSelectStmt(ss sqlparser.SelectStatement, depth uint) (sTables []proto.Table) {
	if depth > MAX_JOIN_DEPTH {
		return nil
	}
	depth++

	switch t := ss.(type) {
	case *sqlparser.Select:
		sTables = append(sTables, getTablesFromTableExprs(sqlparser.TableExprs(t.From))...)
	case *sqlparser.Union:
		sTables = append(sTables, getTablesFromSelectStmt(t.Left, depth)...)
		sTables = append(sTables, getTablesFromSelectStmt(t.Right, depth)...)
	}

	return sTables
}

func getTablesFromTableExprs(tes sqlparser.TableExprs) (tables []proto.Table) {
	for _, te := range tes {
		tables = append(tables, getTablesFromTableExpr(te, 0)...)
	}
	return tables
}

func getTablesFromTableExpr(te sqlparser.TableExpr, depth uint) (tables []proto.Table) {
	if depth > MAX_JOIN_DEPTH {
		return nil
	}

	depth++
	switch a := te.(type) {
	case *sqlparser.AliasedTableExpr:
		switch a.Expr.(type) {
		case sqlparser.TableName:
			t := a.Expr.(sqlparser.TableName)
			db := t.Qualifier.String()
			tbl := parseTableName(t.Name.String())
			if db != "" || tbl != "" {
				table := proto.Table{
					Db:    db,
					Table: tbl,
				}
				tables = append(tables, table)
			}
		case *sqlparser.DerivedTable:
			tables = append(tables, getTablesFromSelectStmt(a.Expr.(*sqlparser.DerivedTable).Select, depth)...)
		}

	case *sqlparser.JoinTableExpr:
		// This case happens for JOIN clauses. It recurses to the bottom
		// of the tree via the left expressions, then it unwinds. E.g. with
		// "a JOIN b JOIN c" the tree is:
		//
		//  Left			Right
		//  a     b      c	AliasedTableExpr (case above)
		//  |     |      |
		//  +--+--+      |
		//     |         |
		//    t2----+----+	JoinTableExpr
		//          |
		//        var t (t @ depth=1) JoinTableExpr
		//
		// Code will go left twice to arrive at "a". Then it will unwind and
		// store the right-side values: "b" then "c". Because of this, if
		// MAX_JOIN_DEPTH is reached, we lose the whole tree because if we take
		// the existing right-side tables, we'll generate a misleading partial
		// list of tables, e.g. "SELECT b c".
		tables = append(tables, getTablesFromTableExpr(a.LeftExpr, depth)...)
		tables = append(tables, getTablesFromTableExpr(a.RightExpr, depth)...)

	case *sqlparser.ParenTableExpr:
		tables = append(tables, getTablesFromTableExprs(a.Exprs)...)
	}

	return tables
}

func parseTableName(tableName string) string {
	// https://dev.mysql.com/doc/refman/5.7/en/select.html#idm140358784149168
	// You are permitted to specify DUAL as a dummy table name in situations where no tables are referenced:
	//
	// ```
	// mysql> SELECT 1 + 1 FROM DUAL;
	//         -> 2
	// ```
	// DUAL is purely for the convenience of people who require that all SELECT statements
	// should have FROM and possibly other clauses. MySQL may ignore the clauses.
	// MySQL does not require FROM DUAL if no tables are referenced.
	if tableName == "dual" {
		tableName = ""
	}
	return tableName
}
