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
)

func TableInfo(c mysql.Connector, tables *proto.TableInfoQuery) (proto.TableInfoResult, error) {
	res := make(proto.TableInfoResult)

	if len(tables.Create) > 0 {
		for _, t := range tables.Create {
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
		}
	}

	if len(tables.Index) > 0 {
		for _, t := range tables.Index {
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
		for _, t := range tables.Status {
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
	status := &proto.ShowTableStatus{}
	err := c.DB().QueryRow(fmt.Sprintf("SHOW TABLE STATUS FROM %s WHERE Name='%s'", db, table)).Scan(
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
	)
	if err == sql.ErrNoRows {
		err = fmt.Errorf("table %s.%s doesn't exist", db, table)
	}
	return status, err
}
