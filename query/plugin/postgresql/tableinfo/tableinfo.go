package tableinfo

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/util"
	"github.com/shatteredsilicon/ssm/proto"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	MAX_JOIN_DEPTH = 100
	defaultSchema  = "public"
)

type IndexStatus struct {
	IdxScan     proto.NullInt64
	IdxTupRead  proto.NullInt64
	IdxTupFetch proto.NullInt64
}

type TableStatus struct {
	SeqScan          proto.NullInt64
	SeqTupRead       proto.NullInt64
	IdxScan          proto.NullInt64
	IdxTupFetch      proto.NullInt64
	NTupIns          proto.NullInt64
	NTupUpd          proto.NullInt64
	NTupDel          proto.NullInt64
	NTupHotUpd       proto.NullInt64
	NLiveTup         proto.NullInt64
	NDeadTup         proto.NullInt64
	NModSinceAnalyze proto.NullInt64
	NInsSinceVaccum  proto.NullInt64
	VaccumCount      proto.NullInt64
	AutoVaccumCount  proto.NullInt64
	AnalyzeCount     proto.NullInt64
	AutoAnalyzeCount proto.NullInt64
}

type TableInfo struct {
	Type   proto.DBObjectType      `json:",omitempty"`
	Create string                  `json:",omitempty"`
	Index  map[string]*IndexStatus `json:",omitempty"`
	Status *TableStatus            `json:",omitempty"`
	Errors []string                `json:",omitempty"`
}

type TableInfoResult map[string]*TableInfo

func GetTableInfo(db *sql.DB, tables *proto.TableInfoQuery) (TableInfoResult, error) {
	res := make(TableInfoResult)

	createList := append([]proto.Table{}, tables.Create...)
	indexList := append([]proto.Table{}, tables.Index...)
	statusList := append([]proto.Table{}, tables.Status...)

	if len(tables.Create) > 0 {
		for i := 0; i < len(createList); i++ {
			t := createList[i]

			if t.Db == "" {
				t.Db = getDefaultSchema(t.Table)
			}

			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &TableInfo{}
				tableInfo = res[dbTable]
			}

			schema := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			tableType, def, err := showCreate(db, tables.DB, schema, table)
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
			s, err := sqlparser.NewTestParser().Parse(def)
			if err != nil {
				continue
			}

			switch ss := s.(type) {
			case sqlparser.SelectStatement:
				tables := getTablesFromSelectStmt(ss, 0)
				createList = append(createList, tables...)
			default:
				continue
			}
		}
	}

	if len(tables.Index) > 0 {
		for _, t := range indexList {
			if t.Db == "" {
				t.Db = getDefaultSchema(t.Table)
			}

			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &TableInfo{}
				tableInfo = res[dbTable]
			}
			if tableInfo.Type == proto.TypeDBView {
				continue
			}

			schema := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			indexes, err := showIndex(db, schema, table)
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
			if t.Db == "" {
				t.Db = getDefaultSchema(t.Table)
			}

			dbTable := t.Db + "." + t.Table
			tableInfo, ok := res[dbTable]
			if !ok {
				res[dbTable] = &TableInfo{}
				tableInfo = res[dbTable]
			}
			if tableInfo.Type == proto.TypeDBView {
				continue
			}

			schema := util.EscapeString(t.Db)
			table := util.EscapeString(t.Table)
			status, err := showStatus(db, schema, table)
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

func showCreate(db *sql.DB, catalog, schema, table string) (proto.DBObjectType, string, error) {
	var def, viewDef string
	var tableType proto.DBObjectType

	if err := db.QueryRow(`
		SELECT view_definition
		FROM information_schema.views
		WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
	`, catalog, schema, table).Scan(&viewDef); err == nil {
		return proto.TypeDBView, viewDef, nil
	} else if err != sql.ErrNoRows {
		return tableType, "", err
	}

	tableType = proto.TypeDBTable
	err := db.QueryRow(`
		SELECT 'CREATE TABLE ' || table_name || E' (\n' || column_definition || E'\n);'
		FROM (
			SELECT
				table_name,
				string_agg(
					E'\t' || column_name || ' ' || data_type ||
					CASE
						WHEN character_maximum_length IS NOT NULL
						THEN '(' || character_maximum_length || ')'
						ELSE ''
					END ||
					CASE
						WHEN is_nullable = 'NO'
						THEN ' NOT NULL'
						ELSE ''
					END,
					E',\n' ORDER BY ordinal_position
				) AS column_definition
			FROM information_schema.columns
			WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
			GROUP BY table_name
		) AS table_columns
	`, catalog, schema, table).Scan(&def)
	if err != nil {
		return tableType, "", err
	}

	rows, err := db.Query(`
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
	`, schema, table)
	if err != nil {
		return tableType, "", err
	}
	defer rows.Close()

	for rows.Next() {
		var idxDef string
		err = rows.Scan(&idxDef)
		if err != nil {
			return tableType, "", err
		}
		if !strings.HasSuffix(strings.TrimSpace(idxDef), ";") {
			idxDef += ";"
		}
		def += "\n" + idxDef
	}

	return tableType, def, err
}

func showIndex(db *sql.DB, schema, table string) (map[string]*IndexStatus, error) {
	rows, err := db.Query(`
		SELECT
			indexrelname,
			idx_scan,
			idx_tup_read,
			idx_tup_fetch
		FROM pg_stat_all_indexes
		WHERE schemaname = $1 AND relname = $2
	`, schema, table)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	defer rows.Close()

	indexes := map[string]*IndexStatus{}
	for rows.Next() {
		var indexName string
		var indexRow IndexStatus

		err := rows.Scan(
			&indexName,
			&indexRow.IdxScan,
			&indexRow.IdxTupRead,
			&indexRow.IdxTupFetch,
		)
		if err != nil {
			return nil, err
		}
		indexes[indexName] = &indexRow
	}

	return indexes, nil
}

func showStatus(db *sql.DB, schema, table string) (*TableStatus, error) {
	var status TableStatus
	err := db.QueryRow(`
		SELECT
			seq_scan,
			seq_tup_read,
			idx_scan,
			idx_tup_fetch,
			n_tup_ins,
			n_tup_upd,
			n_tup_del,
			n_tup_hot_upd,
			n_live_tup,
			n_dead_tup,
			n_mod_since_analyze,
			n_ins_since_vacuum,
			vacuum_count,
			autovacuum_count,
			analyze_count,
			autoanalyze_count
		FROM pg_stat_all_tables
		WHERE schemaname = $1 AND relname = $2
	`, schema, table).Scan(
		&status.SeqScan,
		&status.SeqTupRead,
		&status.IdxScan,
		&status.IdxTupFetch,
		&status.NTupIns,
		&status.NTupUpd,
		&status.NTupDel,
		&status.NTupHotUpd,
		&status.NLiveTup,
		&status.NDeadTup,
		&status.NModSinceAnalyze,
		&status.NInsSinceVaccum,
		&status.VaccumCount,
		&status.AutoVaccumCount,
		&status.AnalyzeCount,
		&status.AutoAnalyzeCount,
	)
	if err != nil {
		return nil, err
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

func getDefaultSchema(table string) string {
	if strings.HasPrefix(table, "pg_") {
		return "pg_catalog"
	}
	return defaultSchema
}
