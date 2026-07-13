package tableinfo

import (
	"database/sql"
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/util"
	"github.com/shatteredsilicon/ssm/proto"
)

const (
	MAX_EXPR_DEPTH = 100
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
	Type        proto.DBObjectType      `json:",omitempty"`
	Create      string                  `json:",omitempty"`
	Index       map[string]*IndexStatus `json:",omitempty"`
	Status      *TableStatus            `json:",omitempty"`
	Errors      []string                `json:",omitempty"`
	GuessSchema *proto.GuessDB          `json:"-"`
}

type TableParam struct {
	Db          string
	Table       string
	GuessSchema *proto.GuessDB `json:"-"`
}

type TableInfoQuery struct {
	UUID   string
	DB     string       // GLOBAL database
	Create []TableParam // SHOW CREATE TABLE Db.Table
	Index  []TableParam // SHOW INDEXES FROM Db.Table
	Status []TableParam // SHOW TABLE STATUS FROM Db LIKE 'Table'
}

type TableInfoResult map[string]*TableInfo

func GetTableInfo(db *sql.DB, tableQuery *TableInfoQuery) (TableInfoResult, error) {
	res := make(TableInfoResult)

	createList := append([]TableParam{}, tableQuery.Create...)
	indexList := append([]TableParam{}, tableQuery.Index...)
	statusList := append([]TableParam{}, tableQuery.Status...)

	for i := 0; i < len(createList) || i < len(tableQuery.Create); i++ {
		if i >= len(createList) {
			if err := GuessAndFillSchemas(db, &TableInfoQuery{
				UUID:   tableQuery.UUID,
				DB:     tableQuery.DB,
				Create: tableQuery.Create[i:],
				Index:  tableQuery.Index[i:],
				Status: tableQuery.Status[i:],
			}); err != nil {
				// ignore this error, as we already get sufficient table info
				// for top level tables.
				break
			}

			createList = append(createList, tableQuery.Create[i:]...)
			indexList = append(indexList, tableQuery.Index[i:]...)
			statusList = append(statusList, tableQuery.Status[i:]...)
		}

		t := createList[i]

		dbTable := t.Table
		if len(t.Db) > 0 {
			dbTable = t.Db + "." + t.Table
		}

		tableInfo, ok := res[dbTable]
		if !ok {
			res[dbTable] = &TableInfo{GuessSchema: t.GuessSchema}
			tableInfo = res[dbTable]
		}

		schema := util.EscapeString(t.Db)
		table := util.EscapeString(t.Table)
		tableType, def, err := showCreate(db, tableQuery.DB, schema, table)
		if err != nil {
			if tableInfo.Errors == nil {
				tableInfo.Errors = []string{}
			}
			tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("Can't get definition of %s: %s", t.Table, err))
			continue
		}
		tableInfo.Create = def
		tableInfo.Type = tableType

		if tableType != proto.TypeDBView {
			continue
		}

		pr, err := pg_query.Parse(def)
		if err != nil {
			continue
		}
		tables := getTablesFromParseResult(pr)
		tableQuery.Create = append(tableQuery.Create, tables...)
		tableQuery.Index = append(tableQuery.Index, tables...)
		tableQuery.Status = append(tableQuery.Status, tables...)
	}

	for _, t := range indexList {
		dbTable := t.Table
		if len(t.Db) > 0 {
			dbTable = t.Db + "." + t.Table
		}

		tableInfo, ok := res[dbTable]
		if !ok {
			res[dbTable] = &TableInfo{GuessSchema: t.GuessSchema}
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
			tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("Can't get INDEX information of %s.%s: %s", t.Db, t.Table, err))
			continue
		}
		tableInfo.Index = indexes
	}

	for _, t := range statusList {
		dbTable := t.Table
		if len(t.Db) > 0 {
			dbTable = t.Db + "." + t.Table
		}

		tableInfo, ok := res[dbTable]
		if !ok {
			res[dbTable] = &TableInfo{GuessSchema: t.GuessSchema}
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
			tableInfo.Errors = append(tableInfo.Errors, fmt.Sprintf("Can't get STATUS information of %s.%s: %s", t.Db, t.Table, err))
			continue
		}
		tableInfo.Status = status
	}

	return res, nil
}

func showCreate(db *sql.DB, catalog, schema, table string) (proto.DBObjectType, string, error) {
	var def string
	var viewDef sql.NullString
	var tableType proto.DBObjectType

	if err := db.QueryRow(`
		SELECT view_definition
		FROM information_schema.views
		WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
	`, catalog, schema, table).Scan(&viewDef); err == nil && viewDef.Valid {
		return proto.TypeDBView, viewDef.String, nil
	} else if err != nil && err != sql.ErrNoRows {
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
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &status, nil
}

func getTablesFromNode(node *pg_query.Node, depth uint) (sTables []TableParam) {
	if depth > MAX_EXPR_DEPTH {
		return nil
	}
	depth++

	switch s := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		sTables = append(sTables, getTablesFromSelectStmt(s.SelectStmt, depth)...)
	case *pg_query.Node_TableLikeClause:
		sTables = append(sTables, TableParam{Db: s.TableLikeClause.Relation.Schemaname, Table: s.TableLikeClause.Relation.Relname})
	case *pg_query.Node_FromExpr:
		for _, fe := range s.FromExpr.Fromlist {
			sTables = append(sTables, getTablesFromNode(fe, depth)...)
		}
	case *pg_query.Node_FuncCall:
		for _, arg := range s.FuncCall.Args {
			sTables = append(sTables, getTablesFromNode(arg, depth)...)
		}
	case *pg_query.Node_FuncExpr:
		for _, arg := range s.FuncExpr.Args {
			sTables = append(sTables, getTablesFromNode(arg, depth)...)
		}
	case *pg_query.Node_ResTarget:
		for _, i := range s.ResTarget.Indirection {
			sTables = append(sTables, getTablesFromNode(i, depth)...)
		}
		if s.ResTarget.Val != nil {
			sTables = append(sTables, getTablesFromNode(s.ResTarget.Val, depth)...)
		}
	case *pg_query.Node_JoinExpr:
		if s.JoinExpr.Larg != nil {
			sTables = append(sTables, getTablesFromNode(s.JoinExpr.Larg, depth)...)
		}
		if s.JoinExpr.Rarg != nil {
			sTables = append(sTables, getTablesFromNode(s.JoinExpr.Rarg, depth)...)
		}
	case *pg_query.Node_RangeVar:
		sTables = append(sTables, TableParam{Db: s.RangeVar.Schemaname, Table: s.RangeVar.Relname})
	case *pg_query.Node_AExpr:
		if s.AExpr.Lexpr != nil {
			sTables = append(sTables, getTablesFromNode(s.AExpr.Lexpr, depth)...)
		}
		if s.AExpr.Rexpr != nil {
			sTables = append(sTables, getTablesFromNode(s.AExpr.Rexpr, depth)...)
		}
	case *pg_query.Node_SubLink:
		if s.SubLink.Subselect != nil {
			sTables = append(sTables, getTablesFromNode(s.SubLink.Subselect, depth)...)
		}
	}

	return sTables
}

func getTablesFromSelectStmt(stmt *pg_query.SelectStmt, depth uint) (sTables []TableParam) {
	for _, t := range stmt.TargetList {
		sTables = append(sTables, getTablesFromNode(t, depth)...)
	}
	for _, f := range stmt.FromClause {
		sTables = append(sTables, getTablesFromNode(f, depth)...)
	}
	return
}

func getTablesFromParseResult(pr *pg_query.ParseResult) []TableParam {
	var tables []TableParam
	for _, s := range pr.Stmts {
		tables = append(tables, getTablesFromNode(s.Stmt, 0)...)
	}
	newTables := make([]TableParam, 0)
	tableMap := make(map[string]struct{})
	for _, t := range tables {
		key := t.Db + "." + t.Table
		if _, ok := tableMap[key]; !ok {
			tableMap[key] = struct{}{}
			newTables = append(newTables, t)
		}
	}
	return newTables
}

func GuessAndFillSchemas(db *sql.DB, query *TableInfoQuery) error {
	guessMap := make(map[string]proto.GuessDB)
	var err error

	tableNames := make([]string, 0)
	for i := range query.Create {
		if query.Create[i].Db == "" {
			tableNames = append(tableNames, query.Create[i].Table)
		}
	}

	// there are some tables don't have
	// explicit schemas, we guess it
	if len(tableNames) > 0 {
		guessMap, err = getGuessSchemasOfTables(db, tableNames)
		if err != nil {
			return err
		}
	}

	if len(guessMap) > 0 {
		for i := range query.Create {
			if len(query.Create[i].Db) > 0 {
				continue
			}
			if guessSchema, ok := guessMap[query.Create[i].Table]; ok {
				query.Create[i].Db = guessSchema.DB
				query.Create[i].GuessSchema = &guessSchema
			}
		}
		for i := range query.Index {
			if len(query.Index[i].Db) > 0 {
				continue
			}
			if guessSchema, ok := guessMap[query.Index[i].Table]; ok {
				query.Index[i].Db = guessSchema.DB
				query.Index[i].GuessSchema = &guessSchema
			}
		}
		for i := range query.Status {
			if len(query.Status[i].Db) > 0 {
				continue
			}
			if guessSchema, ok := guessMap[query.Status[i].Table]; ok {
				query.Status[i].Db = guessSchema.DB
				query.Status[i].GuessSchema = &guessSchema
			}
		}
	}

	return nil
}

// getGuessSchemasOfTables tries to guess the schemas of
// tableNames (using information_schema.tables),
// a nil result will be returned if the tables are not found
func getGuessSchemasOfTables(db *sql.DB, tableNames []string) (map[string]proto.GuessDB, error) {
	if len(tableNames) == 0 {
		return nil, nil
	}

	names := make([]interface{}, len(tableNames))
	for i := range tableNames {
		names[i] = tableNames[i]
	}

	// fetch 2 rows to compare, see if it's ambiguous
	rows, err := db.Query(fmt.Sprintf(`
		SELECT tables.table_schema, tables.table_name, pg_class.reltuples::bigint AS table_rows
		FROM information_schema.tables tables
		LEFT JOIN pg_class ON pg_class.oid = CONCAT(tables.table_schema, '.', tables.table_name)::regclass
		WHERE tables.table_name IN (%s)
		ORDER BY table_rows DESC
	`, util.NumericPlaceholders(len(names))), names...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	guessMap := make(map[string]proto.GuessDB)
	for rows.Next() {
		var schema, table string
		var tableRows int64

		err = rows.Scan(&schema, &table, &tableRows)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		if guessDB, exists := guessMap[table]; !exists || !guessDB.IsAmbiguous {
			guessMap[table] = proto.GuessDB{
				DB:          schema,
				IsAmbiguous: exists,
			}
		}
	}

	return guessMap, nil
}
