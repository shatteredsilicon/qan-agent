package queryinfo

import (
	"database/sql"
	"encoding/json"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/util"
	"github.com/shatteredsilicon/ssm/proto"
)

// QueryInfo represents a TABLE/PROCEDURE/VIEW
// structure of QueryInfo cmd api
type QueryInfo struct {
	Type            proto.DBObjectType
	Create          string                            `json:",omitempty"`
	Index           map[string]*tableinfo.IndexStatus `json:",omitempty"`
	Status          *tableinfo.TableStatus            `json:",omitempty"`
	Errors          []string                          `json:",omitempty"`
	IsSchemaGuessed bool                              `json:",omitempty"`
}

// QueryInfoResult represents the response
// of QueryInfo cmd api
type QueryInfoResult struct {
	Info        map[string]*QueryInfo
	SkipExplain bool
}

type ProcedureParam struct {
	DB          string
	Name        string
	GuessSchema *proto.GuessDB `json:"-"`
}

type QueryInfoParam struct {
	UUID         string
	DB           string                 // GLOBAL database
	Table        []tableinfo.TableParam // SHOW CREATE TABLE Db.Table
	Procedure    []ProcedureParam
	Index        []tableinfo.TableParam // SHOW INDEXES FROM Db.Table
	Status       []tableinfo.TableParam // SHOW TABLE STATUS FROM Db LIKE 'Table'
	QueryExample string
}

func GetQueryInfo(db *sql.DB, param *QueryInfoParam) (*QueryInfoResult, error) {
	res := make(map[string]*QueryInfo)
	guessMap := make(map[string]proto.GuessDB)
	var err error

	if len(param.Table) > 0 {
		tableNames := make([]string, 0)
		for i := range param.Table {
			if param.Table[i].Db == "" {
				tableNames = append(tableNames, param.Table[i].Table)
			}
		}

		// there are some tables don't have
		// explicit schemas, we guess it
		if len(tableNames) > 0 {
			guessMap, err = getGuessSchemasOfTables(db, tableNames)
			if err != nil {
				return nil, err
			}
		}

		if len(guessMap) > 0 {
			for i := range param.Table {
				if len(param.Table[i].Db) > 0 {
					continue
				}
				if guessSchema, ok := guessMap[param.Table[i].Table]; ok {
					param.Table[i].Db = guessSchema.DB
					param.Table[i].GuessSchema = &guessSchema
				}
			}
			for i := range param.Index {
				if len(param.Index[i].Db) > 0 {
					continue
				}
				if guessSchema, ok := guessMap[param.Index[i].Table]; ok {
					param.Index[i].Db = guessSchema.DB
					param.Index[i].GuessSchema = &guessSchema
				}
			}
			for i := range param.Status {
				if len(param.Status[i].Db) > 0 {
					continue
				}
				if guessSchema, ok := guessMap[param.Status[i].Table]; ok {
					param.Status[i].Db = guessSchema.DB
					param.Status[i].GuessSchema = &guessSchema
				}
			}
		}

		tableRes, err := tableinfo.GetTableInfo(db, &tableinfo.TableInfoQuery{
			UUID:   param.UUID,
			DB:     param.DB,
			Create: param.Table,
			Index:  param.Index,
			Status: param.Status,
		})
		if err != nil {
			return nil, err
		}
		for k, v := range tableRes {
			res[k] = &QueryInfo{
				Type:            v.Type,
				Create:          v.Create,
				Index:           v.Index,
				Status:          v.Status,
				Errors:          v.Errors,
				IsSchemaGuessed: v.GuessSchema != nil && v.GuessSchema.IsAmbiguous,
			}
		}
	}

	if len(param.Procedure) > 0 {
		procedureNames := make([]string, 0)
		for i := range param.Procedure {
			if param.Procedure[i].DB == "" {
				procedureNames = append(procedureNames, param.Procedure[i].Name)
			}
		}

		// there are some procedures don't have
		// explicit schemas, we guess it
		if len(procedureNames) > 0 {
			guessMap, err = getGuessSchemasOfProcedures(db, procedureNames)
			if err != nil {
				return nil, err
			}
		}

		if len(guessMap) > 0 {
			for i := range param.Procedure {
				if len(param.Procedure[i].DB) > 0 {
					continue
				}
				if guessSchema, ok := guessMap[param.Procedure[i].Name]; ok {
					param.Procedure[i].DB = guessSchema.DB
					param.Procedure[i].GuessSchema = &guessSchema
				}
			}
		}

		for _, p := range param.Procedure {
			dbProcedure := p.DB + "." + p.Name
			queryInfo, ok := res[dbProcedure]
			if !ok {
				res[dbProcedure] = &QueryInfo{IsSchemaGuessed: p.GuessSchema != nil && p.GuessSchema.IsAmbiguous}
				queryInfo = res[dbProcedure]
			}

			schema := util.EscapeString(p.DB)
			name := util.EscapeString(p.Name)
			def, err := showCreateProcedure(db, schema, name)
			queryInfo.Type = proto.TypeDBProcedure
			if err != nil {
				if queryInfo.Errors == nil {
					queryInfo.Errors = []string{}
				}
				queryInfo.Errors = append(queryInfo.Errors, fmt.Sprintf("Can't get definition of procedure %s: %s", p.Name, err))
				continue
			}
			queryInfo.Create = def
		}
	}

	var skipExplain bool
	if len(param.QueryExample) > 0 {
		skipExplain = shouldSkipExplain(param.QueryExample)
	}

	return &QueryInfoResult{
		Info:        res,
		SkipExplain: skipExplain,
	}, nil
}

func shouldSkipExplain(query string) bool {
	jsonStr, err := pg_query.ParseToJSON(query)
	if err != nil {
		return false
	}

	parseTree := map[string]interface{}{}
	if err = json.Unmarshal([]byte(jsonStr), &parseTree); err != nil {
		return false
	}

	return util.IsJSONKeyExists(parseTree, "ParamRef", 0)
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

// getGuessSchemasOfProcedures tries to guess the schemas of
// procedures (using information_schema.routines), the
// schema with least ambiguity will be returned,
// a nil result will be returned if the tables are not found
func getGuessSchemasOfProcedures(db *sql.DB, procedureNames []string) (map[string]proto.GuessDB, error) {
	if len(procedureNames) == 0 {
		return nil, nil
	}

	names := make([]interface{}, len(procedureNames))
	for i := range procedureNames {
		names[i] = procedureNames[i]
	}

	// fetch 2 rows to compare, see if it's ambiguous
	rows, err := db.Query(fmt.Sprintf(`
		SELECT routine_schema, routine_name
		FROM information_schema.routines
		WHERE routine_type = 'PROCEDURE' AND routine_name IN (%s)
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
		var schema, name string

		err = rows.Scan(&schema, &name)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		if guessSchema, exists := guessMap[name]; !exists || !guessSchema.IsAmbiguous {
			guessMap[name] = proto.GuessDB{
				DB:          schema,
				IsAmbiguous: exists,
			}
		}
	}

	return guessMap, nil
}

func showCreateProcedure(db *sql.DB, schema, name string) (string, error) {
	var def string
	err := db.QueryRow(`
		SELECT pg_get_functiondef(p.oid)
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE p.proname = $1
		AND n.nspname = $2
	`, name, schema).Scan(&def)
	if err == sql.ErrNoRows {
		err = fmt.Errorf("procedure %s doesn't exist ", name)
	}
	return def, err
}
