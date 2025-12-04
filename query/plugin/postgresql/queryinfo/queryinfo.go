package queryinfo

import (
	"database/sql"
	"encoding/json"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/util"
	"github.com/shatteredsilicon/ssm/proto"
	protoQuery "github.com/shatteredsilicon/ssm/proto/query"
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

type QueryInfoParam struct {
	UUID         string
	DB           string                 // GLOBAL database
	Table        []tableinfo.TableParam // SHOW CREATE TABLE Db.Table
	Procedure    []protoQuery.Procedure
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
// tableNames (using information_schema.tables), the
// schema with least ambiguity will be returned,
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
