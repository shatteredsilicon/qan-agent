package queryinfo

import (
	"database/sql"
	"fmt"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/util"
	"github.com/shatteredsilicon/ssm/proto"
)

func QueryInfo(c mysql.Connector, param *proto.QueryInfoParam) (*proto.QueryInfoResult, error) {
	res := make(map[string]*proto.QueryInfo)
	var dbName string
	var guessDB *proto.GuessDB
	var err error

	if len(param.Table) > 0 {
		var dbMissed bool
		tableNames := make([]string, len(param.Table))
		for i := range param.Table {
			tableNames[i] = param.Table[i].Table
			if param.Table[i].Db == "" {
				dbMissed = true
			} else {
				dbName = param.Table[i].Db
				break
			}
		}

		if dbMissed && dbName == "" {
			guessDB, err = getGuessDBOfTables(c, tableNames)
			if err != nil {
				return nil, err
			}
			if guessDB != nil {
				dbName = guessDB.DB
			}
		}

		if dbMissed && dbName != "" {
			for i := range param.Table {
				if param.Table[i].Db == "" {
					param.Table[i].Db = dbName
				}
			}
			for i := range param.Index {
				if param.Index[i].Db == "" {
					param.Index[i].Db = dbName
				}
			}
			for i := range param.Status {
				if param.Status[i].Db == "" {
					param.Status[i].Db = dbName
				}
			}
		}

		tableRes, err := tableinfo.TableInfo(c, &proto.TableInfoQuery{
			UUID:   param.UUID,
			Create: param.Table,
			Index:  param.Index,
			Status: param.Status,
		})
		if err != nil {
			return nil, err
		}
		for k, v := range tableRes {
			res[k] = &proto.QueryInfo{
				Type:   v.Type,
				Create: v.Create,
				Index:  v.Index,
				Status: v.Status,
				Errors: v.Errors,
			}
		}
	}

	if len(param.Procedure) > 0 {
		var dbMissed bool
		procedureNames := make([]string, len(param.Procedure))
		for i := range param.Procedure {
			procedureNames[i] = param.Procedure[i].Name
			if param.Procedure[i].DB == "" {
				dbMissed = true
			} else {
				dbName = param.Procedure[i].DB
				break
			}
		}

		if dbMissed && dbName == "" {
			guessDB, err = getGuessDBOfProcedures(c, procedureNames)
			if err != nil {
				return nil, err
			}
			if guessDB != nil {
				dbName = guessDB.DB
			}
		}

		for _, p := range param.Procedure {
			if p.DB == "" {
				p.DB = dbName
			}

			dbProcedure := p.DB + "." + p.Name
			queryInfo, ok := res[dbProcedure]
			if !ok {
				res[dbProcedure] = &proto.QueryInfo{}
				queryInfo = res[dbProcedure]
			}

			db := util.EscapeString(p.DB)
			name := util.EscapeString(p.Name)
			def, err := showCreateProcedure(c, util.Ident(db, name))
			if err != nil {
				if queryInfo.Errors == nil {
					queryInfo.Errors = []string{}
				}
				queryInfo.Errors = append(queryInfo.Errors, fmt.Sprintf("SHOW CREATE PROCEDURE %s: %s", p.Name, err))
				continue
			}
			queryInfo.Create = def
			queryInfo.Type = proto.TypeDBProcedure
		}
	}

	return &proto.QueryInfoResult{
		GuessDB: guessDB,
		Info:    res,
	}, nil
}

func showCreateProcedure(c mysql.Connector, name string) (string, error) {
	// Result from SHOW CREATE PROCEDURE includes six columns, "Procedure",
	// "sql_mode", "Create Procedure", "character_set_client", "collation_connection"
	// and "Database Collation"
	var c1, c2, def, c4, c5, c6 string
	err := c.DB().QueryRow("SHOW CREATE PROCEDURE "+name).Scan(&c1, &c2, &def, &c4, &c5, &c6)
	if err == sql.ErrNoRows {
		err = fmt.Errorf("procedure %s doesn't exist ", name)
	}
	return def, err
}

func showCreateView(c mysql.Connector, name string) (string, error) {
	// Result from SHOW CREATE VIEW includes four columns, "View",
	// "Create View", "character_set_client", "collation_connection"
	var c1, def, c3, c4 string
	err := c.DB().QueryRow("SHOW CREATE VIEW "+name).Scan(&c1, &def, &c3, &c4)
	if err == sql.ErrNoRows {
		err = fmt.Errorf("view %s doesn't exist ", name)
	}
	return def, err
}

// getGuessDBOfTables tries to guess the database name of
// tableNames (using information_schema.tables), the
// database with least ambiguity will be returned,
// a nil result will be returned if the tables are not found
func getGuessDBOfTables(c mysql.Connector, tableNames []string) (*proto.GuessDB, error) {
	if len(tableNames) == 0 {
		return nil, nil
	}

	names := make([]interface{}, len(tableNames))
	for i := range tableNames {
		names[i] = tableNames[i]
	}

	var db string
	var count int
	err := c.DB().QueryRow(fmt.Sprintf(`
		SELECT t.table_schema, tt.schema_count
		FROM information_schema.tables t
		JOIN (
			SELECT table_name, COUNT(table_schema) AS schema_count
			FROM information_schema.tables
			GROUP BY table_name
		) tt ON t.table_name = tt.table_name
		WHERE t.table_name IN (%s)
		ORDER BY tt.schema_count ASC, t.table_rows IS NULL ASC, t.table_rows DESC
		LIMIT 1
	`, util.Placeholders(len(names))), names...).Scan(&db, &count)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &proto.GuessDB{
		DB:          db,
		IsAmbiguous: count != 1,
	}, nil
}

// getGuessDBOfProcedures tries to guess the database name of
// procedureNames (using information_schema.routines), the
// database with least ambiguity will be returned,
// a nil result will be returned if the procedures are not found
func getGuessDBOfProcedures(c mysql.Connector, procedureNames []string) (*proto.GuessDB, error) {
	if len(procedureNames) == 0 {
		return nil, nil
	}

	names := make([]interface{}, len(procedureNames))
	for i := range procedureNames {
		names[i] = procedureNames[i]
	}

	var db string
	var count int
	err := c.DB().QueryRow(fmt.Sprintf(`
		SELECT t.routine_schema, tt.schema_count
		FROM information_schema.routines t
		JOIN (
			SELECT specific_name, COUNT(routine_schema) AS schema_count
			FROM information_schema.routines
			GROUP BY specific_name
		) tt ON t.specific_name = tt.specific_name
		WHERE t.routine_type = 'PROCEDURE' AND t.specific_name IN (%s)
		ORDER BY tt.schema_count ASC
		LIMIT 1
	`, util.Placeholders(len(names))), names...).Scan(&db, &count)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &proto.GuessDB{
		DB:          db,
		IsAmbiguous: count != 1,
	}, nil
}
