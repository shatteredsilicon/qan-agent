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
		tableNames := make([]string, 0)
		for i := range param.Table {
			if param.Table[i].Db == "" {
				tableNames = append(tableNames, param.Table[i].Table)
			}
		}

		// there are some tables don't have
		// explicit schemas, we guess it
		if len(tableNames) > 0 {
			guessDB, err = getGuessDBOfTables(c, tableNames)
			if err != nil {
				return nil, err
			}
			if guessDB != nil {
				dbName = guessDB.DB
			}
		}

		if len(tableNames) > 0 && dbName != "" {
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
		procedureNames := make([]string, 0)
		for i := range param.Procedure {
			if param.Procedure[i].DB == "" {
				procedureNames = append(procedureNames, param.Procedure[i].Name)
			}
		}

		// there are some procedures don't have
		// explicit schemas, we guess it
		if len(procedureNames) > 0 && dbName == "" {
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

	// fetch 2 rows to compare, see if it's ambiguous
	rows, err := c.DB().Query(fmt.Sprintf(`
		SELECT table_schema, COUNT(*) AS table_count, SUM(ifnull(table_rows,0)) AS table_rows
		FROM information_schema.tables
		WHERE table_name IN (%s)
		GROUP BY table_schema
		ORDER BY table_count DESC, table_rows DESC
		LIMIT 2;
	`, util.Placeholders(len(names))), names...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schema string
	tableCounts := make([]int64, 0)
	for i := 0; i < 2 && rows.Next(); i++ {
		var db string
		var tableCount, tableRows int64

		err = rows.Scan(&db, &tableCount, &tableRows)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		if schema == "" {
			schema = db
		}
		tableCounts = append(tableCounts, tableCount)
	}

	// if the second schema matches the same amount of tables
	// as the first one, means it's ambiguous
	return &proto.GuessDB{
		DB:          schema,
		IsAmbiguous: len(tableCounts) > 1 && tableCounts[0] == tableCounts[1],
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

	rows, err := c.DB().Query(fmt.Sprintf(`
		SELECT routine_schema, COUNT(*) AS procedure_count
		FROM information_schema.routines
		WHERE routine_type = 'PROCEDURE' AND specific_name IN (%s)
		GROUP BY routine_schema
		ORDER BY procedure_count DESC
		LIMIT 2
	`, util.Placeholders(len(names))), names...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schema string
	procedureCounts := make([]int64, 0)
	for i := 0; i < 2 && rows.Next(); i++ {
		var db string
		var procedureCount int64

		err = rows.Scan(&db, &procedureCount)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		if schema == "" {
			schema = db
		}
		procedureCounts = append(procedureCounts, procedureCount)
	}

	return &proto.GuessDB{
		DB:          schema,
		IsAmbiguous: len(procedureCounts) > 1 && procedureCounts[0] == procedureCounts[1],
	}, nil
}
