package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer/event"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/explain"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/queryinfo"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/util"
	"github.com/shatteredsilicon/ssm/proto"
	"github.com/shatteredsilicon/ssm/proto/qan"
	queryProto "github.com/shatteredsilicon/ssm/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Collector interface {
	Prepare() error
	Start(context.Context)
	Stop()
}

func pretchDataHandler(config analyzer.QAN, db *sql.DB) func(*event.Class) error {
	return func(class *event.Class) error {
		if !util.ValueOf(config.PrefetchMetadata) {
			return nil
		}

		q := queryinfo.QueryInfoParam{}

		query := class.Fingerprint
		if class.Example != nil && class.Example.Query != "" {
			query = class.Example.Query
			q.DB = class.Example.Db
		}

		abstract, tables, procedures, err := parseQuery(query)
		if err != nil {
			return err
		}

		for _, table := range tables {
			q.Table = append(q.Table, tableinfo.TableParam{Db: table.Db, Table: table.Table})
		}
		for _, procedure := range procedures {
			q.Procedure = append(q.Procedure, queryinfo.ProcedureParam{DB: procedure.DB, Name: procedure.Name})
		}

		queryInfo, err := queryinfo.GetQueryInfo(db, &q)
		if err != nil {
			return err
		}

		if queryInfo == nil || queryInfo.Info == nil {
			return err
		}

		var tMetadata []qan.TableMetadata
		var vmetadata []qan.TableMetadata
		var pMetadata []qan.ProcedureMetadata
		guessedSchemas := make(map[string]string)
		for id, info := range queryInfo.Info {
			if info == nil {
				continue
			}

			dbAndName := strings.SplitN(id, ".", 2)
			db, name := "", dbAndName[len(dbAndName)-1]
			if len(dbAndName) == 2 {
				db = dbAndName[0]
			}
			switch info.Type {
			case proto.TypeDBView:
				vmetadata = append(vmetadata, qan.TableMetadata{
					Table:     queryProto.Table{Db: db, Table: name},
					QueryInfo: *info,
				})
			case proto.TypeDBProcedure:
				pMetadata = append(pMetadata, qan.ProcedureMetadata{
					Procedure: queryProto.Procedure{DB: db, Name: name},
					QueryInfo: *info,
				})
			default:
				tMetadata = append(tMetadata, qan.TableMetadata{
					Table:     queryProto.Table{Db: db, Table: name},
					QueryInfo: *info,
				})
			}

			if info.GuessSchema == nil {
				continue
			}
			names := strings.Split(id, ".")
			guessedSchemas[names[len(names)-1]] = info.GuessSchema.DB
		}

		if util.ValueOf(config.PrefetchMetadata) {
			class.Abstract = abstract
			if len(tMetadata) > 0 || len(vmetadata) > 0 || len(pMetadata) > 0 {
				metadata := &qan.Metadata{
					Tables:     tMetadata,
					Views:      vmetadata,
					Procedures: pMetadata,
				}
				class.Metadata = metadata
				if class.Example != nil && class.Example.Query != "" {
					class.Example.Metadata = metadata
				}
			}
		}

		if class.Example == nil || class.Example.Query == "" {
			return nil
		}

		explainResult, err := explain.Explain(db, class.Example.Query, guessedSchemas)
		if err != nil {
			return err
		}

		explainBytes, _ := json.Marshal(explainResult)
		class.Example.Explain = string(explainBytes)

		return nil
	}
}

func parseQuery(query string) (
	abstract string,
	tables []queryProto.Table,
	procedures []queryProto.Procedure,
	err error,
) {
	// Fingerprints replace IN (1, 2) -> in (?+) but "?+" is not valid SQL so
	// it breaks sqlparser/.
	query = strings.Replace(query, "?+", "? ", -1)

	// Strip leading comments before parsing as it could cause problem
	query = sqlparser.StripLeadingComments(query)

	// Internal newlines break everything.
	query = strings.Replace(query, "\n", " ", -1)

	var parseResult *pg_query.ParseResult
	if parseResult, err = pg_query.Parse(query); err != nil {
		return
	}

	if len(parseResult.Stmts) > 0 {
		// Only parse first stmt
		stmt := parseResult.Stmts[0].Stmt
		switch node := stmt.Node.(type) {
		case *pg_query.Node_CreateStmt:
			abstract = "CREATE TABLE"
		case *pg_query.Node_DropStmt:
			abstract = "DROP TABLE"
		case *pg_query.Node_TruncateStmt:
			abstract = "TRUNCATE TABLE"
		case *pg_query.Node_VariableShowStmt:
			abstract = "SHOW"
		case *pg_query.Node_VariableSetStmt:
			abstract = "SET"
		case *pg_query.Node_CreatedbStmt:
			abstract = "CREATE DATABASE " + node.CreatedbStmt.Dbname
		case *pg_query.Node_IndexStmt:
			abstract = "ALTER TABLE"
		case *pg_query.Node_CallStmt:
			abstract = "CALL"
			var schema, name string
			for i := len(node.CallStmt.Funccall.Funcname) - 1; i >= 0; i-- {
				funcname, ok := node.CallStmt.Funccall.Funcname[i].Node.(*pg_query.Node_String_)
				if !ok {
					continue
				}
				if name == "" {
					name = funcname.String_.Sval
					continue
				}
				if schema == "" {
					schema = funcname.String_.Sval
				}
				break
			}
			procedures = append(procedures, queryProto.Procedure{
				DB:   schema,
				Name: name,
			})
		default:
			v := reflect.ValueOf(stmt.Node)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			stmtType := v.Type()
			for i := 0; i < stmtType.NumField() && abstract == ""; i++ {
				for _, tag := range strings.Split(stmtType.Field(i).Tag.Get("protobuf"), ",") {
					if !strings.HasPrefix(tag, "name=") || !strings.HasSuffix(tag, "_stmt") {
						continue
					}
					abstract = strings.ToUpper(strings.ReplaceAll(strings.TrimPrefix(strings.TrimSuffix(tag, "_stmt"), "name="), "_", " "))
					break
				}
			}
		}
	}

	var extraTables []queryProto.Table
	if len(tables) == 0 && len(procedures) == 0 {
		tables, extraTables, procedures = walkQueryNode(parseResult)
	}

	if abstract == "SELECT" {
		for _, table := range tables {
			if table.Db == "" {
				abstract += " " + table.Table
			} else {
				abstract += " " + fmt.Sprintf("%s.%s", table.Db, table.Table)
			}
		}
	} else if abstract == "CALL" {
		for _, procedure := range procedures {
			if procedure.DB == "" {
				abstract += " " + procedure.Name
			} else {
				abstract += " " + fmt.Sprintf("%s.%s", procedure.DB, procedure.Name)
			}
		}
	} else if abstract != "" && len(tables) > 0 {
		abstract += " " + tables[0].String()
	}

	for _, extraTable := range extraTables {
		tables = append(tables, queryProto.Table{
			Db:    extraTable.Db,
			Table: extraTable.Table,
		})
	}

	return
}

var pgRangeVarType = reflect.TypeOf(pg_query.RangeVar{})

func walkQueryNode(node interface{}) (tables []queryProto.Table, extraTables []queryProto.Table, procedures []queryProto.Procedure) {
	v := reflect.ValueOf(node)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Ptr:
		fallthrough
	case reflect.Struct:
		t := v.Type()
		switch t {
		case pgRangeVarType:
			if rangeVar, ok := reflect.ValueOf(node).Interface().(*pg_query.RangeVar); ok {
				tables = append(tables, queryProto.Table{
					Db:    rangeVar.Schemaname,
					Table: rangeVar.Relname,
				})
			}
		default:
			for i := 0; i < t.NumField(); i++ {
				fieldVal := v.Field(i)
				fieldType := t.Field(i)
				if !fieldType.IsExported() {
					continue
				}

				ts, ets, ps := walkQueryNode(fieldVal.Interface())
				if strings.Contains(string(fieldType.Tag), ",name=where_clause,") {
					extraTables = append(extraTables, ts...)
				} else {
					tables = append(tables, ts...)
				}
				extraTables = append(extraTables, ets...)
				procedures = append(procedures, ps...)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			ts, ets, ps := walkQueryNode(v.Index(i).Interface())
			tables = append(tables, ts...)
			extraTables = append(extraTables, ets...)
			procedures = append(procedures, ps...)
		}
	}

	return
}
