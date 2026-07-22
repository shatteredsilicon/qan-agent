package explain

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/ssm/proto"
)

var (
	unsupportedRegex = regexp.MustCompile(`^(?i)\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|GRANT|REVOKE|VACUUM|ANALYZE|EXPLAIN)`)
)

type ExplainQuery struct {
	proto.ExplainQuery
	GuessedSchemas map[string]string
}

type ExplainResult struct {
	proto.ExplainResult
	IsSchemaGuessed bool
}

func Explain(db *sql.DB, query string, guessedSchemas map[string]string) (*ExplainResult, error) {
	if unsupportedRegex.Match([]byte(query)) {
		return nil, nil
	}

	explainResult, err := explain(db, query, guessedSchemas)
	if err != nil {
		return nil, err
	}
	return explainResult, nil
}

// --------------------------------------------------------------------------

func explain(db *sql.DB, query string, guessedSchemas map[string]string) (*ExplainResult, error) {
	explain := &ExplainResult{}
	var originErr error
	explain.TEXT, explain.JSON, originErr = realExplain(db, query)
	if originErr == nil {
		return explain, nil
	}

	// First try failed, see if this is a query that we can
	// guess the schemas to make EXPLAIN works
	s, err := pg_query.Parse(query)
	if err != nil {
		return nil, originErr
	}

	parseNodes := make([]*pg_query.Node, 0)
	for i := range s.Stmts {
		parseNodes = append(parseNodes, s.Stmts[i].Stmt)
	}

	for i := 0; i < len(parseNodes); i++ {
		if parseNodes[i] == nil {
			continue
		}

		switch s := parseNodes[i].Node.(type) {
		case *pg_query.Node_SelectStmt:
			parseNodes = append(parseNodes, s.SelectStmt.TargetList...)
			parseNodes = append(parseNodes, s.SelectStmt.FromClause...)
		case *pg_query.Node_InsertStmt:
			if s.InsertStmt.Relation != nil {
				if s.InsertStmt.Relation.Schemaname == "" && guessedSchemas[s.InsertStmt.Relation.Relname] != "" {
					s.InsertStmt.Relation.Schemaname = guessedSchemas[s.InsertStmt.Relation.Relname]
				}
			}
			if s.InsertStmt.SelectStmt != nil {
				parseNodes = append(parseNodes, s.InsertStmt.SelectStmt)
			}
		case *pg_query.Node_UpdateStmt:
			if s.UpdateStmt.Relation != nil {
				if s.UpdateStmt.Relation.Schemaname == "" && guessedSchemas[s.UpdateStmt.Relation.Relname] != "" {
					s.UpdateStmt.Relation.Schemaname = guessedSchemas[s.UpdateStmt.Relation.Relname]
				}
			}
			if s.UpdateStmt.WhereClause != nil {
				parseNodes = append(parseNodes, s.UpdateStmt.WhereClause)
			}
			parseNodes = append(parseNodes, s.UpdateStmt.FromClause...)
		case *pg_query.Node_DeleteStmt:
			if s.DeleteStmt.Relation != nil {
				if s.DeleteStmt.Relation.Schemaname == "" && guessedSchemas[s.DeleteStmt.Relation.Relname] != "" {
					s.DeleteStmt.Relation.Schemaname = guessedSchemas[s.DeleteStmt.Relation.Relname]
				}
			}
			if s.DeleteStmt.WhereClause != nil {
				parseNodes = append(parseNodes, s.DeleteStmt.WhereClause)
			}
		case *pg_query.Node_MergeStmt:
			if s.MergeStmt.Relation != nil {
				if s.MergeStmt.Relation.Schemaname == "" && guessedSchemas[s.MergeStmt.Relation.Relname] != "" {
					s.MergeStmt.Relation.Schemaname = guessedSchemas[s.MergeStmt.Relation.Relname]
				}
			}
			if s.MergeStmt.SourceRelation != nil {
				parseNodes = append(parseNodes, s.MergeStmt.SourceRelation)
			}
		case *pg_query.Node_CreateTableAsStmt:
			if s.CreateTableAsStmt.Query != nil {
				parseNodes = append(parseNodes, s.CreateTableAsStmt.Query)
			}
		case *pg_query.Node_TableLikeClause:
			if s.TableLikeClause.Relation.Schemaname == "" && guessedSchemas[s.TableLikeClause.Relation.Relname] != "" {
				s.TableLikeClause.Relation.Schemaname = guessedSchemas[s.TableLikeClause.Relation.Relname]
			}
		case *pg_query.Node_FromExpr:
			parseNodes = append(parseNodes, s.FromExpr.Fromlist...)
		case *pg_query.Node_FuncCall:
			parseNodes = append(parseNodes, s.FuncCall.Args...)
		case *pg_query.Node_FuncExpr:
			parseNodes = append(parseNodes, s.FuncExpr.Args...)
		case *pg_query.Node_ResTarget:
			parseNodes = append(parseNodes, s.ResTarget.Indirection...)
			if s.ResTarget.Val != nil {
				parseNodes = append(parseNodes, s.ResTarget.Val)
			}
		case *pg_query.Node_JoinExpr:
			if s.JoinExpr.Larg != nil {
				parseNodes = append(parseNodes, s.JoinExpr.Larg)
			}
			if s.JoinExpr.Rarg != nil {
				parseNodes = append(parseNodes, s.JoinExpr.Rarg)
			}
		case *pg_query.Node_RangeVar:
			if s.RangeVar.Schemaname == "" && guessedSchemas[s.RangeVar.Relname] != "" {
				s.RangeVar.Schemaname = guessedSchemas[s.RangeVar.Relname]
			}
		case *pg_query.Node_AExpr:
			if s.AExpr.Lexpr != nil {
				parseNodes = append(parseNodes, s.AExpr.Lexpr)
			}
			if s.AExpr.Rexpr != nil {
				parseNodes = append(parseNodes, s.AExpr.Rexpr)
			}
		case *pg_query.Node_SubLink:
			if s.SubLink.Subselect != nil {
				parseNodes = append(parseNodes, s.SubLink.Subselect)
			}
		}
	}

	newQuery, _ := pg_query.Deparse(s)
	if newQuery == "" {
		return nil, originErr
	}

	explain.TEXT, explain.JSON, err = realExplain(db, newQuery)
	if err != nil {
		return nil, err
	}

	explain.IsSchemaGuessed = true
	return explain, nil
}

func realExplain(db *sql.DB, query string) (text, json string, err error) {
	text, err = textExplain(db, query)
	if err != nil {
		return "", "", err
	}

	json, err = jsonExplain(db, query)
	return
}

func textExplain(db *sql.DB, query string) (explain string, err error) {
	if strings.TrimSpace(query) == "" {
		return "", fmt.Errorf("cannot run EXPLAIN on an empty query example")
	}

	rows, err := db.Query(fmt.Sprintf("EXPLAIN (FORMAT TEXT) %s", query))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	explainRows := []string{}
	for rows.Next() {
		var row string
		err = rows.Scan(&row)
		if err != nil {
			return "", err
		}
		explainRows = append(explainRows, row)
	}

	return strings.Join(explainRows, "\n"), err
}

func jsonExplain(db *sql.DB, query string) (string, error) {
	var explain string
	err := db.QueryRow(fmt.Sprintf("EXPLAIN (VERBOSE, FORMAT JSON) %s", query)).Scan(&explain) // VERBOSE for it to puts schema names in EXPLAIN output
	if err != nil {
		return "", err
	}

	return explain, nil
}
