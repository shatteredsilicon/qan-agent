package queryinfo

import (
	"database/sql"
	"encoding/json"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/util"
	"github.com/shatteredsilicon/ssm/proto"
)

// QueryInfo represents a TABLE/PROCEDURE/VIEW
// structure of QueryInfo cmd api
type QueryInfo struct {
	Type   proto.DBObjectType
	Create string                            `json:",omitempty"`
	Index  map[string]*tableinfo.IndexStatus `json:",omitempty"`
	Status *tableinfo.TableStatus            `json:",omitempty"`
	Errors []string                          `json:",omitempty"`
}

// QueryInfoResult represents the response
// of QueryInfo cmd api
type QueryInfoResult struct {
	Info        map[string]*QueryInfo
	SkipExplain bool
}

func GetQueryInfo(db *sql.DB, param *proto.QueryInfoParam) (*QueryInfoResult, error) {
	res := make(map[string]*QueryInfo)

	if len(param.Table) > 0 {
		tableRes, err := tableinfo.GetTableInfo(db, &proto.TableInfoQuery{
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
				Type:   v.Type,
				Create: v.Create,
				Index:  v.Index,
				Status: v.Status,
				Errors: v.Errors,
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
