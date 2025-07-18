package queryinfo

import (
	"database/sql"

	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
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
	Info map[string]*QueryInfo
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

	return &QueryInfoResult{
		Info: res,
	}, nil
}
