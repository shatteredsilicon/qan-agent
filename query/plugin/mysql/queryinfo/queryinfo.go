package queryinfo

import (
	"database/sql"
	"fmt"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/tableinfo"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mysql/util"
	"github.com/shatteredsilicon/ssm/proto"
)

func QueryInfo(c mysql.Connector, param *proto.QueryInfoParam) (proto.QueryInfoResult, error) {
	res := make(proto.QueryInfoResult)

	if len(param.Table) > 0 {
		tableRes, err := tableinfo.TableInfo(c, &proto.TableInfoQuery{
			UUID:   param.UUID,
			Create: param.Table,
			Index:  param.Index,
			Status: param.Status,
		})
		if err != nil {
			return res, err
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
		for _, p := range param.Procedure {
			queryInfo, ok := res[p.String()]
			if !ok {
				res[p.String()] = &proto.QueryInfo{}
				queryInfo = res[p.String()]
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

	return res, nil
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
