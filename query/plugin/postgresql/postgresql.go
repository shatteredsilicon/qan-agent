package postgresql

import (
	"database/sql"
	"encoding/json"
	"net/url"

	"github.com/shatteredsilicon/qan-agent/query/plugin"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/explain"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/queryinfo"
	"github.com/shatteredsilicon/qan-agent/query/plugin/postgresql/tableinfo"
	"github.com/shatteredsilicon/ssm/proto"
)

// verify, at compile time, if main struct implements plugin interface
var _ plugin.Plugin = (*PostgreSQL)(nil)

// PostgreSQL handles cmds related to given instance
type PostgreSQL struct {
	cmds map[string]execFunc
}

// New returns configured pointer *PostgreSQL
func New() *PostgreSQL {
	m := &PostgreSQL{}
	m.cmds = map[string]execFunc{
		"Explain":   m.explain,
		"TableInfo": m.tableInfo,
		"QueryInfo": m.queryInfo,
	}

	return m
}

func (m *PostgreSQL) dbConn(dsn, database string) (*sql.DB, error) {
	dsn = FixDSN(dsn)
	if database != "" {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}
		u.Path = "/" + database
		dsn = u.String()
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// Handle executes cmd for given instance and returns resulting data
func (m *PostgreSQL) Handle(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	c, ok := m.cmds[cmd.Cmd]
	if !ok {
		return nil, plugin.UnknownCmdError(cmd.Cmd)
	}

	return c(cmd, in)
}

type execFunc func(cmd *proto.Cmd, in proto.Instance) (interface{}, error)

func (m *PostgreSQL) explain(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	q := &proto.ExplainQuery{}
	if err := json.Unmarshal(cmd.Data, q); err != nil {
		return nil, err
	}

	db, err := m.dbConn(in.DSN, q.Db)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	result, err := explain.Explain(db, q.Query, q.Convert, len(q.WithExplainRows) > 0)
	if result != nil && len(q.WithExplainRows) > 0 {
		result.Classic = q.WithExplainRows
	}
	return result, err
}

func (m *PostgreSQL) tableInfo(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	tableInfo := &tableinfo.TableInfoQuery{}
	if err := json.Unmarshal(cmd.Data, tableInfo); err != nil {
		return nil, err
	}

	db, err := m.dbConn(in.DSN, tableInfo.DB)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return tableinfo.GetTableInfo(db, tableInfo)
}

func (m *PostgreSQL) queryInfo(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	param := &queryinfo.QueryInfoParam{}
	if err := json.Unmarshal(cmd.Data, param); err != nil {
		return nil, err
	}

	db, err := m.dbConn(in.DSN, param.DB)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return queryinfo.GetQueryInfo(db, param)
}

// FixDSN adds default 'postgresql://' scheme to dsn
// if it doesn't have a scheme
func FixDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u == nil || u.Scheme == "" {
		// assume it's invalid because it doesn't have schema,
		// add default schema 'postgresql://' and try it again
		tmpDSN := "postgresql://" + dsn
		u, err = url.Parse(tmpDSN)
		if err == nil && u != nil && u.Scheme != "" {
			dsn = tmpDSN
		}
	}

	return dsn
}
