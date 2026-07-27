/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package mongo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/shatteredsilicon/qan-agent/instance"
	"github.com/shatteredsilicon/qan-agent/query/plugin"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mongo/explain"
	"github.com/shatteredsilicon/qan-agent/query/plugin/mongo/summary"
	"github.com/shatteredsilicon/ssm/proto"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

const (
	MgoTimeoutDialInfo      = 5 * time.Second
	MgoTimeoutSessionSync   = 5 * time.Second
	MgoTimeoutSessionSocket = 5 * time.Second
)

// verify, at compile time, if main struct implements plugin interface
var _ plugin.Plugin = (*Mongo)(nil)

var (
	// available cmds
	cmds = map[string]execFunc{
		"Explain": execExplain,
		"Summary": execSummary,
	}
)

// Mongo handles cmds related to given instance
type Mongo struct {
	cmds map[string]execFunc
}

// New returns configured pointer *Mongo
func New() *Mongo {
	return &Mongo{
		cmds: cmds,
	}
}

// Handle executes cmd for given instance and returns resulting data
func (m *Mongo) Handle(cmd *proto.Cmd, in instance.Instance) (interface{}, error) {
	c, ok := m.cmds[cmd.Cmd]
	if !ok {
		return nil, plugin.UnknownCmdError(cmd.Cmd)
	}

	return c(cmd, in.Instance)
}

type execFunc func(cmd *proto.Cmd, in proto.Instance) (interface{}, error)

func execExplain(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	q := &proto.ExplainQuery{}
	if err := json.Unmarshal(cmd.Data, q); err != nil {
		return nil, err
	}

	mongoOpts, err := MongoClientOpts(in.DSN)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, err
	}

	return explain.Explain(client, q.Db, q.Query)
}

func execSummary(cmd *proto.Cmd, in proto.Instance) (interface{}, error) {
	return summary.Summary(FixDSN(in.DSN))
}

// FixDSN adds default 'mongodb://' scheme to dsn
// if it doesn't have a scheme
func FixDSN(dsn string) string {
	if _, err := connstring.ParseAndValidate(dsn); err != nil {
		// assume it's invalid because it doesn't have schema,
		// add default schema 'mongodb://' and try it again
		tmpDSN := "mongodb://" + dsn
		_, err = connstring.ParseAndValidate(tmpDSN)
		if err == nil {
			dsn = tmpDSN
		}
	}

	return dsn
}

func MongoClientOpts(dsn string) (*options.ClientOptions, error) {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI(FixDSN(dsn)).SetServerAPIOptions(serverAPI)
	if mongoOpts.Direct == nil {
		// default to directConnection=true if it's not set
		mongoOpts.SetDirect(true)
	}
	if mongoOpts.ConnectTimeout == nil {
		mongoOpts.SetConnectTimeout(MgoTimeoutDialInfo)
	}
	if mongoOpts.SocketTimeout == nil {
		mongoOpts.SetSocketTimeout(MgoTimeoutSessionSocket)
	}
	if mongoOpts.Timeout == nil {
		mongoOpts.SetTimeout(MgoTimeoutSessionSync)
	}
	if mongoOpts.ReadPreference == nil {
		mongoOpts.SetReadPreference(readpref.Nearest())
	}

	if err := mongoOpts.Validate(); err != nil {
		return nil, err
	}

	return mongoOpts, nil
}
