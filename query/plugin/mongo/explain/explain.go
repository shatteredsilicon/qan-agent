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

package explain

import (
	"context"
	"time"

	"github.com/percona/percona-toolkit/src/go/mongolib/explain"
	"github.com/shatteredsilicon/ssm/proto"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	MgoTimeoutDialInfo      = 5 * time.Second
	MgoTimeoutSessionSync   = 5 * time.Second
	MgoTimeoutSessionSocket = 5 * time.Second
)

func Explain(dsn, db, query string) (*proto.ExplainResult, error) {
	// if dsn is incorrect we should exit immediately as this is not gonna correct itself
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI(dsn)
	if err := mongoOpts.Validate(); err != nil {
		return nil, err
	}
	mongoOpts.SetServerAPIOptions(serverAPI).
		SetConnectTimeout(MgoTimeoutDialInfo).
		SetSocketTimeout(MgoTimeoutSessionSocket).
		SetTimeout(MgoTimeoutSessionSync).
		SetReadPreference(readpref.Nearest())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, err
	}

	ex := explain.New(context.Background(), client)
	resultJson, err := ex.Run(db, []byte(query))
	if err != nil {
		return nil, err
	}

	explainResult := &proto.ExplainResult{
		JSON: string(resultJson),
	}
	return explainResult, nil
}
