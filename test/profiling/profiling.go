/*
   Copyright (c) 2017, Percona LLC and/or its affiliates. All rights reserved.

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

package profiling

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type run func(client *mongo.Client) error
type runDB func(client *mongo.Client, dbname string) error

type Profiling struct {
	url    string
	client *mongo.Client
	err    error
}

func New(url string) *Profiling {
	p := &Profiling{
		url: url,
	}
	p.client, p.err = createClient(url)
	return p
}

func (p *Profiling) Enable(dbname string) error {
	return p.Run(func(client *mongo.Client) error {
		return profile(client.Database(dbname), 2)
	})
}

func (p *Profiling) Disable(dbname string) error {
	return p.Run(func(client *mongo.Client) error {
		return profile(client.Database(dbname), 0)
	})
}

func (p *Profiling) Drop(dbname string) error {
	return p.Run(func(client *mongo.Client) error {
		if !p.Exist(dbname) {
			return nil
		}
		return client.Database(dbname).Collection("system.profile").Drop(context.Background())
	})
}

func (p *Profiling) Exist(dbnameToLook string) bool {
	found := fmt.Errorf("found db: %s", dbnameToLook)
	err := p.RunDB(func(client *mongo.Client, dbname string) error {
		if dbnameToLook == dbname {
			return found
		}
		return nil
	})

	return err == found
}

func (p *Profiling) Reset(dbname string) error {
	err := p.Disable(dbname)
	if err != nil {
		return err
	}
	err = p.Drop(dbname)
	if err != nil {
		return err
	}
	err = p.Enable(dbname)
	if err != nil {
		return err
	}
	return nil
}

func (p *Profiling) EnableAll() error {
	return p.RunDB(func(client *mongo.Client, dbname string) error {
		return p.Enable(dbname)
	})
}

func (p *Profiling) DisableAll() error {
	return p.RunDB(func(client *mongo.Client, dbname string) error {
		return p.Disable(dbname)
	})
}

func (p *Profiling) DropAll() error {
	return p.RunDB(func(client *mongo.Client, dbname string) error {
		return p.Drop(dbname)
	})
}

func (p *Profiling) ResetAll() error {
	err := p.DisableAll()
	if err != nil {
		return err
	}
	p.DropAll()
	err = p.EnableAll()
	if err != nil {
		return err
	}
	return nil
}

func (p *Profiling) Run(f run) error {
	if p.err != nil {
		return p.err
	}

	return f(p.client)
}

func (p *Profiling) RunDB(f runDB) error {
	return p.Run(func(client *mongo.Client) error {
		databases, err := client.ListDatabaseNames(context.Background(), bson.D{})
		if err != nil {
			return err
		}
		for _, dbname := range databases {
			err := f(client, dbname)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// DatabaseNames returns the names of non-empty databases present in the cluster.
func (p *Profiling) DatabaseNames() ([]string, error) {
	return p.client.ListDatabaseNames(context.Background(), bson.D{})
}

func profile(db *mongo.Database, v int) error {
	result := struct {
		Was       int
		Slowms    int
		Ratelimit int
	}{}
	return db.RunCommand(context.Background(), bson.D{{"profile", v}}).Decode(&result)
}

func createClient(url string) (*mongo.Client, error) {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI(url)
	if err := mongoOpts.Validate(); err != nil {
		return nil, err
	}
	mongoOpts.SetServerAPIOptions(serverAPI).SetReadPreference(readpref.Nearest())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, err
	}

	return client, nil
}
