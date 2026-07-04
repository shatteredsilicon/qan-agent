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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/shatteredsilicon/ssm/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ExampleQuery is a subset of SystemProfile
type ExampleQuery struct {
	Ns                 string `bson:"ns" json:"ns"`
	Op                 string `bson:"op" json:"op"`
	Query              bson.D `bson:"query,omitempty" json:"query,omitempty"`
	Command            bson.D `bson:"command,omitempty" json:"command,omitempty"`
	OriginatingCommand bson.D `bson:"originatingCommand,omitempty" json:"originatingCommand,omitempty"`
	UpdateObj          bson.D `bson:"updateobj,omitempty" json:"updateobj,omitempty"`
}

func (self ExampleQuery) Db() string {
	ns := strings.SplitN(self.Ns, ".", 2)
	if len(ns) > 0 {
		return ns[0]
	}
	return ""
}

// ExplainCmd returns bson.D ready to use in https://godoc.org/labix.org/v2/mgo#Database.Run
func (self ExampleQuery) ExplainCmd() bson.D {
	cmd := self.Command

	switch self.Op {
	case "query":
		if len(cmd) == 0 {
			cmd = self.Query
		}

		// MongoDB 2.6:
		//
		// "query" : {
		//   "query" : {
		//
		//   },
		//	 "$explain" : true
		// },
		if _, ok := cmd.Map()["$explain"]; ok {
			cmd = bson.D{
				{"explain", ""},
			}
			break
		}

		if len(cmd) == 0 || cmd[0].Key != "find" {
			var filter interface{}
			if len(cmd) > 0 && cmd[0].Key == "query" {
				filter = cmd[0].Value
			} else {
				filter = cmd
			}

			coll := ""
			s := strings.SplitN(self.Ns, ".", 2)
			if len(s) == 2 {
				coll = s[1]
			}

			cmd = bson.D{
				{"find", coll},
				{"filter", filter},
			}
		} else {
			for i := 0; i < len(cmd); i++ {
				switch cmd[i].Key {
				// PMM-1905: Drop "ntoreturn" if it's negative.
				case "ntoreturn":
					// If it's non-negative, then we are fine, continue to next param.
					if cmd[i].Value.(int64) >= 0 {
						continue
					}
					fallthrough
				// Drop $db as it is not supported in MongoDB 3.0.
				case "$db":
					if len(cmd)-1 == i {
						cmd = cmd[:i]
					} else {
						cmd = append(cmd[:i], cmd[i+1:]...)
					}
				}
			}
		}
	case "update":
		s := strings.SplitN(self.Ns, ".", 2)
		coll := ""
		if len(s) == 2 {
			coll = s[1]
		}
		if len(cmd) == 0 {
			cmd = bson.D{
				{Key: "q", Value: self.Query},
				{Key: "u", Value: self.UpdateObj},
			}
		}
		cmd = bson.D{
			{Key: "update", Value: coll},
			{Key: "updates", Value: []interface{}{cmd}},
		}
	case "remove":
		s := strings.SplitN(self.Ns, ".", 2)
		coll := ""
		if len(s) == 2 {
			coll = s[1]
		}
		if len(cmd) == 0 {
			cmd = bson.D{
				{Key: "q", Value: self.Query},
				// we can't determine if limit was 1 or 0 so we assume 0
				{Key: "limit", Value: 0},
			}
		}
		cmd = bson.D{
			{Key: "delete", Value: coll},
			{Key: "deletes", Value: []interface{}{cmd}},
		}
	case "insert":
		if len(cmd) == 0 {
			cmd = self.Query
		}
		if len(cmd) == 0 || cmd[0].Key != "insert" {
			coll := ""
			s := strings.SplitN(self.Ns, ".", 2)
			if len(s) == 2 {
				coll = s[1]
			}

			cmd = bson.D{
				{"insert", coll},
			}
		}
	case "getmore":
		if len(self.OriginatingCommand) > 0 {
			cmd = self.OriginatingCommand
			for i := range cmd {
				// drop $db param as it is not supported in MongoDB 3.0
				if cmd[i].Key == "$db" {
					if len(cmd)-1 == i {
						cmd = cmd[:i]
					} else {
						cmd = append(cmd[:i], cmd[i+1:]...)
					}
					break
				}
			}
		} else {
			cmd = bson.D{
				{Key: "getmore", Value: ""},
			}
		}
	case "command":
		cmd = sanitizeCommand(cmd)

		if len(cmd) == 0 || cmd[0].Key != "group" {
			break
		}

		if group, ok := cmd[0].Value.(bson.D); ok {
			for i := range group {
				// for MongoDB <= 3.2
				// "$reduce" : function () {}
				// It is then Unmarshaled as empty value, so in essence not working
				//
				// for MongoDB >= 3.4
				// "$reduce" : {
				//    "code" : "function () {}"
				// }
				// It is then properly Unmarshaled but then explain fails with "not code"
				//
				// The $reduce function shouldn't affect explain execution plan (e.g. what indexes are picked)
				// so we ignore it for now until we find better way to handle this issue
				if group[i].Key == "$reduce" {
					group[i].Value = "{}"
					cmd[0].Value = group
					break
				}
			}
		}
	}

	return bson.D{
		{
			Key:   "explain",
			Value: cmd,
		},
	}
}

func sanitizeCommand(cmd bson.D) bson.D {
	if len(cmd) < 1 {
		return cmd
	}

	key := cmd[0].Key
	if key != "count" && key != "distinct" {
		return cmd
	}

	for i := range cmd {
		// drop $db param as it is not supported in MongoDB 3.0
		if cmd[i].Key == "$db" {
			if len(cmd)-1 == i {
				cmd = cmd[:i]
			} else {
				cmd = append(cmd[:i], cmd[i+1:]...)
			}
			break
		}
	}

	return cmd
}

type BsonD bson.D

func (d *BsonD) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('{') {
		return fmt.Errorf("expected { but got %s", t)
	}
	for {
		t, err := dec.Token()
		if err != nil {
			return err
		}

		// Might be empty object
		if t == json.Delim('}') {
			return nil
		}

		key, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected key to be a string but got %s", t)
		}

		de := primitive.E{}
		de.Key = key

		if !dec.More() {
			return fmt.Errorf("missing value for key %s", key)
		}

		var raw json.RawMessage
		err = dec.Decode(&raw)
		if err != nil {
			return err
		}

		var v BsonD
		err = bson.UnmarshalExtJSON(raw, true, &v)
		if err != nil {
			var v []BsonD
			err = bson.UnmarshalExtJSON(raw, true, &v)
			if err != nil {
				var v interface{}
				err = bson.UnmarshalExtJSON(raw, true, &v)
				if err != nil {
					return err
				} else {
					de.Value = v
				}
			} else {
				de.Value = v
			}
		} else {
			de.Value = v
		}

		*d = append(*d, de)
		if !dec.More() {
			break
		}
	}

	t, err = dec.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('}') {
		return fmt.Errorf("expect delimeter %s but got %s", json.Delim('}'), t)
	}

	return nil
}

func (d BsonD) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer

	b.WriteByte('{')

	for i, v := range d {
		if i > 0 {
			b.WriteByte(',')
		}

		// marshal key
		key, err := bson.MarshalExtJSON(v.Key, false, true)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')

		var val []byte
		if value, ok := v.Value.(float64); ok && math.IsInf(value, 0) {
			if math.IsInf(value, 1) {
				val = []byte("Infinity")
			} else {
				val = []byte("-Infinity")
			}

			// below is wrong, but I'm later unable to Unmarshal Infinity,
			// so we turn it into string for now
			val = append([]byte(`"`), val...)
			val = append(val, '"')
		} else {
			// marshal value
			val, err = bson.MarshalExtJSON(v.Value, false, true)
			if err != nil {
				return nil, err
			}
		}
		b.Write(val)
	}

	b.WriteByte('}')

	return b.Bytes(), nil
}

func Explain(client *mongo.Client, db, query string) (*proto.ExplainResult, error) {
	var eq ExampleQuery

	err := bson.UnmarshalExtJSON([]byte(query), true, &eq)
	if err != nil {
		return nil, fmt.Errorf("explain: unable to decode query %s: %s", string(query), err)
	}

	if db == "" {
		db = eq.Db()
	}

	var result BsonD
	res := client.Database(db).RunCommand(context.TODO(), eq.ExplainCmd())
	if res.Err() != nil {
		return nil, res.Err()
	}

	if err := res.Decode(&result); err != nil {
		return nil, err
	}

	resultJSON, err := bson.MarshalExtJSON(result, true, true)
	if err != nil {
		return nil, fmt.Errorf("explain: unable to encode explain result of %s: %s", string(query), err)
	}

	explainResult := &proto.ExplainResult{
		JSON: string(resultJSON),
	}
	return explainResult, nil
}
