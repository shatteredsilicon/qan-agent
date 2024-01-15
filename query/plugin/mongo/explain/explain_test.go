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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestExplain(t *testing.T) {
	t.Parallel()

	var err error

	dsn := "127.0.0.1:27017"

	db := "test"
	query := `{"ns":"test.col1","op":"query","query":{"find":"col1","filter":{"name":"Alicja"}}}`

	explainResult, err := Explain(dsn, db, query)
	require.NoError(t, err)

	got := bson.M{}
	err = bson.UnmarshalExtJSON([]byte(explainResult.JSON), true, &got)
	require.NoError(t, err)

	// check structure of the result
	assert.NotEmpty(t, got["executionStats"])
	assert.NotEmpty(t, got["queryPlanner"])
	assert.NotEmpty(t, got["serverInfo"])
	assert.NotEmpty(t, got["ok"])
	assert.Len(t, got, 4)
}

func TestExplainDecodeQueryError(t *testing.T) {
	t.Parallel()

	var err error

	dsn := "127.0.0.1:27017"

	db := "test"
	query := `{Jas`

	explainResult, err := Explain(dsn, db, query)
	assert.Nil(t, explainResult)
	assert.Error(t, err)
	assert.Equal(t, "explain: unable to decode query {Jas: unexpected EOF", err.Error())
}
