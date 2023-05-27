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

package util

import (
	"testing"

	pc "github.com/shatteredsilicon/ssm/proto/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlowLogMySQLBasic(t *testing.T) {
	on, off, err := GetMySQLConfig(pc.QAN{CollectFrom: "slowlog"})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"SET GLOBAL slow_query_log=OFF",
		"SET GLOBAL log_output='file'",
		"SET GLOBAL slow_query_log=ON",
		"SET time_zone='+0:00'",
	}, on)
	assert.Equal(t, []string{
		"SET GLOBAL slow_query_log=OFF",
	}, off)
}

func TestSplitSlowLog(t *testing.T) {
	splitLogs := []struct {
		Log           []byte
		CompleteLog   []byte
		IncompleteLog []byte
	}{
		{
			Log: []byte(`/rdsdbbin/mysql/bin/mysqld, Version: 8.0.32 (Source distribution). started with:
Tcp port: 3306  Unix socket: /tmp/mysql.sock
Time                 Id Command    Argument
# Time: 2023-05-12T08:00:06.278162Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.002968  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0
SET timestamp=1683878406;
FLUSH SLOW LOGS;
# Time: 2023-05-12T08:00:06.279095Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.000077  Lock_time:`),
			CompleteLog: []byte(`/rdsdbbin/mysql/bin/mysqld, Version: 8.0.32 (Source distribution). started with:
Tcp port: 3306  Unix socket: /tmp/mysql.sock
Time                 Id Command    Argument
# Time: 2023-05-12T08:00:06.278162Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.002968  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0
SET timestamp=1683878406;
FLUSH SLOW LOGS;
`),
			IncompleteLog: []byte(`# Time: 2023-05-12T08:00:06.279095Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.000077  Lock_time:`),
		},
		{
			Log: []byte(`# Time: 2023-05-12T08:00:06.279095Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.000077  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1683878406;
SELECT 1;`),
			CompleteLog: []byte{},
			IncompleteLog: []byte(`# Time: 2023-05-12T08:00:06.279095Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     7
# Query_time: 0.000077  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1683878406;
SELECT 1;`),
		},
	}

	for _, splitLog := range splitLogs {
		completeLog, incompleteLog := SplitSlowLog(splitLog.Log)
		assert.Equal(t, string(splitLog.CompleteLog), string(completeLog))
		assert.Equal(t, string(splitLog.IncompleteLog), string(incompleteLog))
	}
}
