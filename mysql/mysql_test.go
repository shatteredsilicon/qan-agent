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

package mysql_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/shatteredsilicon/qan-agent/mysql"
	"github.com/stretchr/testify/require"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MysqlTestSuite struct {
}

var _ = Suite(&MysqlTestSuite{})

var dsn = os.Getenv("PCT_TEST_MYSQL_DSN")

func (s *MysqlTestSuite) SetUpSuite(t *C) {
	require.NotEmpty(t, dsn, "PCT_TEST_MYSQL_DSN is not set")
}

func (s *MysqlTestSuite) TestConnection(t *C) {
	// Only 1 physical connection should be allowed. So calling Connect twice
	// should yield the same underlying sql.DB. Also, Connect and Close should
	// be idempotent.
	conn := mysql.NewConnection(dsn)
	err := conn.Connect()
	t.Assert(err, IsNil)

	db1 := conn.DB()

	err = conn.Connect()
	t.Assert(err, IsNil)

	db2 := conn.DB()
	t.Check(db1, Equals, db2)

	conn.Close()
	t.Check(conn.DB(), IsNil)
	conn.Close()
	t.Check(conn.DB(), IsNil)
}

func (s *MysqlTestSuite) TestMissingSocketError(t *C) {
	// https://jira.percona.com/browse/PCT-791
	conn := mysql.NewConnection("percona:percona@unix(/foo/bar/my.sock)/")
	err := conn.Connect()
	t.Check(
		fmt.Sprintf("%s", err),
		Equals,
		"Cannot connect to MySQL percona:***@unix(/foo/bar/my.sock): connect: no such file or directory: /foo/bar/my.sock",
	)
}

func (s *MysqlTestSuite) TestGetGlobalInteger(t *C) {
	conn := mysql.NewConnection(dsn)
	err := conn.Connect()
	t.Assert(err, IsNil)
	defer conn.Close()

	// set variable to be sure defaults don't affect us
	err = conn.Set([]mysql.Query{
		{
			Set: "SET GLOBAL connect_timeout=3",
		},
	})
	t.Check(err, IsNil)

	globalVarInteger, err := conn.GetGlobalVarInteger("connect_timeout")
	t.Check(err, IsNil)
	t.Check(globalVarInteger.Int64, Equals, int64(3))
}

func (s *MysqlTestSuite) TestGetGlobalIntegerWhenNotConnected(t *C) {
	conn := mysql.NewConnection("percona:percona@unix(/foo/bar/my.sock)/")
	err := conn.Connect()
	defer conn.Close()
	t.Check(
		fmt.Sprintf("%s", err),
		Equals,
		"Cannot connect to MySQL percona:***@unix(/foo/bar/my.sock): connect: no such file or directory: /foo/bar/my.sock",
	)
	globalVarNumber, err := conn.GetGlobalVarInteger("connect_timeout")
	t.Check(
		fmt.Sprintf("%s", err),
		Equals,
		"not connected",
	)
	t.Check(globalVarNumber.Int64, Equals, int64(0))

}

func (s *MysqlTestSuite) TestErrorFormatting(t *C) {
	// https://jira.percona.com/browse/PCT-791
	e1 := &net.OpError{
		Op:  "dial",
		Net: "unix",
		Addr: &net.UnixAddr{
			Net:  "unix",
			Name: "/var/lib/mysql.sock",
		},
		Err: fmt.Errorf("no such file or directory"),
	}
	t.Check(mysql.FormatError(e1), Equals, "no such file or directory: /var/lib/mysql.sock")

	e1 = &net.OpError{
		Op:   "dial",
		Net:  "tcp",
		Addr: &net.TCPAddr{IP: net.IP{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff, 0x7f, 0x0, 0x0, 0x1}, Port: 3306, Zone: ""},
		Err:  fmt.Errorf("connection refused"),
	}
	t.Check(mysql.FormatError(e1), Equals, "connection refused: 127.0.0.1:3306")
}
