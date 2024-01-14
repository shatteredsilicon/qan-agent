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

package summary

import (
	"github.com/shatteredsilicon/qan-agent/pct/cmd"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Summary executes `pt-mongodb-summary` for given dsn
func Summary(dsn string) (string, error) {
	opts := options.Client().ApplyURI(dsn)
	if err := opts.Validate(); err != nil {
		return "", err
	}

	name := "pt-mongodb-summary"

	args := []string{}
	// add username, password and auth database e.g. `pt-mongodb-summary -u admin -p admin -a admin`
	args = append(args, authArgs(opts.Auth.Username, opts.Auth.Password, opts.Auth.AuthSource)...)
	// add host[:port] e.g. `pt-mongodb-summary localhost:27017`
	args = append(args, addrArgs(opts.Hosts)...)

	return cmd.NewRealCmd(name, args...).Run()
}

// authArgs returns authentication arguments for cmd
func authArgs(username, password, authDatabase string) (args []string) {
	if username != "" {
		args = append(args, "--username="+username)
	}
	if password != "" {
		args = append(args, "--password="+password)
	}
	if authDatabase != "" {
		args = append(args, "--authenticationDatabase="+password)
	}

	return args
}

// addrArgs returns host[:port] arguments for cmd
func addrArgs(addrs []string) (args []string) {
	if len(addrs) == 0 {
		return nil
	}

	args = append(args, addrs[0])

	return args
}
