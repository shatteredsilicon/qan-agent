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

package factory

import (
	"context"
	"fmt"

	"github.com/shatteredsilicon/ssm/proto"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/instance"
	"github.com/shatteredsilicon/qan-agent/mrms"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	mongoAnalyzer "github.com/shatteredsilicon/qan-agent/qan/analyzer/mongo"
	mysqlAnalyzer "github.com/shatteredsilicon/qan-agent/qan/analyzer/mysql"
	"github.com/shatteredsilicon/qan-agent/ticker"
)

const pkg = "factory"

type UnknownTypeError string

func (u UnknownTypeError) Error() string {
	return fmt.Sprintf("%s: unknown type %s", pkg, u)
}

type Factory struct {
	logChan      chan proto.LogEntry
	spool        data.Spooler
	clock        ticker.Manager
	mrms         mrms.Monitor
	instanceRepo *instance.Repo
}

func New(
	logChan chan proto.LogEntry,
	spool data.Spooler,
	clock ticker.Manager,
	mrms mrms.Monitor,
	instanceRepo *instance.Repo,
) *Factory {
	f := &Factory{
		logChan:      logChan,
		spool:        spool,
		clock:        clock,
		mrms:         mrms,
		instanceRepo: instanceRepo,
	}
	return f
}

func (f *Factory) Make(analyzerType, analyzerName string, protoInstance proto.Instance) (analyzer.Analyzer, error) {
	logger := pct.NewLogger(f.logChan, analyzerName)

	// Expose some global services to plugins
	ctx := context.Background()
	ctx = context.WithValue(ctx, "services", map[string]interface{}{
		"logger": logger,
		"spool":  f.spool,
		"clock":  f.clock,
		"mrms":   f.mrms,
	})

	// In the future we can use here plugin approach
	// https://golang.org/pkg/plugin/
	// for now switch is gonna be enough
	switch analyzerType {
	case "mongo":
		return mongoAnalyzer.New(ctx, protoInstance), nil
	case "mysql":
		return mysqlAnalyzer.New(ctx, protoInstance), nil
	}

	return nil, UnknownTypeError(analyzerType)
}
