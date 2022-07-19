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

package rdsslowlog

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shatteredsilicon/qan-agent/agent"
	"github.com/shatteredsilicon/qan-agent/agent/release"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/ssm/proto"
	pc "github.com/shatteredsilicon/ssm/proto/config"
)

const rdsServiceAPIURI = "/v0/rds/detail"

var timeoutClientConfig = pct.TimeoutClientConfig{
	ConnectTimeout:   10 * time.Second,
	ReadWriteTimeout: 10 * time.Second,
}

// RDSServiceDetail is the response structure from rds service detail api of ssm-managed
type RDSServiceDetail struct {
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	Region             string `json:"region"`
	Instance           string `json:"instance"`
}

// GetRDSServiceDetail fetches rds service detail data from ssm-managed
func GetRDSServiceDetail(cfg agent.AgentConfig, qanCfg pc.QAN) (*RDSServiceDetail, error) {
	schema := "http"
	if cfg.ServerSSL || cfg.ServerInsecureSSL {
		schema = "https"
	}
	u := url.URL{
		Scheme: schema,
		Host:   cfg.ApiHostname,
		Path:   path.Join(cfg.ManagedAPIPath, rdsServiceAPIURI),
	}
	if cfg.ServerPassword != "" {
		u.User = url.UserPassword(cfg.ServerUser, cfg.ServerPassword)
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("qan_db_instance_uuid", qanCfg.UUID)
	req.URL.RawQuery = q.Encode()

	req.Header.Add("X-Percona-Agent-Version", release.VERSION)
	req.Header.Add("X-Percona-Protocol-Version", proto.VERSION)

	client := &http.Client{
		Transport: &http.Transport{
			Dial:            pct.TimeoutDialer(&timeoutClientConfig),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s error: client.Do: %s", u.String(), err)
	}
	defer resp.Body.Close()

	var data []byte
	if resp.Header.Get("Content-Type") == "application/x-gzip" {
		buf := new(bytes.Buffer)
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(buf, gz); err != nil {
			return nil, err
		}
		data = buf.Bytes()
	} else {
		data, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("GET %s error: ioutil.ReadAll: %s", u.String(), err)
		}
	}

	var detail RDSServiceDetail
	if err = json.Unmarshal(data, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}
