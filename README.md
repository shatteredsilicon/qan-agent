# Shattered Silicon Query Analytics Agent

[![GoDoc](https://godoc.org/github.com/shatteredsilicon/qan-agent?status.svg)](https://godoc.org/github.com/shatteredsilicon/qan-agent)
[![Report Card](http://goreportcard.com/badge/github.com/shatteredsilicon/qan-agent)](http://goreportcard.com/report/github.com/shatteredsilicon/qan-agent)

Shattered Silicon Query Analytics (QAN) Agent is part of Shattered Silicon Monitoring (SSM).
See the [SSM docs](https://shatteredsilicon.net/software/ssm/documentation/latest/) for more information.


## Building

1. Setup [`GOPATH`](https://golang.org/doc/code.html#GOPATH).
1. Clone repository to `GOPATH`: `go get -v github.com/shatteredsilicon/qan-agent`.
1. Install dependency management tool [`dep`](https://github.com/golang/dep#installation)
1. Fetch dependencies: `dep ensure -v`.
1. Install agent and installer: `go install -v github.com/shatteredsilicon/qan-agent/bin/...`. Binaries will be created in `$GOPATH/bin`.
