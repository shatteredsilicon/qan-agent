module github.com/shatteredsilicon/qan-agent

go 1.19

require (
	github.com/Masterminds/semver v1.4.2
	github.com/aws/aws-sdk-go v1.44.192
	github.com/fatih/structs v1.0.0
	github.com/go-sql-driver/mysql v1.7.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-version v1.6.0
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d
	github.com/percona/go-mysql v0.0.0-20210708085315-3e7f9d34c354
	github.com/percona/percona-toolkit v0.0.0-20180420142903-765975833f0a
	github.com/percona/pmgo v0.0.0-20171205120904-497d06e28f91
	github.com/peterbourgon/diskv v2.0.1+incompatible
	github.com/pkg/errors v0.9.1
	github.com/shatteredsilicon/ssm v0.0.0-20230919190147-2b2ce9b957bb
	github.com/stretchr/testify v1.8.1
	go4.org v0.0.0-20180417224846-9599cf28b011
	golang.org/x/net v0.7.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/mgo.v2 v2.0.0-20160818020120-3f83fa500528
	vitess.io/vitess v0.15.4
)

require (
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.42.0 // indirect
	github.com/DataDog/datadog-agent/pkg/remoteconfig/state v0.42.0 // indirect
	github.com/DataDog/datadog-go/v5 v5.2.0 // indirect
	github.com/DataDog/go-tuf v0.3.0--fix-localmeta-fork // indirect
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bradfitz/slice v0.0.0-20160823171949-d9036e2120b5 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/montanaflynn/stats v0.7.0 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.39.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.4.0 // indirect
	github.com/shirou/gopsutil v3.20.12+incompatible // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go4.org/intern v0.0.0-20220617035311-6925f38cc365 // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20220617031537-928513b29760 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.5.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/genproto v0.0.0-20230131230820-1c016267d619 // indirect
	google.golang.org/grpc v1.52.3 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/DataDog/dd-trace-go.v1 v1.47.0 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	inet.af/netaddr v0.0.0-20220811202034-502d2d690317 // indirect
	k8s.io/apimachinery v0.20.6 // indirect
)

replace github.com/go-sql-driver/mysql => github.com/shatteredsilicon/go-sql-driver-mysql v0.0.0-20231031081844-ca087917bf67
