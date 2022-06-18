package sender

import (
	"reflect"
	"testing"

	"github.com/shatteredsilicon/ssm/proto"
	"github.com/shatteredsilicon/ssm/proto/qan"
	"github.com/stretchr/testify/require"

	"github.com/shatteredsilicon/qan-agent/data"
	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/test/mock"
)

func TestNew(t *testing.T) {
	reportChan := make(chan *qan.Report)
	dataChan := make(chan interface{})
	spool := mock.NewSpooler(dataChan)
	logChan := make(chan proto.LogEntry)
	logger := pct.NewLogger(logChan, "test")
	sender1 := New(reportChan, spool, logger)

	type args struct {
		reportChan <-chan *qan.Report
		spool      data.Spooler
		logger     *pct.Logger
	}
	tests := []struct {
		name string
		args args
		want *Sender
	}{
		{
			name: "TestNew",
			args: args{
				reportChan: reportChan,
				spool:      spool,
				logger:     logger,
			},
			want: sender1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.reportChan, tt.args.spool, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New(%v, %v, %v) = %v, want %v", tt.args.reportChan, tt.args.spool, tt.args.logger, got, tt.want)
			}
		})
	}
}

func TestSender_Start(t *testing.T) {
	reportChan := make(chan *qan.Report)
	dataChan := make(chan interface{})
	spool := mock.NewSpooler(dataChan)
	logChan := make(chan proto.LogEntry)
	logger := pct.NewLogger(logChan, "test")
	sender1 := New(reportChan, spool, logger)

	// start sender
	err := sender1.Start()
	require.NoError(t, err)

	// running multiple Start() should be idempotent
	err = sender1.Start()
	require.NoError(t, err)

	// running multiple Stop() should be idempotent
	sender1.Stop()
	sender1.Stop()
}
