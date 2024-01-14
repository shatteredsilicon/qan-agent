package profiler

import (
	"context"
	"testing"
	"time"

	"github.com/shatteredsilicon/ssm/proto"
	"github.com/shatteredsilicon/ssm/proto/config"
	"github.com/shatteredsilicon/ssm/proto/qan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/shatteredsilicon/qan-agent/pct"
	"github.com/shatteredsilicon/qan-agent/test/mock"
	"github.com/shatteredsilicon/qan-agent/test/profiling"
)

func TestCollectingAndSendingData(t *testing.T) {
	// Disable profiling.
	err := profiling.New("").DisableAll()
	require.NoError(t, err)
	// Enable profiling for default db.
	err = profiling.New("").Enable("")
	require.NoError(t, err)

	// Create dependencies.
	serviceName := "plugin"
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI("").SetServerAPIOptions(serverAPI)
	logChan := make(chan proto.LogEntry)
	logger := pct.NewLogger(logChan, serviceName)
	dataChan := make(chan interface{})
	spool := mock.NewSpooler(dataChan)
	// Create the QAN config.
	exampleQueries := true
	qanConfig := config.QAN{
		UUID:           "12345678",
		Interval:       5, // seconds
		ExampleQueries: &exampleQueries,
	}
	plugin := New(mongoOpts, logger, spool, qanConfig)

	assert.Empty(t, plugin.Status())
	err = plugin.Start()
	require.NoError(t, err)
	assert.Equal(t, "Profiling enabled for all queries (ratelimit: 1)", plugin.Status()["collector-profile-test"])

	// Add some data to mongo e.g. people.
	people := []map[string]string{
		{"name": "Kamil"},
		{"name": "Carlos"},
	}
	// Add data through separate connection.
	client, err := mongo.Connect(context.TODO(), mongoOpts)
	require.NoError(t, err)
	for _, person := range people {
		_, err = client.Database("").Collection("people").InsertOne(context.TODO(), &person)
		require.NoError(t, err)
	}

	// Wait until we receive data
	select {
	case data := <-dataChan:
		qanReport := data.(*qan.Report)
		assert.EqualValues(t, 2, qanReport.Global.TotalQueries)
		assert.EqualValues(t, 1, qanReport.Global.UniqueQueries)
	case <-time.After(2 * time.Duration(qanConfig.Interval) * time.Second):
		t.Fatal("timeout waiting for data")
	}

	status := plugin.Status()
	assert.Equal(t, "Profiling enabled for all queries (ratelimit: 1)", status["collector-profile-test"])
	assert.Equal(t, "2", status["collector-in-test"])
	assert.Equal(t, "2", status["collector-out-test"])
	assert.Equal(t, "2", status["parser-docs-in-test"])
	assert.Equal(t, "2", status["aggregator-docs-in"])
	assert.Equal(t, "1", status["aggregator-reports-out"])
	assert.Equal(t, "1", status["sender-in"])
	assert.Equal(t, "1", status["sender-out"])

	err = plugin.Stop()
	require.NoError(t, err)
	assert.Empty(t, plugin.Status())
}
