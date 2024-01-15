package collector

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/percona/percona-toolkit/src/go/mongolib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/shatteredsilicon/qan-agent/test/profiling"
)

func TestNew(t *testing.T) {
	t.Parallel()

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI("mongodb://127.0.0.1:27017").SetServerAPIOptions(serverAPI)
	require.NoError(t, mongoOpts.Validate())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	require.NoError(t, err)

	type args struct {
		client *mongo.Client
		dbName string
	}
	tests := []struct {
		name string
		args args
		want *Collector
	}{
		{
			name: "127.0.0.1:27017",
			args: args{
				client: client,
				dbName: "",
			},
			want: &Collector{
				client: client,
				dbName: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.client, tt.args.dbName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New(%v, %v) = %v, want %v", tt.args.client, tt.args.dbName, got, tt.want)
			}
		})
	}
}

func TestCollector_StartStop(t *testing.T) {
	t.Parallel()

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI("mongodb://127.0.0.1:27017").SetServerAPIOptions(serverAPI)
	require.NoError(t, mongoOpts.Validate())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	require.NoError(t, err)

	collector1 := New(client, "")
	docsChan, err := collector1.Start()
	require.NoError(t, err)
	assert.NotNil(t, docsChan)

	defer collector1.Stop()
}

func TestCollector_Stop(t *testing.T) {
	t.Parallel()

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI("mongodb://127.0.0.1:27017").SetServerAPIOptions(serverAPI)
	require.NoError(t, mongoOpts.Validate())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	require.NoError(t, err)

	// #1
	notStarted := New(client, "")

	// #2
	started := New(client, "")
	_, err = started.Start()
	require.NoError(t, err)

	tests := []struct {
		name string
		self *Collector
	}{
		{
			name: "not started",
			self: notStarted,
		},
		{
			name: "started",
			self: started,
		},
		// repeat to be sure Stop() is idempotent
		{
			name: "not started",
			self: notStarted,
		},
		{
			name: "started",
			self: started,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.self.Stop()
		})
	}
}

func TestCollector(t *testing.T) {
	// Disable profiling.
	err := profiling.New("").DisableAll()
	require.NoError(t, err)
	// Enable profiling for default db.
	err = profiling.New("").Enable("")
	require.NoError(t, err)

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	mongoOpts := options.Client().ApplyURI("mongodb://127.0.0.1:27017").SetServerAPIOptions(serverAPI)
	require.NoError(t, mongoOpts.Validate())

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	require.NoError(t, err)

	// create collector
	collector := New(client, "")
	docsChan, err := collector.Start()
	require.NoError(t, err)
	defer collector.Stop()

	// add some data to mongo e.g. people
	people := []map[string]string{
		{"name": "Kamil"},
		{"name": "Carlos"},
	}

	// add data through separate connection
	for _, person := range people {
		_, err = client.Database("test").Collection("people").InsertOne(context.TODO(), &person)
		require.NoError(t, err)
	}

	actual := []proto.SystemProfile{}
F:
	for {
		select {
		case doc, ok := <-docsChan:
			if !ok {
				break F
			}
			if doc.Ns == "test.people" && doc.Op == "insert" {
				actual = append(actual, doc)
			}
			if len(actual) == len(people) {
				// stopping collector should also close docsChan
				collector.Stop()
			}
		case <-time.After(10 * time.Second):
			t.Fatal("didn't recieve enough samples before timeout")
		}
	}
	assert.Len(t, actual, len(people))
}
