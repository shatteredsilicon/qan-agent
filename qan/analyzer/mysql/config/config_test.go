package config

import (
	"testing"

	"github.com/shatteredsilicon/qan-agent/qan/analyzer"
	pc "github.com/shatteredsilicon/ssm/proto/config"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	uuid := "123"
	exampleQueries := true
	cfg := pc.QAN{
		UUID:           uuid,
		Interval:       300,        // 5 min
		MaxSlowLogSize: 1073741824, // 1 GiB
		ExampleQueries: &exampleQueries,
		CollectFrom:    "slowlog",
	}
	_, err := ValidateConfig(analyzer.QAN{QAN: cfg})
	require.NoError(t, err)
}
