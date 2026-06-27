package explain

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

var convertTests = map[string][]struct {
	original string
	expected string
}{
	"DELETE": {
		{
			original: "DELETE FROM t1",
			expected: "SELECT * FROM t1",
		},
		{
			original: "DELETE FROM t1 WHERE id > 1",
			expected: "SELECT * FROM t1 WHERE id > 1",
		},
		{
			original: "DELETE FROM t1 WHERE id > 1 AND c != '1'",
			expected: "SELECT * FROM t1 WHERE id > 1 AND c != '1'",
		},
		{
			original: "DELETE t1, t2 FROM t1 JOIN t2 WHERE t1.id = t2.id",
			expected: "SELECT 1 FROM t1 JOIN t2 WHERE t1.id = t2.id",
		},
	},
	"INSERT": {
		{
			original: "INSERT INTO t1 (id, c1, c2) SELECT id, c1, c2 FROM t2",
			expected: "SELECT id, c1, c2 FROM t2",
		},
	},
}

func TestTryConvertToExplainable(t *testing.T) {
	t.Parallel()

	for name, tests := range convertTests {
		currentTests := tests
		t.Run(name, func(t *testing.T) {
			for _, test := range currentTests {
				converted := tryConvertToExplainable(test.original)

				parser := sqlparser.NewTestParser()
				require.NotNil(t, parser)

				convertedAST, err := parser.Parse(converted)
				require.Nil(t, err)
				require.NotNil(t, convertedAST)

				expectedAST, err := parser.Parse(test.expected)
				require.Nil(t, err)
				require.NotNil(t, expectedAST)

				require.Equal(t, sqlparser.CanonicalString(expectedAST), sqlparser.CanonicalString(convertedAST))
			}
		})

	}
}
