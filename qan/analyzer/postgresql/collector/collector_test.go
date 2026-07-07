package collector

import (
	"fmt"
	"testing"

	queryProto "github.com/shatteredsilicon/ssm/proto/query"
	"github.com/stretchr/testify/require"
)

var parseQueryTests = []struct {
	query      string
	abstract   string
	tables     []queryProto.Table
	procedures []queryProto.Procedure
}{
	{
		"select c from t where id=1",
		"SELECT t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},
	{ // #1
		"select c from db.t where id=1",
		"SELECT db.t",
		[]queryProto.Table{{Db: "db", Table: "t"}},
		nil,
	},
	{ // #2
		"select c from db.t, t2 where id=1",
		"SELECT db.t t2",
		[]queryProto.Table{
			{Db: "db", Table: "t"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
	{ // #3
		"SELECT /*!40001 SQL_NO_CACHE */ * FROM \"film\"",
		"SELECT film",
		[]queryProto.Table{{Db: "", Table: "film"}},
		nil,
	},
	{ // #4
		"select c from ta join tb on (ta.id=tb.id) where id=1",
		"SELECT ta tb",
		[]queryProto.Table{
			{Db: "", Table: "ta"},
			{Db: "", Table: "tb"},
		},
		nil,
	},
	{ // #5
		"select c from ta join tb on (ta.id=tb.id) join tc on (1=1) where id>1",
		"SELECT ta tb tc",
		[]queryProto.Table{
			{Db: "", Table: "ta"},
			{Db: "", Table: "tb"},
			{Db: "", Table: "tc"},
		},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// INSERT
	{ // #6
		"INSERT INTO my_table (a,b,c) VALUES (1, 2, 3)",
		"INSERT my_table",
		[]queryProto.Table{{Db: "", Table: "my_table"}},
		nil,
	},
	{ // #7
		"INSERT INTO d.t (a,b,c) VALUES (1, 2, 3)",
		"INSERT d.t",
		[]queryProto.Table{{Db: "d", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// UPDATE
	{ // #8
		"update t set foo='bar'",
		"UPDATE t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// DELETE
	{ // #9
		"delete from t where id in (1, 2, 3, 4)",
		"DELETE t",
		[]queryProto.Table{{Db: "", Table: "t"}},
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	// Other with partial support
	{ // #10
		"show max_connections",
		"SHOW",
		nil,
		nil,
	},

	/////////////////////////////////////////////////////////////////////
	{ // #11
		"VACUUM \"o2408\".\"agent_log\"",
		"VACUUM o2408.agent_log",
		[]queryProto.Table{
			{Db: "o2408", Table: "agent_log"},
		},
		nil,
	},
	{ // #12
		"select c from t1 join t2 using (c) where id!=1",
		"SELECT t1 t2",
		[]queryProto.Table{
			{Db: "", Table: "t1"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
	{ // #13
		"insert into data values (1, 2, 3)",
		"INSERT data",
		[]queryProto.Table{
			{Db: "", Table: "data"},
		},
		nil,
	},
	{ // #14
		"call\n pita(1)",
		"CALL pita",
		nil,
		[]queryProto.Procedure{
			{DB: "", Name: "pita"},
		},
	},
	{ // #15
		"CALL db.pita(1)",
		"CALL db.pita",
		nil,
		[]queryProto.Procedure{
			{DB: "db", Name: "pita"},
		},
	},
	{ // #16 exceeds MAX_JOIN_DEPTH
		"select c from a" +
			" join b on (1=1) join c on (1=1) join d on (1=1) join e on (1=1)" +
			" join f on (1=1) join g on (1=1) join h on (1=1) join i on (1=1)" +
			" join j on (1=1) join k on (1=1) join l on (1=1) join m on (1=1)" +
			" join n on (1=1) join o on (1=1) join p on (1=1) join q on (1=1)" +
			" join r on (1=1) join s on (1=1) join t on (1=1) join u on (1=1)" +
			" join v on (1=1) join w on (1=1) join x on (1=1) join y on (1=1)" +
			" join z on (1=1)" +
			" where id=1",
		"SELECT a b c d e f g h i j k l m n o p q r s t u v w x y z",
		[]queryProto.Table{
			{"", "a"},
			{"", "b"}, {"", "c"}, {"", "d"}, {"", "e"},
			{"", "f"}, {"", "g"}, {"", "h"}, {"", "i"},
			{"", "j"}, {"", "k"}, {"", "l"}, {"", "m"},
			{"", "n"}, {"", "o"}, {"", "p"}, {"", "q"},
			{"", "r"}, {"", "s"}, {"", "t"}, {"", "u"},
			{"", "v"}, {"", "w"}, {"", "x"}, {"", "y"},
			{"", "z"},
		},
		nil,
	},
	{ // #17
		"SELECT DISTINCT c\n FROM sbtest1\nWHERE id\nBETWEEN 1\nAND 100\nORDER BY  c\n",
		"SELECT sbtest1",
		[]queryProto.Table{{Db: "", Table: "sbtest1"}},
		nil,
	},
	{ // #18
		"SELECT DISTINCT c FROM sbtest2 WHERE id BETWEEN 1 AND 100 ORDER BY c",
		"SELECT sbtest2",
		[]queryProto.Table{{Db: "", Table: "sbtest2"}},
		nil,
	},
	// Don't remove the ; at the end of the next query.
	// There was an error in the past where a ; at the end was making the
	// parser to fail and we want to ensure it works now.
	{ // #19
		"SELECT * from \"sysbenchtest\".\"t6002_0\";",
		"SELECT sysbenchtest.t6002_0",
		[]queryProto.Table{{Db: "sysbenchtest", Table: "t6002_0"}},
		nil,
	},
	// Schema was set as default from the previous USE
	{ // #20
		"SELECT * from \"t6003_0\";",
		"SELECT t6003_0",
		[]queryProto.Table{{Db: "", Table: "t6003_0"}},
		nil,
	},
	{ // #21
		"CREATE TABLE t6004 (id int, a varchar(25), PRIMARY KEY (id))",
		"CREATE TABLE t6004",
		[]queryProto.Table{{Db: "", Table: "t6004"}},
		nil,
	},
	{ // #22
		"ALTER TABLE sakila.actor ADD COLUMN newcol int",
		"ALTER TABLE sakila.actor",
		[]queryProto.Table{{Db: "sakila", Table: "actor"}},
		nil,
	},
	// Db & Table are empty because CREATE DATABASE is not yet supported by Vitess.sqlparser
	{ // #23
		"CREATE DATABASE ssm",
		"CREATE DATABASE ssm",
		nil,
		nil,
	},
	{ // #24
		"create index idx ON percona (f1)",
		"ALTER TABLE percona",
		[]queryProto.Table{{Db: "", Table: "percona"}},
		nil,
	},
	{ // #25 override the default USE
		"create index idx ON brannigan.percona (f1)",
		"ALTER TABLE brannigan.percona",
		[]queryProto.Table{{Db: "brannigan", Table: "percona"}},
		nil,
	},
	// PMM-1892. Upgraded Vitess libraries to support this query.
	// Notice that the query below is not exactly the same reported in the ticket; this
	// query has `auto_increment` between backticks because it is a reserved MySQL word
	// but MySQL accepts it anyway as a field name while Vitess doesn't.
	{ // #26
		"SELECT table_schema, table_name, column_name, \"auto_increment\", " +
			"pow(2, CASE data_type WHEN 'tinyint' THEN 7 WHEN 'smallint' " +
			"THEN 15 WHEN 'mediumint' THEN 23 WHEN 'int' THEN 31 WHEN 'bigint' " +
			"THEN 63 end+(column_type LIKE '% unsigned'))-1 AS max_int FROM " +
			"information_schema.tables t JOIN information_schema.columns c " +
			"USING (table_schema,table_name) WHERE c.extra = 'auto_increment' " +
			"AND t.auto_increment IS NOT NULL",
		"SELECT information_schema.tables information_schema.columns",
		[]queryProto.Table{
			{Db: "information_schema", Table: "tables"},
			{Db: "information_schema", Table: "columns"},
		},
		nil,
	},
	{ // #27
		"SELECT @@version",
		"SELECT",
		nil,
		nil,
	},
	{ // #28
		"SELECT t1.*, t2.* FROM (SELECT * FROM test1) t1 JOIN (SELECT * FROM test2) t2 ON t1.id1 = t2.id2",
		"SELECT test1 test2",
		[]queryProto.Table{
			{Db: "", Table: "test1"},
			{Db: "", Table: "test2"},
		},
		nil,
	},
	{ // #29
		"SELECT t.* FROM (SELECT t1.*, t2.* FROM (SELECT * FROM test1) t1 JOIN (SELECT * FROM test2) t2 ON t1.id1 = t2.id2) t UNION SELECT t.* FROM (SELECT t3.*, t4.* FROM (SELECT * FROM test3) t3 JOIN (SELECT * FROM test4) t4 ON t3.id3 = t4.id4) t",
		"SELECT test1 test2 test3 test4",
		[]queryProto.Table{
			{Db: "", Table: "test1"},
			{Db: "", Table: "test2"},
			{Db: "", Table: "test3"},
			{Db: "", Table: "test4"},
		},
		nil,
	},
	{ // #30
		`
		-- UPDATE test users
		UPDATE test.users
		SET user_id = @USER,
			email = (
				SELECT user_email
				FROM test.wp_users
				WHERE id = @USER
			)
		WHERE wp_user_id = @USER
		`,
		"UPDATE test.users",
		[]queryProto.Table{
			{Db: "test", Table: "users"},
			{Db: "test", Table: "wp_users"},
		},
		nil,
	},
	{ // #31
		`
		SELECT *
		FROM t1
		WHERE dID IN
			(SELECT dID
			FROM t2
			WHERE uID ='12345')
				AND "enabled" = 1
				AND "generated" = 1
		`,
		"SELECT t1",
		[]queryProto.Table{
			{Db: "", Table: "t1"},
			{Db: "", Table: "t2"},
		},
		nil,
	},
	{ // #32
		`
		SELECT tables.table_schema, tables.table_name, pg_class.reltuples::bigint AS table_rows
		FROM information_schema.tables tables
		LEFT JOIN pg_class ON pg_class.oid = CONCAT(tables.table_schema, '.', tables.table_name)::regclass
		WHERE tables.table_name IN ('pg_database','pg_stat_activity')
		ORDER BY table_rows DESC
		`,
		"SELECT information_schema.tables pg_class",
		[]queryProto.Table{
			{Db: "information_schema", Table: "tables"},
			{Db: "", Table: "pg_class"},
		},
		nil,
	},
}

func TestParseQuery(t *testing.T) {
	t.Parallel()

	for i, test := range parseQueryTests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			abstract, tables, procedures, err := parseQuery(test.query)
			require.Nil(t, err)

			require.Equal(t, test.abstract, abstract)
			require.Equal(t, test.tables, tables)
			require.Equal(t, test.procedures, procedures)
		})
	}
}
