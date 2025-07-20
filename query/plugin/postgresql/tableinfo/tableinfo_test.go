package tableinfo

import (
	"reflect"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/shatteredsilicon/ssm/proto"
)

type example struct {
	query  string
	tables []proto.Table
}

var examples = []example{
	{
		query: `
			SELECT query_id,
					query,
					query_start
			FROM pg_stat_activity
		`,
		tables: []proto.Table{
			{Db: "", Table: "pg_stat_activity"},
		},
	},
	{
		query: `
			SELECT queryid,
					pg_database.datname,
					query,
					calls,
					total_exec_time,
					min_exec_time,
					max_exec_time,
					mean_exec_time,
					rows,
					shared_blks_hit,
					shared_blks_read,
					shared_blks_dirtied,
					shared_blks_written
			FROM pg_stat_statements
			JOIN pg_database
				ON pg_stat_statements.dbid = pg_database.oid
		`,
		tables: []proto.Table{
			{Db: "", Table: "pg_stat_statements"},
			{Db: "", Table: "pg_database"},
		},
	},
	{
		query: `
			SELECT a.actor_id,                                                                                       
					a.first_name,
					a.last_name,
					group_concat(DISTINCT (((c.name)::text ||': '::text) || 
				(SELECT group_concat((f.title)::text) AS group_concat
				FROM ((film f
				JOIN film_category fc_1
					ON ((f.film_id = fc_1.film_id)))
				JOIN film_actor fa_1
					ON ((f.film_id = fa_1.film_id)))
				WHERE ((fc_1.category_id = c.category_id)
						AND (fa_1.actor_id = a.actor_id))
				GROUP BY  fa_1.actor_id))) AS film_info
			FROM (((actor a
			LEFT JOIN film_actor fa
				ON ((a.actor_id = fa.actor_id)))
			LEFT JOIN film_category fc
				ON ((fa.film_id = fc.film_id)))
			LEFT JOIN category c
				ON ((fc.category_id = c.category_id)))
			GROUP BY  a.actor_id, a.first_name, a.last_name
		`,
		tables: []proto.Table{
			{Db: "", Table: "film"},
			{Db: "", Table: "film_category"},
			{Db: "", Table: "film_actor"},
			{Db: "", Table: "actor"},
			{Db: "", Table: "category"},
		},
	},
}

func TestParse(t *testing.T) {
	t.Run("examples", func(t *testing.T) {
		for i, e := range examples {
			t.Run(e.query, func(t *testing.T) {
				pr, err := pg_query.Parse(e.query)
				if err != nil {
					t.Errorf("Error in test # %d: %s", i, err)
					return
				}
				tables := getTablesFromParseResult(pr)
				if !reflect.DeepEqual(tables, e.tables) {
					t.Errorf("Test # %d: tables are different.\nWant: %#v\nGot: %#v", i, e.tables, tables)
				}
			})
		}
	})

}
