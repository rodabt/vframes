module main

import vduckdb
import rand
import arrays


@[params]
pub struct ContextConfig {
pub:
	location			string = ":memory:"	
}

@[noinit]
struct DataFrameContext {
	dpath				string
mut:	
	db					vduckdb.DuckDB
}

struct Step {
	id					string
	query				string
}

@[noinit]
struct DataFrame {
mut:
	steps				[]Step
pub mut:
	ctx					DataFrameContext
	display_mode		string = 'box'
	display_max_rows	int = 100
}

// TODO: Option to materialize or not (globally or per step)

/***
 
 CONTEXT CORE

***/


fn init(cfg ContextConfig) DataFrameContext {
	mut db := vduckdb.DuckDB{}
	_ := db.open(cfg.location) or { panic(err) }
	_ := db.query("select 1") or { panic(err) }
	return DataFrameContext{
		dpath: cfg.location
		db: db 
	}
}

fn (mut ctx DataFrameContext) close() {
	ctx.db.close()
}

/***
 
 DATA INPUT AND OUTPUT

***/

fn (mut ctx DataFrameContext) read_csv(filename string) DataFrame {
	mut df := DataFrame{
		ctx: ctx
	}
	tbl := 'tbl_${rand.ulid()}'
	step := Step{
		id: tbl
		query: "select * from '${filename}'"
	}
	df.steps << step
	return df
}

/***
 
 DATA EXPLORATION

***/

fn (df DataFrame) base() (string,string) {
	exps := arrays.map_indexed[Step, string](df.steps, fn (idx int, elem Step) string {
		return '${elem.id} as (${elem.query})'
	})
	last_id := df.steps.last().id
	q_base := 'WITH ${exps.join(",")}'
	return last_id, q_base
}

fn (df DataFrame) head(n int) string {
	mut db := &df.ctx.db
	last_id, q_base := df.base()
	_ := db.query("${q_base} select * from ${last_id} limit ${n}") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) tail(n int) string {
	mut db := &df.ctx.db
	last_id, q_base := df.base()
	tbl := 'tbl_${rand.ulid()}'
	q := "
	${q_base}, 
	${tbl} AS (
		SELECT row_number() OVER() as _row_num,* 
		FROM ${last_id}
	) SELECT * EXCLUDE(_row_num) FROM (SELECT * FROM ${tbl} ORDER BY _row_num DESC limit ${n}) ORDER BY _row_num ASC
	"
	_ := db.query(q) or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) info() string {
	mut db := &df.ctx.db
	last_id, q_base := df.base()
	_ := db.query("${q_base} SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM ${last_id})") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) describe() string {
	mut db := &df.ctx.db
	last_id, q_base := df.base()
	_ := db.query("${q_base} SELECT * FROM (SUMMARIZE SELECT * FROM ${last_id})") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) shape() []int {
	mut db := &df.ctx.db
	last_id, q_base := df.base()
	_ := db.query('${q_base} SELECT COUNT(*) as rows FROM ${last_id}') or { panic(err) }
	res_rows := db.get_array()
	num_rows := (res_rows[0]["rows"] or {0}).int()

	_ := db.query('${q_base} SELECT COUNT(DISTINCT column_name) as cols FROM (SUMMARIZE SELECT * FROM ${last_id})') or { panic(err) }
	res_cols := db.get_array()
	num_cols := (res_cols[0]["cols"] or {0}).int()
	
	return [num_rows,num_cols]
}

fn (df DataFrame) query(q string) DataFrame {
	last_id := df.steps.last().id
	tbl := 'tbl_${rand.ulid()}'
	mut new_df := DataFrame{
		ctx: df.ctx
		steps: df.steps 
	}
	new_df.steps << Step{
		id: tbl
		query: 'select ${q} from ${last_id}'
	}
	return new_df
}


/***
 
 DATA TRANSFORMATION

***/


/** MAIN **/

fn p(msg string, out string) {
	println("\n${msg}")
	println(out)
}


fn main() {
	mut ctx := init() // location: 'ctx.db'
	df := ctx.read_csv('tmp/people-100.csv')
	p("First 5 records:", df.head(5))
	p("Tail:", df.tail(5))
	p("DataFrame info:", df.info())
	p("DataFrame shape:", df.shape().str())
	// // p("DataFrame size:", df.size)
	p("Describe:", df.describe())
	
	df2 := df.query("Index*15 as idx,100 as val")
	p("New 10", df2.head(10))
	ctx.close()
}

