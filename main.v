module main

import vduckdb
import rand
import x.json2

// TODO: Implement in the future
type VTable = []map[string]json2.Any

@[params]
pub struct ContextConfig {
pub:
	location			string = ":memory:"	
}

@[noinit]
struct DataFrameContext {
	dpath				string // os.join_path(os.temp_dir(), 'vf_${rand.ulid()}.db')
mut:	
	db					vduckdb.DuckDB
}

@[noinit]
struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
pub:		
	size				string
	shape				[]int
pub mut:
	ctx					DataFrameContext
	display_mode		string = 'box'
	display_max_rows	int = 100
}

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

/* fn (ctx DataFrameContext) exec(q string) VTable {
	mut res := ctx.db.query(q) or { panic(err) }
	res = db.get_array()
	return res
}

fn (ctx DataFrameContext) q_string(q string) string {
	res := ctx.db.exec(q)
	res = db.get_array()
	return res
}*/

/***
 
 DATA INPUT AND OUTPUT

***/

fn (mut ctx DataFrameContext) read_csv(filename string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	_ := ctx.db.query("create table ${id} as select * from '${filename}'") or { panic(err) }
	_ := ctx.db.query("select count(*) as n_rows from ${id}") or { panic(err) }
	mut res := ctx.db.get_array()
	num_rows := (res[0]["n_rows"] or {0}).int()

	_ := ctx.db.query("select count(*) as n_cols from information_schema.columns where table_name = '${id}'") or { panic(err) }
	res = ctx.db.get_array()
	num_cols := (res[0]["n_cols"] or {0}).int()

	_ := ctx.db.query("pragma database_size") or { panic(err) }
	res = ctx.db.get_array()
	size := (res[0]["memory_usage"] or {'0'}).str()

	df := DataFrame{
		id: id
		ctx: ctx
		shape: [num_rows, num_cols]
		size: size
	}
	return df
}

/***
 
 DATA EXPLORATION

***/

fn (df DataFrame) head(n int) string {
	mut db := &df.ctx.db
	_ := db.query("select * from ${df.id} limit ${n}") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) tail(n int) string {
	mut db := &df.ctx.db
	q := "
	WITH base AS (
		SELECT row_number() OVER() as _row_num,* 
		FROM ${df.id}
	) SELECT * EXCLUDE(_row_num) FROM base ORDER BY _row_num DESC limit ${n}
	"
	_ := db.query(q) or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) info() string {
	mut db := &df.ctx.db
	_ := db.query("PRAGMA table_info('${df.id}')") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) describe() string {
	mut db := &df.ctx.db
	_ := db.query("SUMMARIZE ${df.id}") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) query(q string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${q} from ${df.id}") or { panic(err) }
	_ := db.query("select count(*) as n_rows from ${id}") or { panic(err) } 
	mut res := db.get_array()
	num_rows := (res[0]["n_rows"] or {0}).int()

	_ := db.query("select count(*) as n_cols from information_schema.columns where table_name = '${id}'") or { panic(err) }
	res = db.get_array()
	num_cols := (res[0]["n_cols"] or {0}).int()

	_ := db.query("pragma database_size") or { panic(err) }
	res = db.get_array()
	size := (res[0]["memory_usage"] or {'0'}).str()
	new_df := DataFrame{
		id: id
		ctx: df.ctx
		shape: [num_rows, num_cols]
		size: size
	}
	return new_df
}

/***
 
 DATA TRANSFORMATION

***/


fn main() {
	mut ctx := init(location: 'ctx.db')
	df := ctx.read_csv('tmp/people-100.csv')
	println("First 5 records:")
	println(df.head(5))
	println("\nDataFrame info:")
	println(df.info())
	println("\nDataFrame shape:")
    println(df.shape)
	println("\nDataFrame size:")
    println(df.size)
	println("\nTail:")
    println(df.tail(5))
	println("\nDescribe:")
    println(df.describe())
	println("\nNew dataframe as a result of a query: 'Index*15 as idx, 100 as val'")
	mut df2 := df.query("Index*15 as idx,100 as val")
	println("First 5 records:")
	println(df2.head(10))
	ctx.close()
}