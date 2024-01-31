module main

import vduckdb
import rand
import os
import x.json2

// TODO: Implement in the future
type VTable = []map[string]json2.Any | string

struct DContext {
	dpath				string = os.join_path(os.temp_dir(), 'vf_${rand.ulid()}.db')
}


@[noinit]
struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
	ctx					DContext
pub:		
	size				string
	shape				[]int
pub mut:
	display_mode		string = 'box'
	display_max_rows	int = 100
}


fn (ctx DContext) exec(query string) vduckdb.DuckDB {
	mut db := vduckdb.DuckDB{}
	_ := db.open(ctx.dpath) or { panic(err) }
	_ := db.query(query) or { panic(err) }	
	return db
}


fn (ctx DContext) new() DataFrame {
	return DataFrame{
		ctx: ctx
	}
} 


fn (ctx DContext) read_csv(filename string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := ctx.exec("create table ${id} as select * from '${filename}'")
	db = ctx.exec("pragma enable_progress_bar")
	db = ctx.exec("select count(*) as n_rows from ${id}")
	mut res := db.get_array()
	num_rows := (res[0]["n_rows"] or {0}).int()

	db = ctx.exec("select count(*) as n_cols from information_schema.columns where table_name = '${id}'")
	res = db.get_array()
	num_cols := (res[0]["n_cols"] or {0}).int()

	db = ctx.exec("pragma database_size")
	res = db.get_array()
	size := (res[0]["memory_usage"] or {'0'}).str()

	df := DataFrame{
		id: id
		ctx: ctx
		shape: [num_rows, num_cols]
		size: size
	}
	defer { db.close() }
	return df
} 

fn (df DataFrame) head(n int) string {
	mut db := df.ctx.exec("select * from ${df.id} limit ${n}")
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) info() string {
	mut db := df.ctx.exec("pragma table_info('${df.id}')")
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}


fn main() {
	mut ctx := DContext{}
	mut df := ctx.read_csv('tmp/people-500000.csv')
	println(df.head(5))
	println(df.info())
    println(df.shape)
    println(df.size)
}