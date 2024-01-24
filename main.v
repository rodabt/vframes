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
mut:
	display_mode		string = 'box'
	display_max_rows	int = 100			
	size				string
	info				VTable
	dim					[]int
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
	df := DataFrame{
		id: id
		ctx: ctx
	}
	defer { db.close() }
	return df
} 

fn (df DataFrame) head(n int) string {
	mut db := df.ctx.exec("select * from ${df.id} limit ${n}")
	if df.display_mode == 'box' {
		return db.print_table(max_rows: df.display_max_rows, mode: 'box')
	} else {
		return db.get_array()
	}
}

fn main() {
	mut ctx := DContext{}
	mut df := ctx.read_csv('tmp/people-100.csv')
	println(df.head(5))
	// println(df.dim)
    // println(df.describe())
    // println(df.info())
    // println(df.shape())
}