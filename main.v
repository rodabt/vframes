module main

import vduckdb
import rand
import os

struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
mut:
	dpath				string = os.join_path(os.temp_dir(), 'vf_${rand.ulid()}.db')
}

fn load_from_file(s string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	dpath := os.join_path(os.temp_dir(), 'vf_${rand.ulid()}.db')
	mut db := vduckdb.DuckDB{}
	_ := db.open(dpath) or { panic(err) }
	_ := db.query("create table ${id} as select * from '${s}'") or { panic(err) }
	defer {
		db.close()
	}
	return DataFrame{
		id: id
		dpath: dpath
	}
}

fn main() {
	mut df := load_from_file('tmp/people-100.csv')
	dump(df)
}