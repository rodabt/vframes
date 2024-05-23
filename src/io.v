module vframes

import os
import rand
import x.json2

pub fn (mut ctx DataFrameContext) read_auto(filename string) !DataFrame {
	if !os.is_file(filename) {
		return error("Incorrect filename: ${filename}")
	}
	id := 'tbl_${rand.ulid()}'
	mut df := DataFrame{
		ctx: ctx
	}
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * from '${filename}'") or { panic(err) }
	return DataFrame{
		id: id
		ctx: ctx
	}
}


pub fn (mut ctx DataFrameContext) read_records(dict []map[string]json2.Any) DataFrame {
	id := 'tbl_${rand.ulid()}'
	tmp_dict := dict.map(it.str())
	tmp_file := os.join_path_single(os.temp_dir(),'tmp_${rand.ulid()}.json')
	os.write_file(tmp_file, tmp_dict.join_lines()) or { panic(err) }
	_ := ctx.db.query("create table ${id} as select * from '${tmp_file}'") or { panic(err) }
	return DataFrame{
		ctx: ctx
	}
}

fn (mut ctx DataFrameContext) data() []map[string]json2.Any {
	return ctx.db.get_array()
}