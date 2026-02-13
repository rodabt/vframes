module vframes

import os
import rand
import x.json2

// Reads a data file from disk. It tries automatically to infer the structure directly from the file
// Currently Accepted formats: .csv, .json, .parquet
// NOTE: The json parser is still under testing
pub fn (mut ctx DataFrameContext) read_auto(filename string) !DataFrame {
	if !os.is_file(filename) {
		return error("Incorrect filename: ${filename}")
	}
	id := 'tbl_${rand.ulid()}'
	mut df := DataFrame{
		ctx: ctx
	}
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * from '${filename}'") or { return err }
	return DataFrame{
		id: id
		ctx: ctx
	}
}

// Reads []map[string]json2.Any and store in DataFrame 
pub fn (mut ctx DataFrameContext) read_records(dict []map[string]json2.Any) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	tmp_dict := dict.map(it.str())
	tmp_file := os.join_path_single(os.temp_dir(),'tmp_${rand.ulid()}.json')
	os.write_file(tmp_file, tmp_dict.join_lines()) or { return err }
	_ := ctx.db.query("create table ${id} as select * from '${tmp_file}'") or { return err }
	os.rm(tmp_file) or { return err }
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ToCsvOptions {
	delimiter string = ','
	header bool = true
	nullstr string = 'NA'
}

// Exports DataFrame to a CSV file
pub fn (df DataFrame) to_csv(path string, opts ToCsvOptions) ! {
	mut db := &df.ctx.db
	header_stmt := if opts.header { 'header: true' } else { 'header: false' }
	delim := if opts.delimiter == '\t' { '\t' } else { opts.delimiter[0].str() }
	query := 'COPY (SELECT * FROM ${df.id}) TO \'${path}\' (FORMAT CSV, ${header_stmt}, DELIMITER \'${delim}\', NULL \'${opts.nullstr}\')'
	_ := db.query(query) or { return err }
}

// Exports DataFrame to a JSON file
pub fn (df DataFrame) to_json(path string) ! {
	mut db := &df.ctx.db
	query := "COPY (SELECT * FROM ${df.id}) TO '${path}' (FORMAT JSON)"
	_ := db.query(query) or { return err }
}

// Exports DataFrame to a Parquet file
pub fn (df DataFrame) to_parquet(path string) ! {
	mut db := &df.ctx.db
	query := "COPY (SELECT * FROM ${df.id}) TO '${path}' (FORMAT PARQUET)"
	_ := db.query(query) or { return err }
}