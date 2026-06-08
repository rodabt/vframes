module vframes

import os
import rand
import x.json2

// Reads a data file from disk. It tries automatically to infer the structure directly from the file
// Currently Accepted formats: .csv, .json, .parquet
// NOTE: The json parser is still under testing
pub fn (mut ctx DataFrameContext) read_auto(filename string) !DataFrame {
	// Local non-glob files must exist; remote and glob paths are validated by DuckDB.
	if !is_remote_path(filename) && !filename.contains('*') && !os.is_file(filename) {
		return error("Incorrect filename: ${filename}")
	}
	lower := filename.to_lower()
	if lower.ends_with('.xlsx') {
		return ctx.read_excel(filename)
	}
	// Ensure httpfs for remote files so the generic reader can fetch them.
	if is_remote_path(filename) {
		ctx.ensure_extension('httpfs')!
	}
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
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
	header_stmt := if opts.header { 'HEADER' } else { 'NO HEADER' }
	delim := opts.delimiter
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

// Returns all rows as []map[string]json2.Any (in-memory dict representation)
pub fn (df DataFrame) to_dict() ![]map[string]json2.Any {
	mut db := &df.ctx.db
	_ := db.query('SELECT * FROM ${df.id}') or { return err }
	return db.get_array()
}

@[params]
pub struct ReadCsvOptions {
pub:
	delimiter   string            // '' = DuckDB auto-detect
	header      bool = true       // used only when auto_header is false
	auto_header bool = true       // true = let DuckDB detect the header row
	columns     map[string]string // explicit name -> SQL type overrides
	all_varchar bool              // read every column as VARCHAR
	nullstr     string            // string treated as NULL
	sample_size int               // 0 = DuckDB default
}

// Reads a CSV file (local path or remote URL) into a new DataFrame.
// Supports glob patterns (e.g. 'data/*.csv') which DuckDB unions into one table.
pub fn (mut ctx DataFrameContext) read_csv(path string, opts ReadCsvOptions) !DataFrame {
	ext := scheme_for_path(path)
	if ext != '' {
		ctx.ensure_extension(ext)!
	}
	mut args := []string{}
	if opts.delimiter != '' {
		args << "delim='${opts.delimiter}'"
	}
	if !opts.auto_header {
		args << 'header=${opts.header}'
	}
	if opts.all_varchar {
		args << 'all_varchar=true'
	}
	if opts.nullstr != '' {
		args << "nullstr='${opts.nullstr}'"
	}
	if opts.sample_size > 0 {
		args << 'sample_size=${opts.sample_size}'
	}
	if opts.columns.len > 0 {
		mut pairs := []string{}
		for k, v in opts.columns {
			pairs << "'${k}': '${v}'"
		}
		args << 'columns={${pairs.join(', ')}}'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_csv('${path}'${arg_str})") or {
		return error('Failed to read CSV "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ReadParquetOptions {
pub:
	union_by_name     bool // union files by column name when globbing
	hive_partitioning bool
}

// Reads a Parquet file (local or remote) into a new DataFrame. Supports glob patterns.
pub fn (mut ctx DataFrameContext) read_parquet(path string, opts ReadParquetOptions) !DataFrame {
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.union_by_name {
		args << 'union_by_name=true'
	}
	if opts.hive_partitioning {
		args << 'hive_partitioning=true'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_parquet('${path}'${arg_str})") or {
		return error('Failed to read Parquet "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ReadJsonOptions {
pub:
	format  string            // '' (auto) | 'array' | 'newline_delimited' | 'unstructured'
	columns map[string]string // explicit name -> SQL type overrides
}

// Reads a JSON file (local or remote) into a new DataFrame.
pub fn (mut ctx DataFrameContext) read_json(path string, opts ReadJsonOptions) !DataFrame {
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.format != '' {
		args << "format='${opts.format}'"
	}
	if opts.columns.len > 0 {
		mut pairs := []string{}
		for k, v in opts.columns {
			pairs << "'${k}': '${v}'"
		}
		args << 'columns={${pairs.join(', ')}}'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_json('${path}'${arg_str})") or {
		return error('Failed to read JSON "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ReadExcelOptions {
pub:
	sheet  string      // sheet name; '' = first sheet
	range  string      // cell range, e.g. 'A1:D100'; '' = full sheet
	header bool = true
}

// Reads an Excel (.xlsx) file into a new DataFrame using the DuckDB 'excel' extension.
pub fn (mut ctx DataFrameContext) read_excel(path string, opts ReadExcelOptions) !DataFrame {
	ctx.ensure_extension('excel')!
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.sheet != '' {
		args << "sheet='${opts.sheet}'"
	}
	if opts.range != '' {
		args << "range='${opts.range}'"
	}
	args << 'header=${opts.header}'
	arg_str := ', ' + args.join(', ')
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_xlsx('${path}'${arg_str})") or {
		return error('Failed to read Excel "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ToExcelOptions {
pub:
	header bool = true
	sheet  string = 'Sheet1'
}

// Exports the DataFrame to an Excel (.xlsx) file using the DuckDB 'excel' extension.
pub fn (df DataFrame) to_excel(path string, opts ToExcelOptions) ! {
	mut ctx := df.ctx
	ctx.ensure_extension('excel')!
	mut db := &df.ctx.db
	header_stmt := if opts.header { 'true' } else { 'false' }
	db.query("COPY (SELECT * FROM ${df.id}) TO '${path}' (FORMAT xlsx, HEADER ${header_stmt}, SHEET '${opts.sheet}')") or {
		return error('Failed to write Excel "${path}": ${err.msg()}')
	}
}

// Returns true if the path points at a remote/cloud location (needs the httpfs extension).
pub fn is_remote_path(path string) bool {
	prefixes := ['http://', 'https://', 's3://', 'gs://', 'az://', 'azure://', 'r2://']
	for p in prefixes {
		if path.starts_with(p) {
			return true
		}
	}
	return false
}

// Returns the DuckDB extension a path requires before it can be read, or '' if none.
// Remote paths require 'httpfs'; local .xlsx files require 'excel'.
pub fn scheme_for_path(path string) string {
	if is_remote_path(path) {
		return 'httpfs'
	}
	if path.to_lower().ends_with('.xlsx') {
		return 'excel'
	}
	return ''
}

// Returns the DataFrame as a Markdown table string
pub fn (df DataFrame) to_markdown() !string {
	mut db := &df.ctx.db
	_ := db.query('SELECT * FROM ${df.id}') or { return err }
	cols := db.columns.keys()
	rows := db.get_array()

	// header row
	mut lines := []string{}
	lines << '| ' + cols.join(' | ') + ' |'
	lines << '| ' + cols.map('---').join(' | ') + ' |'

	for row in rows {
		cells := cols.map((row[it] or { json2.Any('') }).str())
		lines << '| ' + cells.join(' | ') + ' |'
	}
	return lines.join('\n')
}