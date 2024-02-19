module main

import vduckdb
import rand
import os
import x.json2


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

@[noinit]
struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
	ctx					DataFrameContext
pub mut:
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

/***
 
 DATA INPUT AND OUTPUT

***/

fn (mut ctx DataFrameContext) read_auto(filename string) DataFrame {
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


fn (mut ctx DataFrameContext) read_records(dict []map[string]json2.Any) DataFrame {
	id := 'tbl_${rand.ulid()}'
	tmp_dict := dict.map(it.str())
	tmp_file := os.join_path_single(os.temp_dir(),'tmp_${rand.ulid()}.json')
	os.write_file(tmp_file, tmp_dict.join_lines()) or { panic(err) }
	_ := ctx.db.query("create table ${id} as select * from '${tmp_file}'") or { panic(err) }
	return DataFrame{
		ctx: ctx
	}
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
	WITH _base as (
		SELECT row_number() OVER() as _row_num,* 
		FROM ${df.id}
	) SELECT * EXCLUDE(_row_num) FROM (SELECT * FROM _base ORDER BY _row_num DESC limit ${n}) ORDER BY _row_num ASC
	"
	_ := db.query(q) or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) info() string {
	mut db := &df.ctx.db
	_ := db.query("SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM ${df.id})") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) describe() string {
	mut db := &df.ctx.db
	_ := db.query("SELECT * FROM (SUMMARIZE SELECT * FROM ${df.id})") or { panic(err) }
	return db.print_table(max_rows: df.display_max_rows, mode: df.display_mode)
}

fn (df DataFrame) shape() []int {
	mut db := &df.ctx.db
	_ := db.query('SELECT COUNT(*) as rows FROM ${df.id}') or { panic(err) }
	res_rows := db.get_array()
	num_rows := (res_rows[0]["rows"] or {0}).int()

	_ := db.query('SELECT COUNT(DISTINCT column_name) as cols FROM (SUMMARIZE SELECT * FROM ${df.id})') or { panic(err) }
	res_cols := db.get_array()
	num_cols := (res_cols[0]["cols"] or {0}).int()
	
	return [num_rows,num_cols]
}

/***
 
 DATA MANIPULATION

***/


fn (df DataFrame) delete_column(col string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * exclude(${col}) from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

fn (df DataFrame) add_column(col string, exp string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select *, ${exp} as ${col} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

fn (df DataFrame) subset(cols []string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

fn (df DataFrame) slice(start int, end int) DataFrame {
	id := 'tbl_${rand.ulid()}'
	offset := start - 1
	limit := end - start + 1
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * from ${df.id} limit ${limit} offset ${offset}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

fn (df DataFrame) group_by(dimensions []string, metrics map[string]string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut sets := []string{}
	for k,v in metrics {
		sets << '${v} as ${k}'
	}
	_ := db.query("create table ${id} as select ${dimensions.join(',')}, ${sets.join(',')} from ${df.id} group by ${dimensions.join(',')}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
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
	df := ctx.read_auto('tmp/people-500000.csv')
	p("First 5 records:", df.head(5))
	p("Last 5 records:", df.tail(5))
	p("DataFrame info:", df.info())
	p("DataFrame shape:", df.shape().str())
	// // p("DataFrame size:", df.size)
	p("Describe:", df.describe())
	
	df2 := df
		.add_column('new_col', 'Index*5')
		.subset(['Email','Phone','new_col'])
	p("Create new DF with new column 'new_col'=Index*5, and select a subset of columns (Email, Phone, new_col):", df2.head(10))

	df3 := df2.delete_column('Email')
	p("Delete Email from new DF:", df3.head(10))

	df4 := ctx.read_auto('tmp/titanic.parquet')
	p("Load parquet (Titanic):", df4.head(10))
	p("Describe:", df4.describe())

	df5 := df4.group_by(['Sex'],{"age_avg": "avg(Age)", "avg_fare": "avg(Fare)"})
	p("Average of Age and Fare by Sex:", df5.head(10))

	df6 := df.slice(2,3)
	p("Slice(2,3) of first DF:", df6.head(10))

	ctx.close()
}

