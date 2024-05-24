module vframes

// Shows first `n` records from DataFrame. Use `to_stdout: false` to return the data as `[]map[string]json2.Any` instead of the console
// Example:
// ```v
// df.head(10) 									// Prints the first 10 records to console
// new_df := df.head(10, to_stdout: false)  	// Assigns the result as []map[string]json2.Any to new_df
// ```
pub fn (df DataFrame) head(n int, dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("select * from ${df.id} limit ${n}") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Same as `head`, but for last `n`records
pub fn (df DataFrame) tail(n int, dconf DFConfig) Data {
	mut db := &df.ctx.db
	q := "
	WITH _base as (
		SELECT row_number() OVER() as _row_num,* 
		FROM ${df.id}
	) SELECT * EXCLUDE(_row_num) FROM (SELECT * FROM _base ORDER BY _row_num DESC limit ${n}) ORDER BY _row_num ASC
	"
	_ := db.query(q) or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array() 
}

// Shows DataFrame columns names and data types
pub fn (df DataFrame) info(dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM ${df.id})") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Shows columns basic statistics (nulls, max, min, etc.)
pub fn (df DataFrame) describe(dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT * FROM (SUMMARIZE SELECT * FROM ${df.id})") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Returns the number of rows and columns of the DataFrame
pub fn (df DataFrame) shape() []int {
	mut db := &df.ctx.db
	_ := db.query('SELECT COUNT(*) as rows FROM ${df.id}') or { panic(err) }
	res_rows := db.get_array()
	num_rows := (res_rows[0]["rows"] or {0}).int()

	_ := db.query('SELECT COUNT(DISTINCT column_name) as cols FROM (SUMMARIZE SELECT * FROM ${df.id})') or { panic(err) }
	res_cols := db.get_array()
	num_cols := (res_cols[0]["cols"] or {0}).int()
	
	return [num_rows,num_cols]
}

// Allows you to use a valid sql expression with the DataFrame. It returns a Data Result.
// Examples: `df.query("value*2 as new_value, lower(name) as lowercase_name")`
pub fn (df DataFrame) query(q string, dconf DFConfig) !Data {
	mut db := &df.ctx.db
	_ := db.query('SELECT ${q} FROM ${df.id}') or { 
		eprintln("Invalid query syntax: ${err.msg()}")
		exit(1)	
	}
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}