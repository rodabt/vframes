module vframes

pub fn (df DataFrame) head(n int, dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("select * from ${df.id} limit ${n}") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

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

pub fn (df DataFrame) info(dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM ${df.id})") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

pub fn (df DataFrame) describe(dconf DFConfig) Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT * FROM (SUMMARIZE SELECT * FROM ${df.id})") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

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

pub fn (df DataFrame) query(q string, dconf DFConfig) !Data {
	mut db := &df.ctx.db
	_ := db.query('SELECT ${q} FROM ${df.id}') or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}