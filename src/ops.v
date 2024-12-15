module vframes

// Calculates the absolute value of each element
/* pub fn (df DataFrame) abs() DataFrame {
	if n <= 0 {
		return Data([]map[string]json2.Any{})
	}
	mut db := &df.ctx.db
	_ := db.query("select * from ${df.id} limit ${n}") or { panic(err) }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return Data(db.get_array())
} */