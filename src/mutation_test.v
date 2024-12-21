module vframes

import x.json2

const data = 	[
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0) },
	{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0) }
]

fn df_init(d []map[string]json2.Any) DataFrame {
	mut ctx := vframes.init()
	df := ctx.read_records(d)
	return df
}

fn test__add_prefix() {
	mut df := df_init(data)
	result := Data([
		{"col_x": json2.Any(1), "col_y": json2.Any("a"), "col_z": json2.Any(-100.0) },
		{"col_x": json2.Any(3), "col_y": json2.Any("c"), "col_z": json2.Any(300.0) }
	])
	assert df.add_prefix('col').values().str() == result.str()
}

fn test__add_suffix() {
	mut df := df_init(data)
	result := Data([
		{"x_col": json2.Any(1), "y_col": json2.Any("a"), "z_col": json2.Any(-100.0) },
		{"x_col": json2.Any(3), "y_col": json2.Any("c"), "z_col": json2.Any(300.0) }
	])
	assert df.add_suffix('col').values().str() == result.str()
}

fn test__dropna() {
	tdata := [
		{"x_col": json2.Any(1), "y_col": json2.Any("a"), "z_col": json2.Any(-100.0) },
		{"x_col": json2.Any(3), "y_col": json2.null, "z_col": json2.Any(300.0) },
		{"x_col": json2.Any(5), "y_col": json2.Any("f"), "z_col": json2.null },
		{"x_col": json2.Any(json2.null), "y_col": json2.null, "z_col": json2.null }
	]
	df := df_init(tdata)
	// ANY
	result1 := Data([{"x_col": "1", "y_col": "a", "z_col": "-100" }])
	// ALL
	result2 := Data([
		{"x_col": "1", "y_col": "a", "z_col": "-100" },
		{"x_col": "3", "y_col": "", "z_col": "300" },
		{"x_col": "5", "y_col": "f", "z_col": "" }
	])
	result3 := Data([{"x_col": "1", "y_col": "a" }])
	assert df.dropna().values(as_string: true) == result1
	assert df.dropna(how: 'all').values(as_string: true) == result2
	assert df.dropna(subset: ['x_col','y_col']).values(as_string: true) == result3
}