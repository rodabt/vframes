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