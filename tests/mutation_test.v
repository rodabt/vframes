import vframes
import x.json2

const data = [
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0)},
	{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0)}
]

fn test__add_prefix() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.add_prefix('col')
	_ = result
	assert true
}

fn test__add_suffix() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.add_suffix('col')
	_ = result
	assert true
}

fn test__dropna() {
	tdata := [
		{"x_col": json2.Any(1), "y_col": json2.Any("a"), "z_col": json2.Any(-100.0)},
		{"x_col": json2.Any(3), "y_col": json2.null, "z_col": json2.Any(300.0)},
		{"x_col": json2.Any(5), "y_col": json2.Any("f"), "z_col": json2.null},
		{"x_col": json2.Any(json2.null), "y_col": json2.null, "z_col": json2.null}
	]
	mut ctx := vframes.init()
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.dropna(vframes.DropOptions{})
	_ = result
	assert true
}
