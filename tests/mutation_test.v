import vframes
import x.json2

const data = [
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0)},
	{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0)}
]

fn test__add_prefix() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.add_prefix('col')!
	cols := result.columns()!
	assert 'col_x' in cols
	assert 'col_y' in cols
	assert 'col_z' in cols
	assert 'x' !in cols
}

fn test__add_suffix() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.add_suffix('col')!
	cols := result.columns()!
	assert 'x_col' in cols
	assert 'y_col' in cols
	assert 'z_col' in cols
	assert 'x' !in cols
}

fn test__dropna() {
	tdata := [
		{"x_col": json2.Any(1), "y_col": json2.Any("a"), "z_col": json2.Any(-100.0)},
		{"x_col": json2.Any(3), "y_col": json2.null, "z_col": json2.Any(300.0)},
		{"x_col": json2.Any(5), "y_col": json2.Any("f"), "z_col": json2.null},
		{"x_col": json2.Any(json2.null), "y_col": json2.null, "z_col": json2.null}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.dropna(vframes.DropOptions{})!
	shape := result.shape()!
	// only row 1 has no nulls
	assert shape[0] == 1
}

fn test__rename() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(3), "y": json2.Any("c")}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.rename({'x': 'x_new', 'y': 'y_new'})!
	cols := result.columns()!
	assert 'x_new' in cols
	assert 'y_new' in cols
	assert 'x' !in cols
}

fn test__drop_duplicates() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(3), "y": json2.Any("c")}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.drop_duplicates([]string{})!
	shape := result.shape()!
	assert shape[0] == 2
}

fn test__sample() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(2), "y": json2.Any("b")},
		{"x": json2.Any(3), "y": json2.Any("c")},
		{"x": json2.Any(4), "y": json2.Any("d")},
		{"x": json2.Any(5), "y": json2.Any("e")}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.sample(n: 2, replace: false)!
	shape := result.shape()!
	assert shape[0] == 2
}

fn test__merge() {
	left_data := [
		{"key": json2.Any("a"), "val": json2.Any(1)},
		{"key": json2.Any("b"), "val": json2.Any(2)}
	]
	right_data := [
		{"key": json2.Any("a"), "val2": json2.Any(10)},
		{"key": json2.Any("c"), "val2": json2.Any(30)}
	]
	mut ctx := vframes.init()!
	left_df := ctx.read_records(left_data) or { panic(err) }
	right_df := ctx.read_records(right_data) or { panic(err) }
	result := left_df.merge(right_df, on: 'key', how: 'inner')!
	shape := result.shape()!
	assert shape[0] == 1
}

fn test__join() {
	left_data := [
		{"key": json2.Any("a"), "val": json2.Any(1)},
		{"key": json2.Any("b"), "val": json2.Any(2)}
	]
	right_data := [
		{"key": json2.Any("a"), "val2": json2.Any(10)},
		{"key": json2.Any("c"), "val2": json2.Any(30)}
	]
	mut ctx := vframes.init()!
	left_df := ctx.read_records(left_data) or { panic(err) }
	right_df := ctx.read_records(right_data) or { panic(err) }
	result := left_df.join(right_df, on: 'key', how: 'left')!
	shape := result.shape()!
	assert shape[0] == 2
}

fn test__concat() {
	data1 := [
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(2), "y": json2.Any("b")}
	]
	data2 := [
		{"x": json2.Any(3), "y": json2.Any("c")},
		{"x": json2.Any(4), "y": json2.Any("d")}
	]
	mut ctx := vframes.init()!
	df1 := ctx.read_records(data1) or { panic(err) }
	df2 := ctx.read_records(data2) or { panic(err) }
	result := vframes.concat([df1, df2])!
	shape := result.shape()!
	assert shape[0] == 4
}

fn test__pivot() {
	tdata := [
		{"date": json2.Any("2020-01-01"), "variable": json2.Any("temp"), "value": json2.Any(20)},
		{"date": json2.Any("2020-01-01"), "variable": json2.Any("humidity"), "value": json2.Any(60)},
		{"date": json2.Any("2020-01-02"), "variable": json2.Any("temp"), "value": json2.Any(22)},
		{"date": json2.Any("2020-01-02"), "variable": json2.Any("humidity"), "value": json2.Any(65)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.pivot(index: 'date', columns: 'variable', values: 'value')!
	cols := result.columns()!
	assert 'temp_value' in cols
	assert 'humidity_value' in cols
}

fn test__pivot_table() {
	tdata := [
		{"date": json2.Any("2020-01-01"), "variable": json2.Any("temp"), "value": json2.Any(20)},
		{"date": json2.Any("2020-01-01"), "variable": json2.Any("temp"), "value": json2.Any(22)},
		{"date": json2.Any("2020-01-02"), "variable": json2.Any("temp"), "value": json2.Any(25)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.pivot_table(index: 'date', columns: 'variable', values: 'value', aggfunc: 'mean')!
	shape := result.shape()!
	// 2 distinct dates → 2 rows
	assert shape[0] == 2
	cols := result.columns()!
	assert 'temp_value' in cols
}

fn test__melt() {
	tdata := [
		{"date": json2.Any("2020-01-01"), "temp": json2.Any(20), "humidity": json2.Any(60)},
		{"date": json2.Any("2020-01-02"), "temp": json2.Any(22), "humidity": json2.Any(65)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.melt(id_vars: ['date'], value_vars: ['temp', 'humidity'])!
	shape := result.shape()!
	assert shape[0] == 4
}

fn test__assign() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any(2)},
		{"x": json2.Any(3), "y": json2.Any(4)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.assign('z', 'x + y')!
	cols := result.columns()!
	assert 'z' in cols
}

fn test__sort_values_asc() {
	tdata := [
		{"x": json2.Any(3), "y": json2.Any("c")},
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(2), "y": json2.Any("b")}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.sort_values(['x'], vframes.SortOptions{ ascending: true })!
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 1
	assert rows[1]['x'] or { json2.Any(0) }.int() == 2
	assert rows[2]['x'] or { json2.Any(0) }.int() == 3
}

fn test__sort_values_desc() {
	tdata := [
		{"x": json2.Any(3), "y": json2.Any("c")},
		{"x": json2.Any(1), "y": json2.Any("a")},
		{"x": json2.Any(2), "y": json2.Any("b")}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.sort_values(['x'], vframes.SortOptions{ ascending: false })!
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 3
	assert rows[2]['x'] or { json2.Any(0) }.int() == 1
}

fn test__filter() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any(10)},
		{"x": json2.Any(2), "y": json2.Any(20)},
		{"x": json2.Any(3), "y": json2.Any(30)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.filter('x > 1')!
	shape := result.shape()!
	assert shape[0] == 2
}

fn test__drop() {
	tdata := [
		{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(100)},
		{"x": json2.Any(2), "y": json2.Any("b"), "z": json2.Any(200)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata) or { panic(err) }
	result := df.drop(['y', 'z'])!
	cols := result.columns()!
	assert 'x' in cols
	assert 'y' !in cols
	assert 'z' !in cols
}
