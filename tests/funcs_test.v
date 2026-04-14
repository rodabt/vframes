import vframes
import x.json2

const data = [
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0)},
	{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0)}
]

fn test__add_integer() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.add[int](2)!
	shape := result.shape()!
	// same number of rows and columns as original
	assert shape[0] == 2
	assert shape[1] == 3
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// x was 1 and 3, now 3 and 5
	assert rows[0]['x'] or { json2.Any(0) }.int() == 3
	assert rows[1]['x'] or { json2.Any(0) }.int() == 5
}

fn test__add_decimal() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.add(1.2)!
	shape := result.shape()!
	assert shape[0] == 2
	assert shape[1] == 3
}

fn test__abs() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.abs()!
	shape := result.shape()!
	assert shape[0] == 2
	assert shape[1] == 3
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// z was -100.0 → abs → 100.0
	assert rows[0]['z'] or { json2.Any(0.0) }.f64() == 100.0
	assert rows[1]['z'] or { json2.Any(0.0) }.f64() == 300.0
}

fn test__max() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.max(vframes.FuncOptions{})!
	shape := result.shape()!
	// max collapses to 1 row, all columns kept
	assert shape[0] == 1
	assert shape[1] == 3
}

fn test__min() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data) or { panic(err) }
	result := df.min(vframes.FuncOptions{})!
	shape := result.shape()!
	assert shape[0] == 1
	assert shape[1] == 3
}

fn test__mean() {
	d := [
		{"x": json2.Any(10), "y": json2.Any(14)},
		{"x": json2.Any(4), "y": json2.Any(10)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.mean(vframes.FuncOptions{})!
	shape := result.shape()!
	// mean returns 1 row, only numeric cols (both cols here)
	assert shape[0] == 1
	assert shape[1] == 2
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// mean(10,4)=7, mean(14,10)=12
	assert rows[0]['x'] or { json2.Any(0.0) }.f64() == 7.0
	assert rows[0]['y'] or { json2.Any(0.0) }.f64() == 12.0
}

fn test__median() {
	d := [
		{"x": json2.Any(-10), "y": json2.Any(-50000)},
		{"x": json2.Any(-1), "y": json2.Any(0)},
		{"x": json2.Any(2), "y": json2.Any(-3)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.median(vframes.FuncOptions{})!
	shape := result.shape()!
	// median returns 1 row, only numeric cols
	assert shape[0] == 1
	assert shape[1] == 2
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// median of -10,-1,2 is -1; median of -50000,0,-3 is -3
	assert rows[0]['x'] or { json2.Any(0) }.int() == -1
	assert rows[0]['y'] or { json2.Any(0) }.int() == -3
}

fn test__sum() {
	d := [
		{"x": json2.Any(10), "y": json2.Any(14)},
		{"x": json2.Any(4), "y": json2.Any(10)},
		{"x": json2.Any(2), "y": json2.Any(15)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.sum(vframes.FuncOptions{})!
	shape := result.shape()!
	// sum returns 1 row, only numeric cols
	assert shape[0] == 1
	assert shape[1] == 2
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// sum(10,4,2)=16; sum(14,10,15)=39
	assert rows[0]['x'] or { json2.Any(0) }.int() == 16
	assert rows[0]['y'] or { json2.Any(0) }.int() == 39
}

fn test__pow() {
	d := [
		{"x": json2.Any(10)},
		{"x": json2.Any(4)},
		{"x": json2.Any(2)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.pow(2, vframes.FuncOptions{})!
	shape := result.shape()!
	assert shape[0] == 3
	assert shape[1] == 1
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// 10^2=100, 4^2=16, 2^2=4
	assert rows[0]['x'] or { json2.Any(0.0) }.f64() == 100.0
	assert rows[1]['x'] or { json2.Any(0.0) }.f64() == 16.0
	assert rows[2]['x'] or { json2.Any(0.0) }.f64() == 4.0
}

fn test__cumsum() {
	d := [
		{"x": json2.Any(1), "y": json2.Any(10)},
		{"x": json2.Any(2), "y": json2.Any(20)},
		{"x": json2.Any(3), "y": json2.Any(30)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.cumsum()!
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 1
	assert rows[1]['x'] or { json2.Any(0) }.int() == 3
	assert rows[2]['x'] or { json2.Any(0) }.int() == 6
}

fn test__cummax() {
	d := [
		{"x": json2.Any(3)},
		{"x": json2.Any(1)},
		{"x": json2.Any(4)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.cummax()!
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 3
	assert rows[1]['x'] or { json2.Any(0) }.int() == 3
	assert rows[2]['x'] or { json2.Any(0) }.int() == 4
}

fn test__cummin() {
	d := [
		{"x": json2.Any(3)},
		{"x": json2.Any(1)},
		{"x": json2.Any(4)}
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d) or { panic(err) }
	result := df.cummin()!
	rows := result.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 3
	assert rows[1]['x'] or { json2.Any(0) }.int() == 1
	assert rows[2]['x'] or { json2.Any(0) }.int() == 1
}
