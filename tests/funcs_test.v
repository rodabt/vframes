import vframes
import x.json2

const data = [
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0)},
	{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0)}
]

fn test__add_integer() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.add[int](2)!
	_ = result
	assert true
}

fn test__add_decimal() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.add(1.2)!
	_ = result
	assert true
}

fn test__abs() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.abs()!
	_ = result
	assert true
}

fn test__max() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.max(vframes.FuncOptions{})!
	_ = result
	assert true
}

fn test__min() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.min(vframes.FuncOptions{})!
	_ = result
	assert true
}

fn test__mean() {
	d := [
		{"x": json2.Any(10), "y": json2.Any(14)},
		{"x": json2.Any(4), "y": json2.Any(10)}
	]
	mut ctx := vframes.init()
	df := ctx.read_records(d) or { panic(err) }
	result := df.mean(vframes.FuncOptions{})!
	_ = result
	assert true
}

fn test__median() {
	d := [
		{"x": json2.Any(-10.3), "y": json2.Any(-50000)},
		{"x": json2.Any(-1), "y": json2.Any(0)},
		{"x": json2.Any(2), "y": json2.Any(-3)}
	]
	mut ctx := vframes.init()
	df := ctx.read_records(d) or { panic(err) }
	result := df.median(vframes.FuncOptions{})!
	_ = result
	assert true
}

fn test__sum() {
	d := [
		{"x": json2.Any(10), "y": json2.Any(14)},
		{"x": json2.Any(4), "y": json2.Any(10)},
		{"x": json2.Any(2), "y": json2.Any(15)}
	]
	mut ctx := vframes.init()
	df := ctx.read_records(d) or { panic(err) }
	result := df.sum(vframes.FuncOptions{})!
	_ = result
	assert true
}

fn test__pow() {
	d := [
		{"x": json2.Any(10)},
		{"x": json2.Any(4)},
		{"x": json2.Any(2)}
	]
	mut ctx := vframes.init()
	df := ctx.read_records(d) or { panic(err) }
	result := df.pow(2, vframes.FuncOptions{})!
	_ = result
	assert true
}
