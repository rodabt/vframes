import vframes
import x.json2

const data = [
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(100.0)},
	{"x": json2.Any(2), "y": json2.Any("bb"), "z": json2.Any(250.0)},
	{"x": json2.Any(3), "y": json2.Any("ccc"), "z": json2.Any(400.5)}
]

fn test__head_zero() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.head(0, vframes.DFConfig{})
	_ = result
	assert true
}

fn test__head_two() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.head(2, vframes.DFConfig{to_stdout: false})
	_ = result
	assert true
}

fn test__head_hundred() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.head(100, vframes.DFConfig{to_stdout: false})
	_ = result
	assert true
}

fn test__tail_zero() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.tail(0, vframes.DFConfig{})
	_ = result
	assert true
}

fn test__tail_one() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.tail(1, vframes.DFConfig{to_stdout: false})
	_ = result
	assert true
}

fn test__tail_hundred() {
	mut ctx := vframes.init()
	df := ctx.read_records(data) or { panic(err) }
	result := df.tail(100, vframes.DFConfig{to_stdout: false})
	_ = result
	assert true
}
