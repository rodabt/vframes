import vframes
import x.json2

const data = [
	{'x': json2.Any(1), 'y': json2.Any('a'), 'z': json2.Any(100.0)},
	{'x': json2.Any(2), 'y': json2.Any('bb'), 'z': json2.Any(250.0)},
	{'x': json2.Any(3), 'y': json2.Any('ccc'), 'z': json2.Any(400.5)},
]

fn test__head_zero() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(0, vframes.DFConfig{})! as []map[string]json2.Any
	assert result.len == 0
}

fn test__head_two() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(2, vframes.DFConfig{ to_stdout: false })! as []map[string]json2.Any
	assert result.len == 2
}

fn test__head_hundred() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	// asking for more rows than exist returns all rows
	result := df.head(100, vframes.DFConfig{ to_stdout: false })! as []map[string]json2.Any
	assert result.len == 3
}

fn test__tail_zero() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(0, vframes.DFConfig{})! as []map[string]json2.Any
	assert result.len == 0
}

fn test__tail_one() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(1, vframes.DFConfig{ to_stdout: false })! as []map[string]json2.Any
	assert result.len == 1
	// last row has x=3
	assert result[0]['x'] or { json2.Any(0) }.int() == 3
}

fn test__tail_hundred() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	// asking for more rows than exist returns all rows
	result := df.tail(100, vframes.DFConfig{ to_stdout: false })! as []map[string]json2.Any
	assert result.len == 3
}
