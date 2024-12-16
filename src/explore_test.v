module vframes

import x.json2

const data = 	[
	{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(100.0) },
	{"x": json2.Any(2), "y": json2.Any("bb"), "z": json2.Any(250.0) },
	{"x": json2.Any(3), "y": json2.Any("ccc"), "z": json2.Any(400.5) }
]

fn df_init(d []map[string]json2.Any) DataFrame {
	mut ctx := vframes.init()
	df := ctx.read_records(d)
	return df
}

fn test__head_zero() {
	mut df := df_init(data)
	assert df.head(0) == Data([]map[string]json2.Any{}) 
}

fn test__head_two() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(100.0) },
		{"x": json2.Any(2), "y": json2.Any("bb"), "z": json2.Any(250.0) }
	])
	assert df.head(2, to_stdout: false).str() == result.str()
}

fn test__head_hundred() {
	mut df := df_init(data)
	assert df.head(100, to_stdout: false).str() == vframes.Data(data).str()
}

fn test__tail_zero() {
	mut df := df_init(data)
	assert df.tail(0) == Data([]map[string]json2.Any{}) 
}

fn test__tail_one() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(3), "y": json2.Any("ccc"), "z": json2.Any(400.5) }
	])
	assert df.tail(1, to_stdout: false).str() == result.str()
}

fn test__tail_hundred() {
	mut df := df_init(data)
	assert df.tail(100, to_stdout: false).str() == vframes.Data(data).str()
}