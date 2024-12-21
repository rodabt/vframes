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


fn test__add_integer() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(3), "y": json2.Any("a"), "z": json2.Any(-98.0) },
		{"x": json2.Any(5), "y": json2.Any("c"), "z": json2.Any(302.0) }
	])
	assert df.add[int](2).values().str() == result.str()
}

fn test__add_decimal() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(2.2), "y": json2.Any("a"), "z": json2.Any(-98.8) },
		{"x": json2.Any(4.2), "y": json2.Any("c"), "z": json2.Any(301.2) }
	])
	assert df.add(1.2).values().str() == result.str()
}

fn test__abs() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(100.0) },
		{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0) }
	])
	assert df.abs().values().str() == result.str()
}

fn test__max() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(3), "y": json2.Any("c"), "z": json2.Any(300.0) }
	])
	assert df.max().values().str() == result.str()
}

fn test__min() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(1), "y": json2.Any("a"), "z": json2.Any(-100.0) }
	])
	assert df.min().values().str() == result.str()
}

fn test__mean() {
	mut df := df_init(data)
	result := Data([
		{"x": json2.Any(2), "z": json2.Any(100.0) }
	])
	assert df.mean().values().str() == result.str()
}

fn test__median() {
	d := [
		{"x": json2.Any(-10.3),"y": json2.Any(-50000),"z": json2.Any('a')},
		{"x": json2.Any(-1),"y": json2.Any(0),"z": json2.Any('b')},
		{"x": json2.Any(2),"y": json2.Any(-3),"z": json2.Any('c')}
	]
	mut df := df_init(d)
	result := Data([
		{"x": json2.Any(-1), "y": json2.Any(-3) }
	])
	assert df.median().values().str() == result.str()
}

fn test__sum() {
	d := [
		{"x": json2.Any(10),"y": json2.Any(14),"z": json2.Any('a')},
		{"x": json2.Any(4),"y": json2.Any(10),"z": json2.Any('b')},
		{"x": json2.Any(2),"y": json2.Any(15),"z": json2.Any('c')}
	] 
	mut df := df_init(d)
	result := Data([
		{"x": json2.Any(16), "y": json2.Any(39) }
	])
	assert df.sum().values().str() == result.str()
}

fn test__pow() {
	d := [
		{"x": json2.Any(10),"y": json2.Any(14),"z": json2.Any('a')},
		{"x": json2.Any(4),"y": json2.Any(10),"z": json2.Any('b')},
		{"x": json2.Any(2),"y": json2.Any(15),"z": json2.Any('c')}
	] 
	mut df := df_init(d)
	result := Data([
		{"x": json2.Any(100),"y": json2.Any(196),"z": json2.Any('a')},
		{"x": json2.Any(16),"y": json2.Any(100),"z": json2.Any('b')},
		{"x": json2.Any(4),"y": json2.Any(225),"z": json2.Any('c')}
	])
	assert df.pow(2).values().str() == result.str()
}