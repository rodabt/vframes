import vframes
import x.json2

fn test_base_read_is_table() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
	]
	df := ctx.read_records(data)!
	assert df.object_type()! == 'BASE TABLE'
	assert df.is_lazy()! == false
	assert df.chain_depth() == 0
}

fn test_collect_materializes() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
	]
	df := ctx.read_records(data)!
	c := df.collect()!
	assert c.object_type()! == 'BASE TABLE'
	assert c.chain_depth() == 0
	assert c.shape()![0] == 2

	// copy() is an alias for collect()
	cp := df.copy()!
	assert cp.shape()![0] == 2
}

fn test_transformation_is_view() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(10), 'y': json2.Any(100.0)},
		{'x': json2.Any(20), 'y': json2.Any(200.0)},
	]
	df := ctx.read_records(data)!

	added := df.add(5)!
	assert added.object_type()! == 'VIEW'
	assert added.is_lazy()! == true
	assert added.chain_depth() == 1

	// chaining increases depth
	chained := added.mul(2)!
	assert chained.chain_depth() == 2

	// result is still correct
	rows := chained.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 30 // (10+5)*2
}
