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
