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

fn test_eq_on_views() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'a': json2.Any(1), 'b': json2.Any(1)},
		{'a': json2.Any(2), 'b': json2.Any(9)},
	]
	df := ctx.read_records(data)!
	// compare a view against a view (both are transformations); previously this
	// errored because views do not expose rowid. eq now uses POSITIONAL JOIN.
	// NOTE: boolean columns are read back as '' by the vduckdb binding, so we
	// assert structure (view, row/col count), as the other boolean ops do.
	left := df.add(0)!
	right := df.add(0)!
	res := left.eq(right)!
	assert res.object_type()! == 'VIEW'
	assert res.shape()![0] == 2
	assert res.columns()! == ['a', 'b']
}

fn test_mutation_ops_are_views() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'a': json2.Any(1), 'b': json2.Any(10)},
		{'a': json2.Any(2), 'b': json2.Any(20)},
		{'a': json2.Any(3), 'b': json2.Any(30)},
	]
	df := ctx.read_records(data)!

	filtered := df.filter('a > 1')!
	assert filtered.object_type()! == 'VIEW'
	assert filtered.shape()![0] == 2

	sub := df.subset(['a'])!
	assert sub.object_type()! == 'VIEW'
	assert sub.columns()!.len == 1

	sorted := df.sort_values(['b'], ascending: false)!
	assert sorted.object_type()! == 'VIEW'
}

fn test_bfill_on_view() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(1), 'y': json2.Any(json2.null)},
		{'x': json2.Any(json2.null), 'y': json2.Any(2)},
	]
	df := ctx.read_records(data)!
	// bfill applied to a view (a transformed frame), exercising the rowid rewrite
	v := df.add(0)!
	filled := v.bfill()!
	assert filled.shape()![0] == 2
}
