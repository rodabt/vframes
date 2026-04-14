import vframes
import x.json2

fn test_basic_dataframe_operations() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice'), 'age': json2.Any(30), 'salary': json2.Any(85000.0)},
		{'id': json2.Any(2), 'name': json2.Any('Bob'), 'age': json2.Any(25), 'salary': json2.Any(65000.0)},
		{'id': json2.Any(3), 'name': json2.Any('Carol'), 'age': json2.Any(35), 'salary': json2.Any(92000.0)},
	]

	df := ctx.read_records(data)!

	shape := df.shape()!
	assert shape[0] == 3
	assert shape[1] == 4

	cols := df.columns()!
	assert cols.len == 4
	assert 'name' in cols
	assert 'age' in cols

	types := df.dtypes()!
	assert types.len == 4

	df_subset := df.subset(['name', 'age'])!
	subset_shape := df_subset.shape()!
	assert subset_shape[1] == 2

	df_calc := df.add_column('doubled_age', 'age * 2')!
	calc_shape := df_calc.shape()!
	assert calc_shape[1] == 5

	df_deleted := df.delete_column('salary')!
	deleted_cols := df_deleted.columns()!
	assert 'salary' !in deleted_cols
	assert deleted_cols.len == 3

	df_sliced := df.slice(2, 3)!
	sliced_shape := df_sliced.shape()!
	assert sliced_shape[0] == 2

	df_prefixed := df.add_prefix('col_')!
	prefixed_cols := df_prefixed.columns()!
	assert 'col__id' in prefixed_cols || 'col_id' in prefixed_cols
}

fn test_grouping_and_aggregation() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'region': json2.Any('North'), 'sales': json2.Any(50000.0), 'quantity': json2.Any(50)},
		{'region': json2.Any('North'), 'sales': json2.Any(25000.0), 'quantity': json2.Any(100)},
		{'region': json2.Any('South'), 'sales': json2.Any(45000.0), 'quantity': json2.Any(45)},
	]

	df := ctx.read_records(data)!

	df_grouped := df.group_by(['region'], {
		'total_sales': 'sum(sales)',
		'avg_quantity': 'avg(quantity)'
	})!
	grouped_shape := df_grouped.shape()!
	assert grouped_shape[0] == 2  // 2 distinct regions
	grouped_cols := df_grouped.columns()!
	assert 'total_sales' in grouped_cols
	assert 'avg_quantity' in grouped_cols

	df_filtered := df.query('sales > 30000', vframes.DFConfig{})!
	filtered_shape := df_filtered.shape()!
	assert filtered_shape[0] == 2  // two rows with sales > 30000

	df_sum := df.sum(vframes.FuncOptions{})!
	sum_shape := df_sum.shape()!
	assert sum_shape[0] == 1  // sum collapses to 1 row

	df_mean := df.mean(vframes.FuncOptions{})!
	mean_shape := df_mean.shape()!
	assert mean_shape[0] == 1
}

fn test_missing_values_handling() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice'), 'age': json2.Any(30)},
		{'id': json2.Any(2), 'name': json2.Any('Bob'), 'age': json2.null},
		{'id': json2.Any(3), 'name': json2.Any('Carol'), 'age': json2.Any(35)},
	]

	df := ctx.read_records(data)!

	df_isna := df.isna()!
	isna_shape := df_isna.shape()!
	assert isna_shape[0] == 3
	assert isna_shape[1] == 3

	df_notna := df.notna()!
	notna_shape := df_notna.shape()!
	assert notna_shape[0] == 3

	df_dropna := df.dropna(vframes.DropOptions{how: 'any'})!
	dropna_shape := df_dropna.shape()!
	// row 2 has null age, so it's dropped
	assert dropna_shape[0] == 2

	df_ffill := df.ffill()!
	assert df_ffill.shape()![0] == 3

	df_bfill := df.bfill()!
	assert df_bfill.shape()![0] == 3

	num_data := [
		{'x': json2.Any(1), 'y': json2.Any(100.0)},
		{'x': json2.Any(json2.null), 'y': json2.Any(200.0)},
		{'x': json2.Any(3), 'y': json2.Any(json2.null)},
	]
	df_num := ctx.read_records(num_data)!
	df_filled := df_num.fillna(vframes.FillnaOptions{value: '0'})!
	assert df_filled.shape()![0] == 3
}

fn test_data_transformations() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'value': json2.Any(100.5), 'category': json2.Any('A')},
		{'value': json2.Any(-50.2), 'category': json2.Any('B')},
		{'value': json2.Any(75.0), 'category': json2.Any('A')},
	]

	df := ctx.read_records(data)!

	df_abs := df.abs()!
	abs_shape := df_abs.shape()!
	assert abs_shape[0] == 3

	df_rounded := df.round(0)!
	assert df_rounded.shape()![0] == 3

	df_clipped := df.clip(0.0, 150.0)!
	assert df_clipped.shape()![0] == 3

	df_squared := df.pow(2, vframes.FuncOptions{})!
	assert df_squared.shape()![0] == 3

	df_converted := df.astype({'value': 'int'})!
	converted_types := df_converted.dtypes()!
	assert converted_types['value'].to_lower() == 'integer'
}

fn test_value_operations() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	str_data := [
		{'cat1': json2.Any('A'), 'cat2': json2.Any('X')},
		{'cat1': json2.Any('B'), 'cat2': json2.Any('Y')},
		{'cat1': json2.Any('C'), 'cat2': json2.Any('Z')},
	]
	df_str := ctx.read_records(str_data)!
	df_isin := df_str.isin(['A', 'C'])!
	assert df_isin.shape()![0] == 3

	df_replaced := df_str.replace('A', 'Alpha')!
	assert df_replaced.shape()![0] == 3

	data := [
		{'category': json2.Any('A'), 'value': json2.Any(100)},
		{'category': json2.Any('B'), 'value': json2.Any(200)},
	]
	df := ctx.read_records(data)!
	df_nunique := df.nunique()!
	nunique_shape := df_nunique.shape()!
	assert nunique_shape[0] == 1  // nunique returns 1 row
}

fn test_extremes() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'value': json2.Any(100.0)},
		{'id': json2.Any(2), 'value': json2.Any(50.0)},
		{'id': json2.Any(3), 'value': json2.Any(200.0)},
	]

	df := ctx.read_records(data)!
	df_largest := df.nlargest(2)!
	assert df_largest.shape()![0] == 2

	df_smallest := df.nsmallest(2)!
	assert df_smallest.shape()![0] == 2
}

fn test_arithmetic_operations() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(10), 'y': json2.Any(100.0)},
		{'x': json2.Any(20), 'y': json2.Any(200.0)},
	]

	df := ctx.read_records(data)!

	df_add := df.add(5)!
	add_rows := df_add.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert add_rows[0]['x'] or { json2.Any(0) }.int() == 15

	df_sub := df.sub(2)!
	sub_rows := df_sub.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert sub_rows[0]['x'] or { json2.Any(0) }.int() == 8

	df_mul := df.mul(2)!
	mul_rows := df_mul.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert mul_rows[0]['x'] or { json2.Any(0) }.int() == 20

	df_div := df.div(2)!
	div_rows := df_div.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert div_rows[0]['x'] or { json2.Any(0.0) }.f64() == 5.0
}

fn test_dataframe_info_and_describe() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice'), 'value': json2.Any(100.0)},
		{'id': json2.Any(2), 'name': json2.Any('Bob'), 'value': json2.Any(200.0)},
	]

	df := ctx.read_records(data)!

	info_result := df.info(vframes.DFConfig{to_stdout: false})! as []map[string]json2.Any
	// info returns one row per column
	assert info_result.len == 3

	describe_result := df.describe(vframes.DFConfig{to_stdout: false})! as []map[string]json2.Any
	// describe returns one row per column
	assert describe_result.len == 3
}

fn test_head_and_tail() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
		{'id': json2.Any(3), 'name': json2.Any('Carol')},
		{'id': json2.Any(4), 'name': json2.Any('David')},
		{'id': json2.Any(5), 'name': json2.Any('Eve')},
	]

	df := ctx.read_records(data)!

	head_result := df.head(3, vframes.DFConfig{to_stdout: false})! as []map[string]json2.Any
	assert head_result.len == 3

	tail_result := df.tail(2, vframes.DFConfig{to_stdout: false})! as []map[string]json2.Any
	assert tail_result.len == 2
	// last two rows are David (4) and Eve (5)
	assert tail_result[1]['id'] or { json2.Any(0) }.int() == 5
}

fn test_std_and_var() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(10), 'y': json2.Any(100.0)},
		{'x': json2.Any(20), 'y': json2.Any(200.0)},
		{'x': json2.Any(30), 'y': json2.Any(300.0)},
	]

	df := ctx.read_records(data)!

	df_std := df.std()!
	std_shape := df_std.shape()!
	assert std_shape[0] == 1

	df_var := df.var()!
	var_shape := df_var.shape()!
	assert var_shape[0] == 1
}

fn test_median() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(10), 'y': json2.Any(100.0)},
		{'x': json2.Any(20), 'y': json2.Any(200.0)},
		{'x': json2.Any(30), 'y': json2.Any(300.0)},
	]

	df := ctx.read_records(data)!

	df_median := df.median(vframes.FuncOptions{})!
	median_shape := df_median.shape()!
	assert median_shape[0] == 1
	rows := df_median.values(vframes.ValuesParams{})! as []map[string]json2.Any
	// median of 10,20,30 is 20
	assert rows[0]['x'] or { json2.Any(0) }.int() == 20
}
