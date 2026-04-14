import vframes
import x.json2

// Test basic functionality used in examples
// These tests verify that the operations complete without panicking

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

	types := df.dtypes()!
	assert types.len == 4

	df_subset := df.subset(['name', 'age'])
	_ = df_subset

	df_calc := df.add_column('doubled_age', 'age * 2')
	_ = df_calc

	df_deleted := df.delete_column('salary')
	_ = df_deleted

	df_sliced := df.slice(2, 3)
	_ = df_sliced

	df_prefixed := df.add_prefix('col_')
	_ = df_prefixed

	assert true
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
	})
	_ = df_grouped

	df_filtered := df.query('sales > 30000', vframes.DFConfig{})!
	_ = df_filtered

	df_sum := df.sum(vframes.FuncOptions{})!
	_ = df_sum

	df_mean := df.mean(vframes.FuncOptions{})!
	_ = df_mean

	assert true
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
	_ = df_isna

	df_notna := df.notna()!
	_ = df_notna

	df_dropna := df.dropna(vframes.DropOptions{how: 'any'})!
	_ = df_dropna

	df_ffill := df.ffill()!
	_ = df_ffill

	df_bfill := df.bfill()!
	_ = df_bfill

	num_data := [
		{'x': json2.Any(1), 'y': json2.Any(100.0)},
		{'x': json2.Any(json2.null), 'y': json2.Any(200.0)},
		{'x': json2.Any(3), 'y': json2.Any(json2.null)},
	]
	df_num := ctx.read_records(num_data)!
	df_filled := df_num.fillna(vframes.FillnaOptions{value: '0'})!
	_ = df_filled

	assert true
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
	_ = df_abs

	df_rounded := df.round(0)!
	_ = df_rounded

	df_clipped := df.clip(0.0, 150.0)!
	_ = df_clipped

	df_squared := df.pow(2, vframes.FuncOptions{})!
	_ = df_squared

	df_converted := df.astype({'value': 'int'})!
	_ = df_converted

	assert true
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
	_ = df_isin

	df_replaced := df_str.replace('A', 'Alpha')!
	_ = df_replaced

	data := [
		{'category': json2.Any('A'), 'value': json2.Any(100)},
		{'category': json2.Any('B'), 'value': json2.Any(200)},
	]
	df := ctx.read_records(data)!
	df_nunique := df.nunique()!
	_ = df_nunique

	assert true
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
	_ = df
	assert true
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
	_ = df_add

	df_sub := df.sub(2)!
	_ = df_sub

	df_mul := df.mul(2)!
	_ = df_mul

	df_div := df.div(2)!
	_ = df_div

	assert true
}

fn test_dataframe_info_and_describe() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice'), 'value': json2.Any(100.0)},
		{'id': json2.Any(2), 'name': json2.Any('Bob'), 'value': json2.Any(200.0)},
	]

	df := ctx.read_records(data)!

	info_result := df.info(vframes.DFConfig{to_stdout: false})!
	_ = info_result

	describe_result := df.describe(vframes.DFConfig{to_stdout: false})!
	_ = describe_result

	assert true
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

	head_result := df.head(3, vframes.DFConfig{to_stdout: false})!
	_ = head_result

	tail_result := df.tail(2, vframes.DFConfig{to_stdout: false})!
	_ = tail_result

	assert true
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
	_ = df_std

	df_var := df.var()!
	_ = df_var

	assert true
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
	_ = df_median

	assert true
}
