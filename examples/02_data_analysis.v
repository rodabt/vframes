// Example 2: Data Analysis - Grouping, Aggregations, and Transformations
// This example demonstrates advanced data analysis capabilities:
// - Grouping and aggregation
// - Mathematical operations on DataFrames
// - Statistical functions
// - Complex queries

import vframes
import x.json2

fn print_header(s string) {
	println('\n=== ${s} ===')
}

fn main() {
	// Initialize context
	mut ctx := vframes.init()
	defer { ctx.close() }

	print_header('Creating Sample Sales Data')
	
	// Create sample sales data
	sales_data := [
		{'region': json2.Any('North'), 'product': json2.Any('Laptop'), 'sales': json2.Any(50000.0), 'quantity': json2.Any(50), 'month': json2.Any(1)},
		{'region': json2.Any('North'), 'product': json2.Any('Monitor'), 'sales': json2.Any(25000.0), 'quantity': json2.Any(100), 'month': json2.Any(1)},
		{'region': json2.Any('South'), 'product': json2.Any('Laptop'), 'sales': json2.Any(45000.0), 'quantity': json2.Any(45), 'month': json2.Any(1)},
		{'region': json2.Any('South'), 'product': json2.Any('Monitor'), 'sales': json2.Any(20000.0), 'quantity': json2.Any(80), 'month': json2.Any(1)},
		{'region': json2.Any('North'), 'product': json2.Any('Laptop'), 'sales': json2.Any(55000.0), 'quantity': json2.Any(55), 'month': json2.Any(2)},
		{'region': json2.Any('North'), 'product': json2.Any('Monitor'), 'sales': json2.Any(28000.0), 'quantity': json2.Any(110), 'month': json2.Any(2)},
		{'region': json2.Any('South'), 'product': json2.Any('Laptop'), 'sales': json2.Any(48000.0), 'quantity': json2.Any(48), 'month': json2.Any(2)},
		{'region': json2.Any('South'), 'product': json2.Any('Monitor'), 'sales': json2.Any(22000.0), 'quantity': json2.Any(90), 'month': json2.Any(2)},
		{'region': json2.Any('East'), 'product': json2.Any('Laptop'), 'sales': json2.Any(60000.0), 'quantity': json2.Any(60), 'month': json2.Any(2)},
		{'region': json2.Any('East'), 'product': json2.Any('Monitor'), 'sales': json2.Any(30000.0), 'quantity': json2.Any(120), 'month': json2.Any(2)},
	]
	
	df := ctx.read_records(sales_data)!
	println('Loaded ${df.shape()[0]} rows')
	df.head(10, vframes.DFConfig{})

	print_header('Grouping and Aggregation')
	
	// Group by region and calculate aggregates
	println('\nGroup by region with aggregations:')
	df_by_region := df.group_by(['region'], {
		'total_sales': 'sum(sales)',
		'avg_sales': 'avg(sales)',
		'total_quantity': 'sum(quantity)',
		'count': 'count(*)'
	})
	df_by_region.head(10, vframes.DFConfig{})
	
	// Group by multiple columns
	println('\nGroup by region and product:')
	df_by_region_product := df.group_by(['region', 'product'], {
		'total_sales': 'sum(sales)',
		'avg_quantity': 'avg(quantity)'
	})
	df_by_region_product.head(10, vframes.DFConfig{})

	print_header('Mathematical Operations')
	
	// Add calculated columns
	println('\nAdding price_per_unit column (sales / quantity):')
	df_with_price := df.add_column('price_per_unit', 'sales / quantity')
	df_with_price.head(5, vframes.DFConfig{})
	
	// Perform arithmetic operations on the entire DataFrame
	println('\nAdding 1000 to all numeric values:')
	df_plus_1000 := df.add(1000)!
	df_plus_1000.head(3, vframes.DFConfig{})
	
	println('\nMultiplying sales by 1.1 (10%% increase):')
	// First select only numeric columns, then multiply
	df_increased := df.add_column('sales_increased', 'sales * 1.1')
	df_increased.head(5, vframes.DFConfig{})

	print_header('Statistical Functions')
	
	// Calculate aggregates across all rows
	println('\nSum of all numeric columns:')
	df_sum := df.sum(vframes.FuncOptions{})!
	df_sum.head(5, vframes.DFConfig{})
	
	println('\nMean of all numeric columns:')
	df_mean := df.mean(vframes.FuncOptions{})!
	df_mean.head(5, vframes.DFConfig{})
	
	println('\nStandard deviation:')
	df_std := df.std()!
	df_std.head(5, vframes.DFConfig{})
	
	println('\nCount of non-null values:')
	df_count := df.count()!
	df_count.head(5, vframes.DFConfig{})

	print_header('Filtering with Queries')
	
	// Use SQL-like queries for complex filtering
	println('\nFilter: sales > 40000')
	df_filtered := df.query('sales > 40000', vframes.DFConfig{})!
	df_filtered.head(10, vframes.DFConfig{})
	
	println('\nFilter: region = "North" AND month = 2')
	df_filtered2 := df.query('region = \'North\' AND month = 2', vframes.DFConfig{})!
	df_filtered2.head(10, vframes.DFConfig{})
	
	println('\nSelect specific columns with WHERE:')
	df_selected := df.query('region, product, sales WHERE sales > 30000', vframes.DFConfig{})!
	df_selected.head(10, vframes.DFConfig{})

	print_header('Finding Extremes')
	
	// Get top N rows by numeric columns
	println('\nTop 3 sales records:')
	df_top3 := df.nlargest(3)!
	df_top3.head(10, vframes.DFConfig{})
	
	println('\nBottom 3 sales records:')
	df_bottom3 := df.nsmallest(3)!
	df_bottom3.head(10, vframes.DFConfig{})

	print_header('Value Analysis')
	
	// Count unique values
	println('\nCount of unique values in each column:')
	df_nunique := df.nunique()!
	df_nunique.head(5, vframes.DFConfig{})
	
	// Get value counts for the first column (region)
	println('\nValue counts for first column:')
	df_value_counts := df.value_counts()!
	df_value_counts.head(10, vframes.DFConfig{})

	print_header('Data Analysis Complete')
	println('This example demonstrated:')
	println('- Group by operations with custom aggregations')
	println('- Multi-column grouping')
	println('- Mathematical operations (add, multiply)')
	println('- Calculated columns with expressions')
	println('- Statistical functions (sum, mean, std, count)')
	println('- SQL-like queries for filtering')
	println('- Finding top/bottom N records')
	println('- Counting unique values')
	println('- Value frequency counts')
}
