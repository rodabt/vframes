// Example 1: Basic Usage - Data Loading and Exploration
// This example demonstrates the fundamental operations in vframes:
// - Initializing a context
// - Loading data from various sources
// - Exploring the DataFrame structure
// - Basic column operations

import vframes
import x.json2

fn print_header(s string) {
	println('\n=== ${s} ===')
}

fn main() {
	// Initialize an in-memory context
	// You can also use a persisted database: vframes.init(location: 'data.db')
	mut ctx := vframes.init()
	defer { ctx.close() }

	print_header('Creating DataFrame from records')
	
	// Create sample data - a dataset of employees
	// Note: Use json2.Any for all values
	employees := [
		{'id': json2.Any(1), 'name': json2.Any('Alice Johnson'), 'department': json2.Any('Engineering'), 'salary': json2.Any(85000.0), 'years': json2.Any(5)},
		{'id': json2.Any(2), 'name': json2.Any('Bob Smith'), 'department': json2.Any('Sales'), 'salary': json2.Any(65000.0), 'years': json2.Any(3)},
		{'id': json2.Any(3), 'name': json2.Any('Carol White'), 'department': json2.Any('Engineering'), 'salary': json2.Any(92000.0), 'years': json2.Any(7)},
		{'id': json2.Any(4), 'name': json2.Any('David Brown'), 'department': json2.Any('Marketing'), 'salary': json2.Any(58000.0), 'years': json2.Any(2)},
		{'id': json2.Any(5), 'name': json2.Any('Eve Davis'), 'department': json2.Any('Sales'), 'salary': json2.Any(72000.0), 'years': json2.Any(4)},
		{'id': json2.Any(6), 'name': json2.Any('Frank Miller'), 'department': json2.Any('Engineering'), 'salary': json2.Any(105000.0), 'years': json2.Any(10)},
		{'id': json2.Any(7), 'name': json2.Any('Grace Wilson'), 'department': json2.Any('Marketing'), 'salary': json2.Any(61000.0), 'years': json2.Any(3)},
		{'id': json2.Any(8), 'name': json2.Any('Henry Taylor'), 'department': json2.Any('Sales'), 'salary': json2.Any(68000.0), 'years': json2.Any(2)},
	]
	
	// Load the data into a DataFrame
	df := ctx.read_records(employees)!
	println('Loaded ${df.shape()[0]} rows and ${df.shape()[1]} columns')

	print_header('DataFrame Shape and Structure')
	
	// Get basic information about the DataFrame
	shape := df.shape()
	println('Shape: ${shape[0]} rows, ${shape[1]} columns')
	println('Columns: ${df.columns()}')
	println('Data Types: ${df.dtypes()}')

	print_header('Viewing Data (First 5 rows)')
	
	// Display first 5 rows
	df.head(5, vframes.DFConfig{})
	
	print_header('Viewing Data (Last 3 rows)')
	
	// Display last 3 rows
	df.tail(3, vframes.DFConfig{})

	print_header('DataFrame Info and Statistics')
	
	// Show column information
	println('Column info:')
	df.info(vframes.DFConfig{})
	
	// Show summary statistics
	print_header('Summary Statistics')
	df.describe(vframes.DFConfig{})

	print_header('Column Operations')
	
	// Select a subset of columns
	println('\nSelecting subset of columns (name, department, salary):')
	df_subset := df.subset(['name', 'department', 'salary'])
	df_subset.head(3, vframes.DFConfig{})
	
	// Add a new calculated column
	println('\nAdding a calculated column (salary_per_year):')
	df_with_calc := df.add_column('salary_per_year', 'salary / years')
	df_with_calc.head(5, vframes.DFConfig{})
	
	// Add prefix to all columns
	println('\nAdding prefix "emp_" to all columns:')
	df_prefixed := df.add_prefix('emp_')
	println('Columns after prefix: ${df_prefixed.columns()}')
	df_prefixed.head(3, vframes.DFConfig{})
	
	// Delete a column
	println('\nDeleting the "years" column:')
	df_deleted := df.delete_column('years')
	println('Columns after deletion: ${df_deleted.columns()}')
	df_deleted.head(3, vframes.DFConfig{})

	print_header('Slicing Data')
	
	// Get rows 3-5 (inclusive)
	println('\nSlicing rows 3-5:')
	df_sliced := df.slice(3, 5)
	df_sliced.head(10, vframes.DFConfig{})

	print_header('Basic Usage Complete')
	println('This example demonstrated:')
	println('- Initializing a vframes context')
	println('- Loading data from records')
	println('- Exploring DataFrame structure (shape, columns, dtypes)')
	println('- Viewing data (head, tail)')
	println('- Getting info and statistics')
	println('- Selecting columns (subset)')
	println('- Adding calculated columns')
	println('- Adding prefixes/suffixes to columns')
	println('- Deleting columns')
	println('- Slicing rows')
}
