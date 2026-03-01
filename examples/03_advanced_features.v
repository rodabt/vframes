// Example 3: Advanced Features - Missing Values, Data Export, and Complex Operations
// This example demonstrates advanced vframes capabilities:
// - Handling missing values
// - Data transformation pipelines
// - Exporting data to various formats
// - Working with external data files

import vframes
import x.json2
import os

fn print_header(s string) {
	println('\n=== ${s} ===')
}

fn main() {
	// Initialize context
	mut ctx := vframes.init()
	defer { ctx.close() }

	print_header('Creating Data with Missing Values')
	
	// Create sample data with missing values
	// Note: json2.null represents null/missing values
	data_with_nulls := [
		{'id': json2.Any(1), 'name': json2.Any('Alice'), 'age': json2.Any(30), 'salary': json2.Any(85000.0), 'city': json2.Any('NYC')},
		{'id': json2.Any(2), 'name': json2.Any('Bob'), 'age': json2.null, 'salary': json2.Any(65000.0), 'city': json2.Any('LA')},
		{'id': json2.Any(3), 'name': json2.Any('Carol'), 'age': json2.Any(35), 'salary': json2.null, 'city': json2.Any('NYC')},
		{'id': json2.Any(4), 'name': json2.Any('David'), 'age': json2.Any(28), 'salary': json2.Any(72000.0), 'city': json2.null},
		{'id': json2.Any(5), 'name': json2.Any('Eve'), 'age': json2.null, 'salary': json2.null, 'city': json2.Any('Chicago')},
		{'id': json2.Any(6), 'name': json2.Any('Frank'), 'age': json2.Any(45), 'salary': json2.Any(95000.0), 'city': json2.Any('LA')},
	]
	
	df := ctx.read_records(data_with_nulls)!
	println('Original data with missing values:')
	df.head(10, vframes.DFConfig{})

	print_header('Detecting Missing Values')
	
	// Check which values are null
	println('\nBoolean mask of missing values:')
	df_isna := df.isna()!
	df_isna.head(10, vframes.DFConfig{})
	
	// Check which values are not null
	println('\nBoolean mask of non-missing values:')
	df_notna := df.notna()!
	df_notna.head(10, vframes.DFConfig{})

	print_header('Handling Missing Values')
	
	// Drop rows with any NA values
	println('\nDrop rows with any NA values:')
	df_dropna := df.dropna(vframes.DropOptions{how: 'any'})
	df_dropna.head(10, vframes.DFConfig{})
	println('Shape after dropna: ${df_dropna.shape()}')
	
	// Fill NA with a specific value
	println('\nFill NA with default values:')
	df_filled := df.fillna(vframes.FillnaOptions{value: '0'})!
	df_filled.head(10, vframes.DFConfig{})
	
	// Forward fill (use previous non-null value)
	println('\nForward fill NA values:')
	df_ffill := df.ffill()!
	df_ffill.head(10, vframes.DFConfig{})
	
	// Backward fill (use next non-null value)
	println('\nBackward fill NA values:')
	df_bfill := df.bfill()!
	df_bfill.head(10, vframes.DFConfig{})

	print_header('Data Transformations')
	
	// Create a clean dataset for transformations
	clean_data := [
		{'id': json2.Any(1), 'value': json2.Any(100.5), 'category': json2.Any('A')},
		{'id': json2.Any(2), 'value': json2.Any(-50.2), 'category': json2.Any('B')},
		{'id': json2.Any(3), 'value': json2.Any(75.0), 'category': json2.Any('A')},
		{'id': json2.Any(4), 'value': json2.Any(-25.8), 'category': json2.Any('B')},
		{'id': json2.Any(5), 'value': json2.Any(200.0), 'category': json2.Any('A')},
	]
	
	df_clean := ctx.read_records(clean_data)!
	println('Clean data for transformations:')
	df_clean.head(10, vframes.DFConfig{})
	
	// Apply absolute value
	println('\nAbsolute values:')
	df_abs := df_clean.abs()!
	df_abs.head(10, vframes.DFConfig{})
	
	// Round values
	println('\nRounded to 0 decimal places:')
	df_rounded := df_clean.round(0)!
	df_rounded.head(10, vframes.DFConfig{})
	
	// Clip values to a range
	println('\nClipped to range [0, 150]:')
	df_clipped := df_clean.clip(0.0, 150.0)!
	df_clipped.head(10, vframes.DFConfig{})
	
	// Power transformation
	println('\nSquared values:')
	df_squared := df_clean.pow(2, vframes.FuncOptions{})!
	df_squared.head(10, vframes.DFConfig{})

	print_header('Type Conversions')
	
	// Convert column types
	println('\nConvert value column to integer:')
	df_converted := df_clean.astype({'value': 'int'})!
	df_converted.head(10, vframes.DFConfig{})
	println('Types after conversion: ${df_converted.dtypes()}')

	print_header('Working with Files')
	
	// Create a DataFrame to export
	export_data := [
		{'product': json2.Any('Widget'), 'price': json2.Any(29.99), 'quantity': json2.Any(100)},
		{'product': json2.Any('Gadget'), 'price': json2.Any(49.99), 'quantity': json2.Any(50)},
		{'product': json2.Any('Tool'), 'price': json2.Any(19.99), 'quantity': json2.Any(200)},
		{'product': json2.Any('Device'), 'price': json2.Any(99.99), 'quantity': json2.Any(25)},
	]
	
	df_export := ctx.read_records(export_data)!
	
	// Export to CSV
	csv_path := os.join_path(os.temp_dir(), 'vframes_export.csv')
	df_export.to_csv(csv_path, vframes.ToCsvOptions{})!
	println('\nExported to CSV: ${csv_path}')
	
	// Export to JSON
	json_path := os.join_path(os.temp_dir(), 'vframes_export.json')
	df_export.to_json(json_path)!
	println('Exported to JSON: ${json_path}')
	
	// Export to Parquet
	parquet_path := os.join_path(os.temp_dir(), 'vframes_export.parquet')
	df_export.to_parquet(parquet_path)!
	println('Exported to Parquet: ${parquet_path}')
	
	// Read back the CSV file to verify
	println('\nReading back the CSV file:')
	df_imported := ctx.read_auto(csv_path)!
	df_imported.head(10, vframes.DFConfig{})
	
	// Clean up temporary files
	os.rm(csv_path) or {}
	os.rm(json_path) or {}
	os.rm(parquet_path) or {}

	print_header('Checking Values')
	
	// Check if values are in a list
	println('\nCheck if category is in ["A", "C"]:')
	df_isin := df_clean.isin(['A', 'C'])!
	df_isin.head(10, vframes.DFConfig{})
	
	// Replace values
	println('\nReplace "A" with "Alpha":')
	df_replaced := df_clean.replace('A', 'Alpha')!
	df_replaced.head(10, vframes.DFConfig{})

	print_header('Advanced Features Complete')
	println('This example demonstrated:')
	println('- Creating data with missing (null) values')
	println('- Detecting missing values (isna, notna)')
	println('- Dropping rows with missing values (dropna)')
	println('- Filling missing values (fillna, ffill, bfill)')
	println('- Absolute values (abs)')
	println('- Rounding values (round)')
	println('- Clipping values to a range (clip)')
	println('- Power transformations (pow)')
	println('- Type conversions (astype)')
	println('- Exporting to CSV, JSON, and Parquet')
	println('- Reading data from files (read_auto)')
	println('- Checking if values are in a list (isin)')
	println('- Replacing values (replace)')
}
