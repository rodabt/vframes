# VFrames Examples

This directory contains practical examples demonstrating the capabilities of VFrames, a high-performance DataFrame library for V powered by DuckDB.

## Examples Overview

### 01_basic_usage.v
**Data Loading and Exploration**

Demonstrates the fundamental operations:
- Initializing a vframes context (in-memory or persisted)
- Loading data from records (array of maps)
- Exploring DataFrame structure:
  - `shape()` - Get dimensions (rows, columns)
  - `columns()` - List column names
  - `dtypes()` - Show data types
  - `head(n)` - View first n rows
  - `tail(n)` - View last n rows
  - `info()` - Show column information
  - `describe()` - Summary statistics
- Column operations:
  - `subset(cols)` - Select specific columns
  - `add_column(name, expr)` - Add calculated columns
  - `add_prefix(prefix)` / `add_suffix(suffix)` - Rename columns
  - `delete_column(col)` - Remove columns
- `slice(start, end)` - Extract row ranges

**Run:** `v run examples/01_basic_usage.v`

### 02_data_analysis.v
**Grouping, Aggregations, and Transformations**

Shows advanced data analysis capabilities:
- Grouping operations:
  - `group_by(dimensions, metrics)` - Group and aggregate
  - Multi-column grouping
- Mathematical operations:
  - `add(n)`, `sub(n)`, `mul(n)`, `div(n)` - Arithmetic
  - `add_column(name, expr)` - Calculated columns
- Statistical functions:
  - `sum(opts)`, `mean(opts)`, `median(opts)` - Central tendency
  - `std()`, `var()` - Variability
  - `count()`, `nunique()` - Counting
- Complex filtering:
  - `query(sql_expression)` - SQL-like queries
- Finding extremes:
  - `nlargest(n)`, `nsmallest(n)` - Top/bottom records
- Value analysis:
  - `value_counts()` - Frequency counts

**Run:** `v run examples/02_data_analysis.v`

### 03_advanced_features.v
**Missing Values, Exports, and Complex Operations**

Covers advanced features:
- Missing value handling:
  - `isna()`, `notna()` - Detect missing values
  - `dropna(opts)` - Remove rows with NA
  - `fillna(opts)` - Fill with values
  - `ffill()`, `bfill()` - Forward/backward fill
- Data transformations:
  - `abs()` - Absolute values
  - `round(decimals)` - Rounding
  - `clip(min, max)` - Limit to range
  - `pow(n, opts)` - Power transformation
  - `astype(type_map)` - Type conversion
- Data export:
  - `to_csv(path, opts)` - CSV export
  - `to_json(path)` - JSON export
  - `to_parquet(path)` - Parquet export
- File operations:
  - `read_auto(path)` - Auto-detect format (CSV/JSON/Parquet)
- Value operations:
  - `isin(values)` - Check membership
  - `replace(old, new)` - Replace values

**Run:** `v run examples/03_advanced_features.v`

## Quick Start

1. **Install vframes:**
   ```bash
   v install https://github.com/rodabt/vduckdb
   v install https://github.com/rodabt/vframes
   ```

2. **Set DuckDB library path:**
   ```bash
   export LIBDUCKDB_DIR=/path/to/duckdb/lib
   ```

3. **Run an example:**
   ```bash
   v run examples/01_basic_usage.v
   ```

## Sample Data Files

The directory includes sample data files for testing:
- `titanic.parquet` - Classic Titanic dataset in Parquet format
- `people-500000.csv` - Large CSV file with 500,000 records
- `data.json` - Sample JSON data file

## Key Concepts

### DataFrame Context
All operations require a context that manages the DuckDB connection:
```v
mut ctx := vframes.init()           // In-memory (default)
mut ctx := vframes.init(location: 'data.db')  // Persisted
defer { ctx.close() }             // Always close when done
```

### Creating DataFrames
```v
// From records (requires x.json2)
data := [
    {'name': json2.Any('Alice'), 'age': json2.Any(30)}
]
df := ctx.read_records(data)!

// From file (auto-detects format)
df := ctx.read_auto('data.csv')!
```

### Method Chaining
Most operations return new DataFrames, enabling method chaining:
```v
df := ctx.read_records(data)!
    .add_column('doubled', 'age * 2')
    .query('age > 25')!
    .head(10)
```

### Error Handling
Use `!` for error propagation and `or` blocks for handling:
```v
df := ctx.read_auto('data.csv') or {
    eprintln('Failed to load: ${err.msg()}')
    return
}
```

## Performance Tips

1. **Use persisted context** for large datasets to avoid memory issues
2. **Chain operations** to minimize intermediate DataFrames
3. **Use SQL queries** via `query()` for complex filtering
4. **Leverage DuckDB** - operations are executed in DuckDB's optimized engine

## More Resources

- [Tutorial](../TUTORIAL.md) - Complete Pandas comparison guide
- [Implementation Roadmap](../IMPLEMENTATION_ROADMAP.md) - API reference
- [Tests](../tests/) - Unit tests showing usage patterns
