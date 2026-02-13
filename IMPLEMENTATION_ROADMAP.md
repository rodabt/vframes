# Implementation Roadmap

This document outlines the planned implementation of VFrames functions, prioritized by common usage in data analysis workflows.

## Priority Phases

### Phase 1: Critical (Most Common - Daily Use)
Functions essential for basic data manipulation tasks.

| Status | Function | Description |
|--------|----------|-------------|
| [X] | `to_csv` | Export DataFrame to CSV file |
| [X] | `to_json` | Export DataFrame to JSON file |
| [X] | `to_parquet` | Export DataFrame to Parquet file |
| [X] | `rename` | Rename columns |
| [X] | `rename_axis` | Rename axis (alias for rename) |
| [X] | `replace` | Replace values in DataFrame |
| [X] | `astype` | Convert column data types |
| [X] | `isin` | Filter rows by list of values |
| [X] | `value_counts` | Count unique values |
| [X] | `agg` / `aggregate` | Aggregate functions (sum, mean, etc.) |
| [X] | `describe` | Basic statistics (already implemented) |

### Phase 2: High Priority (Common - Weekly Use)
Functions frequently used in data cleaning and transformation.

| Status | Function | Description |
|--------|----------|-------------|
| [X] | `merge` | Merge two DataFrames |
| [X] | `join` | Join two DataFrames |
| [X] | `concat` | Concatenate DataFrames |
| [X] | `pivot` | Pivot table functionality |
| [X] | `pivot_table` | Advanced pivot with aggregation |
| [X] | `melt` | Unpivot DataFrame |
| [X] | `drop_duplicates` | Remove duplicate rows |
| [X] | `sample` | Random sample of rows |
| [X] | `assign` | Add new columns via assignment |
| [X] | `select` | Select columns (already implemented) |

### Phase 3: Medium Priority (Occasional Use)
Useful for specific analytical workflows.

| Status | Function | Description |
|--------|----------|-------------|
| [X] | `apply` | Apply custom functions |
| [X] | `map` | Map function to elements |
| [X] | `rank` | Rank values |
| [X] | `quantile` | Calculate quantiles |
| [X] | `corr` | Correlation matrix |
| [X] | `cov` | Covariance matrix |
| [X] | `rolling` | Rolling window calculations |
| [X] | `shift` | Shift values |
| [X] | `diff` | Calculate differences |
| [X] | `pct_change` | Percentage change |

### Phase 4: Lower Priority (Specialized Use)
Advanced functions for specific use cases.

| Status | Function | Description |
|--------|----------|-------------|
| [ ] | `cummax` | Cumulative maximum |
| [ ] | `cummin` | Cumulative minimum |
| [ ] | `cumprod` | Cumulative product |
| [ ] | `cumsum` | Cumulative sum |
| [ ] | `ewm` | Exponentially weighted functions |
| [ ] | `resample` | Resample time series |
| [ ] | `interpolate` | Interpolate missing values |
| [ ] | `get` | Get value by label |
| [ ] | `at` | Access single value by label |
| [ ] | `iat` | Access single value by position |

### Phase 5: DataFrame Operations (Index/Label Management)
Advanced index manipulation.

| Status | Function | Description |
|--------|----------|-------------|
| [ ] | `loc` | Label-based indexing |
| [ ] | `iloc` | Position-based indexing |
| [ ] | `set_index` | Set index column |
| [ ] | `reset_index` | Reset index (already implemented) |
| [ ] | `reindex` | Reindex DataFrame |
| [ ] | `rename_axis` | Rename axis |

### Phase 6: Output/Export Formats
Various export formats.

| Status | Function | Description |
|--------|----------|-------------|
| [ ] | `to_dict` | Export to dictionary |
| [ ] | `to_string` | String representation |
| [ ] | `to_html` | HTML table |
| [ ] | `to_excel` | Excel file |
| [ ] | `to_sql` | SQL table |
| [ ] | `to_records` | NumPy records |
| [ ] | `to_markdown` | Markdown table |
| [ ] | `to_clipboard` | Copy to clipboard |
| [ ] | `to_orc` | ORC file format |

### Phase 7: Iteration/Iteration Helpers
Row/column iteration.

| Status | Function | Description |
|--------|----------|-------------|
| [ ] | `iterrows` | Iterate over rows |
| [ ] | `itertuples` | Iterate over rows as tuples |
| [ ] | `items` | Iterate over column pairs |
| [ ] | `iterrows` | (already listed above) |

### Phase 8: Advanced/Experimental
Complex operations.

| Status | Function | Description |
|--------|----------|-------------|
| [ ] | `unstack` | Unstack pivot |
| [ ] | `stack` | Stack DataFrame |
| [ ] | `explode` | Explode list-like columns |
| [ ] | `melt` | Already in Phase 2 |
| [ ] | `where` | Conditional replacement |
| [ ] | `mask` | (already implemented) |
| [ ] | `eval` | Evaluate expressions |
| [ ] | `query` | (already implemented) |

## Implementation Notes

### Completed Functions (v0.1.3+)
- `sort_values` - Sort by column values
- `isna` / `isnull` - Check for null values
- `notna` / `notnull` - Check for non-null values
- `fillna` - Fill null values (value, ffill, bfill)
- `ffill` / `bfill` - Forward/backward fill
- Error handling - Proper error propagation

### Completed Functions (v0.1.4+ - Phase 1 & 2)

#### Phase 1 - Critical
- `to_csv` - Export DataFrame to CSV file
- `to_json` - Export DataFrame to JSON file
- `to_parquet` - Export DataFrame to Parquet file
- `rename` - Rename columns
- `rename_axis` - Rename axis (alias)
- `replace` - Replace values in DataFrame
- `astype` - Convert column data types
- `isin` - Filter rows by list of values
- `value_counts` - Count unique values
- `agg` / `aggregate` - Aggregate functions

#### Phase 2 - High Priority
- `merge` - Merge two DataFrames
- `join` - Join two DataFrames
- `concat` - Concatenate DataFrames
- `pivot` / `pivot_table` - Pivot table functionality
- `melt` - Unpivot DataFrame
- `sample` - Random sample of rows
- `assign` - Add new columns via assignment

#### Phase 3 - Medium Priority
- `apply` - Apply custom SQL functions
- `map` - Map function to elements (alias for apply)
- `rank` - Rank values (various methods)
- `quantile` - Calculate quantiles
- `corr` - Correlation matrix for numeric columns
- `cov` - Covariance matrix for numeric columns
- `rolling` - Rolling window calculations
- `shift` - Shift values by periods
- `diff` - Calculate differences between rows
- `pct_change` - Percentage change between rows

### DuckDB Backend
Many functions can leverage DuckDB's powerful SQL engine:
- `merge`, `join` - SQL JOIN operations
- `pivot`, `unstack` - SQL pivot capabilities
- `rolling`, `ewm` - SQL window functions
- `rank`, `dense_rank` - SQL window functions
- `corr`, `cov` - Statistical functions
- `quantile` - SQL quantile functions

### Memory Management
Consider implementing:
- Automatic cleanup of intermediate tables
- Table naming strategy for garbage collection
- Memory-mapped file handling for large datasets

## Contributing

When implementing new functions:
1. Add tests in `src/funcs_test.v` or `src/mutation_test.v`
2. Update this document when starting implementation
3. Use consistent error handling (return `!DataFrame`)
4. Document function with V doc comments
5. Consider both in-memory and persisted contexts
