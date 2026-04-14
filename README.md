# VFrames

A Pandas-like DataFrame library for V, powered by DuckDB.

## Overview

VFrames provides a familiar data-manipulation API for V developers, backed by an embedded DuckDB engine. Every operation compiles down to SQL and runs inside DuckDB's vectorized query executor — giving you fast in-memory analytics with a concise, expressive API.

## Features

- **Pandas-like API** — familiar method names for data scientists coming from Python
- **DuckDB backend** — vectorized execution, analytical SQL functions, columnar storage
- **Multiple file formats** — read and write CSV, JSON, and Parquet with auto-detection
- **Immutable design** — every operation returns a new DataFrame; originals are never mutated
- **Full error propagation** — no hidden panics; errors surface as V result types (`!T`)
- **Rich function set** — filtering, grouping, joins, pivots, rolling windows, cumulative ops, and more

## Installation

```bash
# 1. Install the DuckDB V bindings
v install https://github.com/rodabt/vduckdb

# 2. Install VFrames
v install https://github.com/rodabt/vframes
```

Ensure `LIBDUCKDB_DIR` points to the directory containing `libduckdb.so` / `libduckdb.dylib`.

## Quick Start

```v
import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()!
    defer { ctx.close() }

    // Load from a file (CSV, JSON, or Parquet — detected automatically)
    df := ctx.read_auto('employees.csv')!

    // Or build from in-memory records
    data := [
        {'name': json2.Any('Alice'), 'dept': json2.Any('Eng'),   'salary': json2.Any(90000)},
        {'name': json2.Any('Bob'),   'dept': json2.Any('Sales'), 'salary': json2.Any(70000)},
        {'name': json2.Any('Carol'), 'dept': json2.Any('Eng'),   'salary': json2.Any(95000)},
    ]
    df2 := ctx.read_records(data)!

    // Explore
    println('Shape: ${df2.shape()!}')          // [3, 3]
    println('Columns: ${df2.columns()!}')       // ['name', 'dept', 'salary']
    df2.head(5, vframes.DFConfig{})!

    // Transform
    df3 := df2
        .filter('salary > 75000')!
        .add_column('bonus', 'salary * 0.1')!
        .sort_values(['salary'], vframes.SortOptions{ascending: false})!

    // Aggregate
    by_dept := df2.group_by(['dept'], {
        'avg_salary': 'avg(salary)',
        'headcount':  'count(*)',
    })!
    by_dept.head(10, vframes.DFConfig{})!

    // Export
    df3.to_csv('/tmp/result.csv', vframes.ToCsvOptions{})!
    println(df3.to_markdown()!)
}
```

## Core Concepts

### Context

All DataFrames live inside a `DataFrameContext`, which owns the DuckDB connection. Open one context per workflow and close it when done:

```v
mut ctx := vframes.init()!              // in-memory (default)
mut ctx := vframes.init(location: 'data.db')!  // persisted to disk
defer { ctx.close() }
```

### Immutability

Every method returns a **new** DataFrame backed by a new DuckDB table. Originals are untouched:

```v
df2 := df.add_column('tax', 'salary * 0.2')!
// df still has the original columns; df2 has the extra column
```

### Error handling

Functions return `!T`. Propagate with `!` or handle inline with `or {}`:

```v
df := ctx.read_auto('missing.csv')!             // panics on error
df := ctx.read_auto('missing.csv') or {         // handle gracefully
    eprintln('File not found: ${err}')
    return
}
```

## API Summary

### I/O

| Function | Description |
|----------|-------------|
| `ctx.read_auto(path)!` | Read CSV / JSON / Parquet, auto-detected |
| `ctx.read_records(data)!` | Load from `[]map[string]json2.Any` |
| `df.to_csv(path, opts)!` | Export to CSV |
| `df.to_json(path)!` | Export to newline-delimited JSON |
| `df.to_parquet(path)!` | Export to Parquet |
| `df.to_dict()!` | Return all rows as `[]map[string]json2.Any` |
| `df.to_markdown()!` | Return DataFrame as a Markdown table string |

### Exploration

| Function | Returns | Description |
|----------|---------|-------------|
| `df.head(n, cfg)!` | `Data` | First N rows |
| `df.tail(n, cfg)!` | `Data` | Last N rows |
| `df.shape()!` | `[]int` | `[rows, cols]` |
| `df.columns()!` | `[]string` | Column names |
| `df.dtypes()!` | `map[string]string` | Column types |
| `df.describe(cfg)!` | `Data` | Summary statistics |
| `df.info(cfg)!` | `Data` | Column names and types |
| `df.values(opts)!` | `Data` | All rows |

### Selection & Mutation

| Function | Description |
|----------|-------------|
| `df.subset(cols)!` | Select columns by name |
| `df.select_cols(cols)!` | Alias for `subset` |
| `df.slice(start, end)!` | Select row range (1-indexed, inclusive) |
| `df.filter(condition)!` | Filter rows by SQL WHERE condition |
| `df.query(expr, cfg)!` | SQL column expression or `SELECT cols WHERE cond` |
| `df.add_column(name, expr)!` | Add column via SQL expression |
| `df.assign(name, expr)!` | Alias for `add_column` |
| `df.delete_column(name)!` | Remove one column |
| `df.drop(cols)!` | Remove multiple columns |
| `df.rename(mapper)!` | Rename columns via `map[string]string` |
| `df.add_prefix(p)!` | Prepend `p_` to all column names |
| `df.add_suffix(s)!` | Append `_s` to all column names |
| `df.sort_values(cols, opts)!` | Sort by one or more columns |
| `df.astype(map)!` | Convert column types |
| `df.replace(old, new)!` | Replace string values |
| `df.isin(values)!` | Boolean mask for listed values |

### Joins & Reshaping

| Function | Description |
|----------|-------------|
| `df1.merge(df2, on: 'col', how: 'inner')!` | SQL join |
| `df1.join(df2, on: 'col')!` | Alias for `merge` |
| `vframes.concat([df1, df2])!` | Stack DataFrames vertically |
| `df.pivot(index, columns, values, aggfunc)!` | Long → wide |
| `df.pivot_table(...)!` | Alias for `pivot` |
| `df.melt(id_vars, value_vars)!` | Wide → long |
| `df.drop_duplicates(subset)!` | Remove duplicate rows |
| `df.sample(n, replace)!` | Random sample |

### Aggregation & Statistics

| Function | Description |
|----------|-------------|
| `df.group_by(dims, metrics)!` | Group and aggregate |
| `df.groupby(...)!` | Alias for `group_by` |
| `df.agg(map)!` | Aggregate without grouping |
| `df.sum(opts)!` | Column-wise sum |
| `df.mean(opts)!` | Column-wise mean |
| `df.median(opts)!` | Column-wise median |
| `df.std()!` | Standard deviation |
| `df.var()!` | Variance |
| `df.min(opts)!` / `df.max(opts)!` | Min / max |
| `df.count()!` | Non-null counts |
| `df.nunique()!` | Distinct-value counts |
| `df.nlargest(n)!` / `df.nsmallest(n)!` | Top / bottom N rows |
| `df.quantile(q)!` | Percentile (0.0 – 1.0) |
| `df.corr()!` / `df.cov()!` | Correlation / covariance matrices |

### Element-wise Math

| Function | Description |
|----------|-------------|
| `df.add(n)!` / `df.sub(n)!` / `df.mul(n)!` / `df.div(n)!` | Scalar arithmetic |
| `df.floordiv(n)!` / `df.mod(n)!` | Integer division, modulo |
| `df.abs()!` | Absolute value |
| `df.pow(n, opts)!` | Power |
| `df.round(decimals)!` | Round |
| `df.clip(min, max)!` | Clamp to range |

### Cumulative & Time-Series

| Function | Description |
|----------|-------------|
| `df.cumsum()!` / `df.cummax()!` / `df.cummin()!` / `df.cumprod()!` | Cumulative aggregates |
| `df.shift(n)!` | Shift rows by N periods |
| `df.diff()!` | Row-to-row difference |
| `df.pct_change()!` | Row-to-row % change |
| `df.rolling(col, func, opts)!` | Rolling window aggregate |
| `df.rank(opts)!` | Row ranking |

### Missing Values

| Function | Description |
|----------|-------------|
| `df.isna()!` / `df.isnull()!` | Boolean null mask |
| `df.notna()!` / `df.notnull()!` | Boolean non-null mask |
| `df.dropna(opts)!` | Drop rows with nulls |
| `df.fillna(opts)!` | Fill nulls with constant |
| `df.ffill()!` / `df.bfill()!` | Forward / backward fill |

## Documentation

- [Tutorial](TUTORIAL.md) — side-by-side guide with Pandas comparisons
- [Examples](examples/) — runnable end-to-end scripts

## Requirements

- V (Vlang) compiler
- DuckDB shared library (`LIBDUCKDB_DIR` environment variable)

## License

MIT License — see [LICENSE](LICENSE) for details.
