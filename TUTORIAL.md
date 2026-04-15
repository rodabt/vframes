# VFrames Tutorial

A side-by-side guide for Python/Pandas developers learning VFrames.

> **Tip:** Standalone runnable examples are in the `examples/` directory.

## Table of Contents

1. [Initialization](#initialization)
2. [Loading Data](#loading-data)
3. [Exploring Data](#exploring-data)
4. [Selecting Columns](#selecting-columns)
5. [Filtering Rows](#filtering-rows)
6. [Adding, Deleting & Renaming Columns](#adding-deleting--renaming-columns)
7. [Sorting](#sorting)
8. [Grouping and Aggregation](#grouping-and-aggregation)
9. [Merging DataFrames](#merging-dataframes)
10. [Reshaping Data](#reshaping-data)
11. [Handling Missing Values](#handling-missing-values)
12. [Mathematical Operations](#mathematical-operations)
13. [Statistical Functions](#statistical-functions)
14. [Cumulative Functions](#cumulative-functions)
15. [Time-Series Operations](#time-series-operations)
16. [Exporting Data](#exporting-data)
17. [Complete Example](#complete-example)
18. [Quick Reference](#quick-reference)

---

## Initialization

### VFrames

```v
import vframes

fn main() {
    // In-memory context (default)
    mut ctx := vframes.init()!
    defer { ctx.close() }  // always close when done

    // Persisted context (writes to disk)
    mut ctx2 := vframes.init(location: 'data.db')!
    defer { ctx2.close() }
}
```

### Pandas

```python
import pandas as pd
# No explicit initialization needed
```

---

## Loading Data

### VFrames

```v
import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()!
    defer { ctx.close() }

    // Auto-detect format from file extension (CSV, JSON, Parquet)
    df := ctx.read_auto('data.csv')!

    // From in-memory records — values must be wrapped in json2.Any
    data := [
        {'name': json2.Any('Alice'), 'age': json2.Any(30)},
        {'name': json2.Any('Bob'),   'age': json2.Any(25)},
    ]
    df2 := ctx.read_records(data)!
}
```

### Pandas

```python
import pandas as pd

df = pd.read_csv('data.csv')

data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
df2 = pd.DataFrame(data)
```

---

## Exploring Data

### VFrames

```v
// Print first/last N rows to stdout
df.head(5)!
df.tail(3)!

// Return rows as data (suppress stdout)
rows := df.head(5, to_stdout: false)! as []map[string]json2.Any

// Shape: [rows, cols]
shape := df.shape()!
println('Rows: ${shape[0]}, Cols: ${shape[1]}')

// Column names
cols := df.columns()!      // []string

// Column data types
types := df.dtypes()!      // map[string]string  e.g. {'age': 'INTEGER'}

// Summary statistics (count, mean, min, max, etc.)
df.describe()!

// Column info (name + type per row)
df.info()!

// All rows as []map[string]json2.Any
all_rows := df.values(vframes.ValuesParams{})! as []map[string]json2.Any
```

### Pandas

```python
df.head(5)!
df.tail(3)!
df.shape          # (rows, cols)
df.columns        # Index of column names
df.dtypes         # Series of types
df.describe()!
df.info()!
df.to_dict('records')
```

---

## Selecting Columns

### VFrames

```v
// Select specific columns
df2 := df.subset(['name', 'age'])!
df2 := df.select_cols(['name', 'age'])!  // alias

// Single-column slice
df2 := df.slice(1, 3)!   // rows 1-3 (1-indexed, inclusive)

// Add prefix or suffix to all column names
df2 := df.add_prefix('emp_')!   // emp_name, emp_age, ...
df2 := df.add_suffix('_raw')!   // name_raw, age_raw, ...
```

### Pandas

```python
df2 = df[['name', 'age']]
df2 = df.iloc[0:3]
df2 = df.add_prefix('emp_')
df2 = df.add_suffix('_raw')
```

---

## Filtering Rows

### VFrames

The `filter` method (and its alias `query`) accepts SQL WHERE conditions:

```v
// Simple condition
df2 := df.filter('age > 25')!

// Compound condition
df2 := df.filter('age > 25 AND city = \'NYC\'')!

// Select columns AND filter (separate with WHERE)
df2 := df.query('name, age WHERE age > 25')!

// Full column expression (no WHERE → column transformation)
df2 := df.query('name, age * 2 as age_doubled')!

// Boolean mask: which values are in a list?
mask := df.isin(['Alice', 'Bob'])!

// Negate: rows NOT matching a condition
df2 := df.mask('age > 25')!
```

### Pandas

```python
df2 = df[df['age'] > 25]
df2 = df[(df['age'] > 25) & (df['city'] == 'NYC')]
df2 = df.query('age > 25')[['name', 'age']]
mask = df.isin(['Alice', 'Bob'])
```

---

## Adding, Deleting & Renaming Columns

### VFrames

```v
// Add a column using any SQL expression
df2 := df.add_column('age_doubled', 'age * 2')!
df2 := df.add_column('initials', 'left(name, 1)')!

// Assign is an alias for add_column
df2 := df.assign('bonus', 'salary * 0.1')!

// Delete one column
df2 := df.delete_column('salary')!

// Drop multiple columns at once
df2 := df.drop(['salary', 'bonus', 'notes'])!

// Rename columns (pass a map of old → new names)
df2 := df.rename({'first_name': 'name', 'emp_id': 'id'})!

// Rename the axis label only (no column rename)
df2 := df.rename_axis('employee')!

// Convert column types
df2 := df.astype({'age': 'double', 'id': 'varchar'})!
```

### Pandas

```python
df['age_doubled'] = df['age'] * 2
df2 = df.assign(bonus=df['salary'] * 0.1)
df2 = df.drop(columns=['salary'])
df2 = df.drop(columns=['salary', 'bonus', 'notes'])
df2 = df.rename(columns={'first_name': 'name', 'emp_id': 'id'})
df2 = df.astype({'age': float})
```

---

## Sorting

### VFrames

```v
// Sort ascending (default)
df2 := df.sort_values(['age'])!

// Sort descending
df2 := df.sort_values(['salary'], ascending: false)!

// Sort by multiple columns
df2 := df.sort_values(['dept', 'salary'], ascending: false)!
```

### Pandas

```python
df2 = df.sort_values(by=['age'])
df2 = df.sort_values(by=['salary'], ascending=False)
df2 = df.sort_values(by=['dept', 'salary'], ascending=False)
```

---

## Grouping and Aggregation

### VFrames

Aggregation expressions are SQL fragments: `avg(col)`, `sum(col)`, `count(*)`, `min(col)`, `max(col)`, `stddev(col)`.

```v
// Group by one column
df2 := df.group_by(['dept'], {
    'avg_salary': 'avg(salary)',
    'headcount':  'count(*)',
    'max_age':    'max(age)',
})!

// Group by multiple columns
df2 := df.group_by(['dept', 'city'], {
    'total_sales': 'sum(sales)',
})!

// groupby is an alias
df2 := df.groupby(['dept'], {'avg_salary': 'avg(salary)'})!

// Aggregate without grouping (collapses to 1 row per column)
df_sum  := df.sum(vframes.FuncOptions{})!
df_mean := df.mean(vframes.FuncOptions{})!
df_min  := df.min(vframes.FuncOptions{})!
df_max  := df.max(vframes.FuncOptions{})!

// agg with a map of column → function
df2 := df.agg({'salary': 'mean', 'age': 'max'})!
```

### Pandas

```python
df2 = df.groupby('dept').agg({
    'salary': 'mean',
    'age':    ['count', 'max'],
}).reset_index()

df.sum()
df.mean()
```

---

## Merging DataFrames

### VFrames

```v
// Inner join (default)
df3 := df1.merge(df2, on: 'id')!

// Left join
df3 := df1.merge(df2, on: 'id', how: 'left')!

// Join on columns with different names
df3 := df1.merge(df2, left_on: 'emp_id', right_on: 'id')!

// join is an alias for merge
df3 := df1.join(df2, on: 'id', how: 'inner')!

// Stack DataFrames vertically
df3 := vframes.concat([df1, df2])!
```

Supported `how` values: `'inner'` (default), `'left'`, `'right'`, `'outer'`/`'full'`, `'cross'`.

### Pandas

```python
df3 = pd.merge(df1, df2, on='id')
df3 = pd.merge(df1, df2, on='id', how='left')
df3 = pd.merge(df1, df2, left_on='emp_id', right_on='id')
df3 = pd.concat([df1, df2], ignore_index=True)
```

---

## Reshaping Data

### VFrames

```v
// Pivot: long → wide
// Columns from 'variable', values from 'value', grouped by 'date'
df_wide := df.pivot(
    index:   'date',
    columns: 'variable',
    values:  'value',
    aggfunc: 'mean',    // optional, default: 'max'
)!

// pivot_table is an alias for pivot
df_wide := df.pivot_table(
    index:   'date',
    columns: 'variable',
    values:  'value',
    aggfunc: 'sum',
)!

// Melt: wide → long
df_long := df.melt(
    id_vars:    ['date'],
    value_vars: ['temp', 'humidity'],
)!
// produces columns: date, variable, value
```

### Pandas

```python
df_wide = df.pivot_table(index='date', columns='variable',
                         values='value', aggfunc='mean')

df_long = df.melt(id_vars=['date'],
                  value_vars=['temp', 'humidity'])
```

---

## Handling Missing Values

### VFrames

```v
// Detect nulls (boolean mask DataFrame)
df_isna  := df.isna()!    // true where value is null
df_notna := df.notna()!   // true where value is not null

// isnull / notnull are aliases
df_isna  := df.isnull()!
df_notna := df.notnull()!

// Drop rows with nulls
df2 := df.dropna(vframes.DropOptions{how: 'any'})!  // drop if any null
df2 := df.dropna(vframes.DropOptions{how: 'all'})!  // drop if all null

// Fill nulls with a constant
df2 := df.fillna(vframes.FillnaOptions{value: '0'})!

// Forward fill (carry previous non-null value forward)
df2 := df.ffill()!

// Backward fill (carry next non-null value backward)
df2 := df.bfill()!
```

### Pandas

```python
df.isna()
df.notna()
df2 = df.dropna(how='any')
df2 = df.fillna(0)
df2 = df.ffill()
df2 = df.bfill()
```

---

## Mathematical Operations

### VFrames

All arithmetic operations apply only to numeric columns; string columns are preserved unchanged.

```v
// Scalar arithmetic
df2 := df.add(10)!      // all numeric cols + 10
df2 := df.sub(5)!       // all numeric cols - 5
df2 := df.mul(2)!       // all numeric cols * 2
df2 := df.div(100)!     // all numeric cols / 100
df2 := df.floordiv(3)!  // integer division
df2 := df.mod(2)!       // modulo

// Element-wise
df2 := df.abs()!              // absolute value
df2 := df.pow(2, vframes.FuncOptions{})!  // square every element
df2 := df.round(2)!           // round to 2 decimal places
df2 := df.clip(0.0, 100.0)!  // clamp values to [0, 100]

// Type conversion
df2 := df.astype({'price': 'integer', 'code': 'varchar'})!
```

### Pandas

```python
df + 10
df - 5
df * 2
df / 100
df.abs()
df ** 2
df.round(2)
df.clip(lower=0, upper=100)
df.astype({'price': int})
```

---

## Statistical Functions

All stat functions collapse the DataFrame to a single summary row containing only numeric columns.

### VFrames

```v
df_sum    := df.sum(vframes.FuncOptions{})!
df_mean   := df.mean(vframes.FuncOptions{})!
df_median := df.median(vframes.FuncOptions{})!
df_std    := df.std()!
df_var    := df.var()!
df_min    := df.min(vframes.FuncOptions{})!
df_max    := df.max(vframes.FuncOptions{})!
df_count  := df.count()!           // count of non-null values
df_nuniq  := df.nunique()!         // count of distinct values

// Top / bottom N rows by numeric magnitude
df_top3    := df.nlargest(3)!
df_bottom3 := df.nsmallest(3)!

// Value frequency table for first column
df_vc := df.value_counts()!

// Percentile
df_q90 := df.quantile(0.90)!

// Rank rows
df_ranked := df.rank(vframes.RankOptions{})!

// Correlation / covariance matrices
df_corr := df.corr()!
df_cov  := df.cov()!
```

### Pandas

```python
df.sum()
df.mean()
df.median()
df.std()
df.var()
df.min(); df.max()
df.count()
df.nunique()
df.nlargest(3, 'col')
df.value_counts()
df.quantile(0.9)
df.rank()
df.corr()
df.cov()
```

---

## Cumulative Functions

Cumulative functions compute running aggregates row by row, preserving all rows.

### VFrames

```v
df2 := df.cumsum()!    // running sum
df2 := df.cummax()!    // running maximum
df2 := df.cummin()!    // running minimum
df2 := df.cumprod()!   // running product
```

### Pandas

```python
df.cumsum()
df.cummax()
df.cummin()
df.cumprod()
```

---

## Time-Series Operations

### VFrames

```v
// Shift rows forward (positive) or backward (negative)
df2 := df.shift(1)!     // each value takes previous row's value

// Difference between consecutive rows
df2 := df.diff()!

// Percent change between consecutive rows
df2 := df.pct_change()!

// Rolling window aggregate
df2 := df.rolling('price', 'avg', vframes.RollingOptions{window: 3})!
```

### Pandas

```python
df.shift(1)
df.diff()
df.pct_change()
df['price'].rolling(3).mean()
```

---

## Exporting Data

### VFrames

```v
// To CSV
df.to_csv('output.csv', vframes.ToCsvOptions{})!

// To CSV with options
df.to_csv('output.tsv', vframes.ToCsvOptions{
    delimiter: '\t',
    header:    true,
    nullstr:   'NA',
})!

// To JSON (newline-delimited)
df.to_json('output.json')!

// To Parquet
df.to_parquet('output.parquet')!

// To in-memory slice
rows := df.to_dict()!   // []map[string]json2.Any

// To Markdown table string
md := df.to_markdown()!
println(md)
```

### Pandas

```python
df.to_csv('output.csv', index=False)
df.to_csv('output.tsv', sep='\t')
df.to_json('output.json', orient='records', lines=True)
df.to_parquet('output.parquet')
rows = df.to_dict('records')
df.to_markdown()
```

---

## Complete Example

### VFrames

```v
import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()!
    defer { ctx.close() }

    // Load data
    data := [
        {'name': json2.Any('Alice'),   'dept': json2.Any('Eng'),     'salary': json2.Any(90000)},
        {'name': json2.Any('Bob'),     'dept': json2.Any('Eng'),     'salary': json2.Any(80000)},
        {'name': json2.Any('Carol'),   'dept': json2.Any('Sales'),   'salary': json2.Any(70000)},
        {'name': json2.Any('David'),   'dept': json2.Any('Sales'),   'salary': json2.Any(65000)},
        {'name': json2.Any('Eve'),     'dept': json2.Any('Eng'),     'salary': json2.Any(95000)},
    ]
    df := ctx.read_records(data)!

    // Explore
    println('Shape: ${df.shape()!}')
    df.head(3)!

    // Add a column
    df2 := df.add_column('bonus', 'salary * 0.1')!

    // Filter
    df3 := df2.filter('salary > 75000')!

    // Sort descending
    df4 := df3.sort_values(['salary'], ascending: false)!

    // Group by department
    by_dept := df.group_by(['dept'], {
        'avg_salary': 'avg(salary)',
        'headcount':  'count(*)',
    })!
    by_dept.head(10)!

    // Export
    df4.to_csv('/tmp/high_earners.csv', vframes.ToCsvOptions{})!

    // Print as Markdown
    println(df4.to_markdown()!)
}
```

### Pandas

```python
import pandas as pd

data = [
    {'name': 'Alice', 'dept': 'Eng',   'salary': 90000},
    {'name': 'Bob',   'dept': 'Eng',   'salary': 80000},
    {'name': 'Carol', 'dept': 'Sales', 'salary': 70000},
    {'name': 'David', 'dept': 'Sales', 'salary': 65000},
    {'name': 'Eve',   'dept': 'Eng',   'salary': 95000},
]
df = pd.DataFrame(data)

print(f'Shape: {df.shape}')
print(df.head(3))

df['bonus'] = df['salary'] * 0.1
df3 = df[df['salary'] > 75000]
df4 = df3.sort_values(by=['salary'], ascending=False)

by_dept = df.groupby('dept').agg(
    avg_salary=('salary', 'mean'),
    headcount=('name', 'count'),
).reset_index()
print(by_dept)

df4.to_csv('/tmp/high_earners.csv', index=False)
print(df4.to_markdown(index=False))
```

---

## Quick Reference

### Context & I/O

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Init (memory) | `vframes.init()!` | — |
| Init (file) | `vframes.init(location: 'f.db')!` | — |
| Read file | `ctx.read_auto('f.csv')!` | `pd.read_csv('f.csv')` |
| From records | `ctx.read_records(data)!` | `pd.DataFrame(data)` |
| To CSV | `df.to_csv('f.csv', ...)!` | `df.to_csv('f.csv')` |
| To JSON | `df.to_json('f.json')!` | `df.to_json('f.json')` |
| To Parquet | `df.to_parquet('f.parquet')!` | `df.to_parquet('f.parquet')` |
| To dict | `df.to_dict()!` | `df.to_dict('records')` |
| To Markdown | `df.to_markdown()!` | `df.to_markdown()` |

### Exploration

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Head | `df.head(n)` | `df.head(n)` |
| Tail | `df.tail(n)` | `df.tail(n)` |
| Shape | `df.shape()!` → `[]int` | `df.shape` |
| Columns | `df.columns()!` → `[]string` | `df.columns` |
| Types | `df.dtypes()!` → `map[string]string` | `df.dtypes` |
| Describe | `df.describe()!` | `df.describe()` |
| Info | `df.info()!` | `df.info()` |

### Selection & Mutation

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Select cols | `df.subset(['a','b'])!` | `df[['a','b']]` |
| Slice rows | `df.slice(1, 5)!` | `df.iloc[0:5]` |
| Filter rows | `df.filter('age > 25')!` | `df[df.age > 25]` |
| SQL filter+select | `df.query('a, b WHERE a > 5', ...)!` | `df.query('a > 5')[['a','b']]` |
| Add column | `df.add_column('c', 'a + b')!` | `df['c'] = df.a + df.b` |
| Delete column | `df.delete_column('c')!` | `df.drop(columns=['c'])` |
| Drop columns | `df.drop(['c','d'])!` | `df.drop(columns=['c','d'])` |
| Rename | `df.rename({'old': 'new'})!` | `df.rename(columns={'old': 'new'})` |
| Add prefix | `df.add_prefix('p_')!` | `df.add_prefix('p_')` |
| Add suffix | `df.add_suffix('_s')!` | `df.add_suffix('_s')` |
| Sort asc | `df.sort_values(['c'], ...)!` | `df.sort_values('c')` |
| Sort desc | `df.sort_values(['c'], ascending: false)!` | `df.sort_values('c', ascending=False)` |

### Aggregation

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Group by | `df.group_by(['c'], {'s': 'sum(v)'})!` | `df.groupby('c').agg(...)` |
| Sum | `df.sum(vframes.FuncOptions{})!` | `df.sum()` |
| Mean | `df.mean(vframes.FuncOptions{})!` | `df.mean()` |
| Median | `df.median(vframes.FuncOptions{})!` | `df.median()` |
| Std dev | `df.std()!` | `df.std()` |
| Variance | `df.var()!` | `df.var()` |
| Min / Max | `df.min(...)!` / `df.max(...)!` | `df.min()` / `df.max()` |
| Count | `df.count()!` | `df.count()` |
| Nunique | `df.nunique()!` | `df.nunique()` |
| Top N | `df.nlargest(n)!` | `df.nlargest(n, col)` |
| Bottom N | `df.nsmallest(n)!` | `df.nsmallest(n, col)` |

### Joins & Reshaping

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Merge | `df1.merge(df2, on: 'id')!` | `pd.merge(df1, df2, on='id')` |
| Left join | `df1.merge(df2, on: 'id', how: 'left')!` | `pd.merge(..., how='left')` |
| Concat | `vframes.concat([df1, df2])!` | `pd.concat([df1, df2])` |
| Pivot | `df.pivot(index: 'i', columns: 'c', values: 'v')!` | `df.pivot_table(...)` |
| Melt | `df.melt(id_vars: ['id'], value_vars: ['a','b'])!` | `df.melt(id_vars=['id'], ...)` |

### Missing Values

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Is null mask | `df.isna()!` | `df.isna()` |
| Not null mask | `df.notna()!` | `df.notna()` |
| Drop rows | `df.dropna(vframes.DropOptions{how: 'any'})!` | `df.dropna()` |
| Fill value | `df.fillna(vframes.FillnaOptions{value: '0'})!` | `df.fillna(0)` |
| Forward fill | `df.ffill()!` | `df.ffill()` |
| Backward fill | `df.bfill()!` | `df.bfill()` |

---

## Important Notes

### 1. Error handling is explicit

Every function that can fail returns `!T`. Use `!` to propagate errors, or `or {}` to handle them:

```v
// Propagate (crashes with error message if it fails)
df := ctx.read_auto('data.csv')!

// Handle explicitly
df := ctx.read_auto('data.csv') or {
    eprintln('Could not load file: ${err}')
    return
}
```

### 2. Values require `json2.Any` wrappers

```v
import x.json2

data := [
    {'name': json2.Any('Alice'), 'age': json2.Any(30), 'score': json2.Any(9.5)},
    {'name': json2.Any('Bob'),   'age': json2.null,    'score': json2.Any(8.0)},
]
```

Use `json2.null` for missing values.

### 3. Operations are immutable

Every method returns a *new* DataFrame — the original is unchanged:

```v
df2 := df.add_column('bonus', 'salary * 0.1')!
// df is unchanged; df2 has the new column
```

### 4. Querying rows vs. transforming columns

`filter` and `query` behave differently depending on the expression:

```v
df.filter('salary > 50000')!                          // WHERE clause
df.query('name, salary WHERE salary > 50000', ...)!   // SELECT + WHERE
df.query('salary * 1.1 as adjusted_salary', ...)!     // column expression
```

### 5. Always close the context

```v
mut ctx := vframes.init()!
defer { ctx.close() }
```

Using `defer` ensures the DuckDB connection is closed even if the function returns early.
