# VFrames Tutorial

This tutorial provides a side-by-side comparison of VFrames with Pandas, helping Python developers quickly adopt VFrames.

## Table of Contents

1. [Initialization](#initialization)
2. [Loading Data](#loading-data)
3. [Exploring Data](#exploring-data)
4. [Selecting Columns](#selecting-columns)
5. [Filtering Rows](#filtering-rows)
6. [Adding/Deleting Columns](#addingdeleting-columns)
7. [Grouping and Aggregation](#grouping-and-aggregation)
8. [Sorting](#sorting)
9. [Merging DataFrames](#merging-dataframes)
10. [Exporting Data](#exporting-data)
11. [Handling Missing Values](#handling-missing-values)

---

## Initialization

### VFrames

```v
import vframes

// In-memory DataFrame
mut ctx := vframes.init()

// Persisted to file
mut ctx := vframes.init(location: 'mydata.db')
```

### Pandas

```python
import pandas as pd

# In-memory DataFrame
df = pd.DataFrame()

# From file
df = pd.read_pickle('mydata.pkl')
```

---

## Loading Data

### VFrames

```v
// Auto-detect format (CSV, JSON, Parquet)
df := ctx.read_auto('data.csv')!

// From records
data := [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]
df := ctx.read_records(data)!
```

### Pandas

```python
import pandas as pd

# From CSV
df = pd.read_csv('data.csv')

# From dictionary
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]
df = pd.DataFrame(data)
```

---

## Exploring Data

### VFrames

```v
// First 5 rows
df.head(5)

// Last 5 rows  
df.tail(5)

// Shape (rows, columns)
shape := df.shape()  // returns [rows, cols]

// Column names
cols := df.columns()

// Data types
dtypes := df.dtypes()

// Summary statistics
df.describe()

// Full info
df.info()
```

### Pandas

```python
# First 5 rows
df.head(5)

# Last 5 rows
df.tail(5)

# Shape
df.shape  # returns (rows, cols)

# Column names
df.columns

# Data types
df.dtypes

# Summary statistics
df.describe()

# Full info
df.info()
```

---

## Selecting Columns

### VFrames

```v
// Select specific columns
df2 := df.select(['name', 'age'])

// Select subset with new column
df2 := df.subset(['name', 'age'])

// Add prefix/suffix
df2 := df.add_prefix('col_')
df2 := df.add_suffix('_col')
```

### Pandas

```python
# Select specific columns
df2 = df[['name', 'age']]

# Add prefix/suffix
df2 = df.add_prefix('col_')
df2 = df.add_suffix('_col')
```

---

## Filtering Rows

### VFrames

```v
// Filter by condition
df2 := df.filter('age', '> 25')!

// Query with SQL-like syntax
df2 := df.query('age > 25', vframes.DFConfig{})!

// Isin filter (create boolean mask)
mask := df.isin(['Alice', 'Bob'])!
```

### Pandas

```python
# Filter by condition
df2 = df[df['age'] > 25]

# Query
df2 = df.query('age > 25')

# Isin
mask = df.isin(['Alice', 'Bob'])
```

---

## Adding/Deleting Columns

### VFrames

```v
// Add column with expression
df2 := df.add_column('age_doubled', 'age * 2')

// Delete column
df2 := df.delete_column('name')

// Rename columns
df2 := df.rename({'old_name': 'new_name'})
```

### Pandas

```python
# Add column
df['age_doubled'] = df['age'] * 2

# Or with assign
df2 = df.assign(age_doubled=lambda x: x['age'] * 2)

# Delete column
df2 = df.drop(columns=['name'])

# Rename columns
df2 = df.rename(columns={'old_name': 'new_name'})
```

---

## Grouping and Aggregation

### VFrames

```v
// Group by and aggregate
df2 := df.group_by(['department'], {
    'salary_avg': 'avg(salary)',
    'count': 'count(*)'
})

// Multiple aggregations
df2 := df.agg({
    'salary': 'mean',
    'age': 'max'
})
```

### Pandas

```python
# Group by and aggregate
df2 = df.groupby('department').agg({
    'salary': 'mean',
    'age': 'count'
}).reset_index()

# Or
df2 = df.groupby('department')['salary'].mean()
```

---

## Sorting

### VFrames

```v
// Sort by values
df2 := df.sort_values(['age'], vframes.SortValuesOptions{
    ascending: true
})

// Sort by index
df2 := df.sort_index(false)  // descending
```

### Pandas

```python
# Sort by values
df2 = df.sort_values(by=['age'], ascending=True)

# Sort by index
df2 = df.sort_index(ascending=False)
```

---

## Merging DataFrames

### VFrames

```v
// Merge (SQL join)
df3 := df1.merge(df2, vframes.MergeOptions{
    on: 'id'
})

// Join
df3 := df1.join(df2, vframes.JoinOptions{
    on: 'id'
})

// Concatenate
df3 := df1.concat([df2, df3], vframes.ConcatOptions{})
```

### Pandas

```python
# Merge
df3 = pd.merge(df1, df2, on='id')

# Join
df3 = df1.join(df2, on='id')

# Concatenate
df3 = pd.concat([df1, df2, df3])
```

---

## Exporting Data

### VFrames

```v
// To CSV
df.to_csv('output.csv', vframes.ToCsvOptions{
    delimiter: ','
})!

// To JSON
df.to_json('output.json')!

// To Parquet
df.to_parquet('output.parquet')!
```

### Pandas

```python
# To CSV
df.to_csv('output.csv')

# To JSON
df.to_json('output.json')

# To Parquet
df.to_parquet('output.parquet')
```

---

## Handling Missing Values

### VFrames

```v
// Drop rows with NA
df2 := df.dropna(vframes.DropOptions{
    how: 'any'  // or 'all'
})

// Fill NA with value
df2 := df.fillna(vframes.FillnaOptions{
    value: '0'
})

// Forward fill
df2 := df.ffill()

// Backward fill
df2 := df.bfill()

// Check for NA
mask := df.isna()
```

### Pandas

```python
# Drop rows with NA
df2 = df.dropna(how='any')

# Fill NA with value
df2 = df.fillna(0)

# Forward fill
df2 = df.ffill()

# Backward fill
df2 = df.bfill()

# Check for NA
mask = df.isna()
```

---

## Common Operations Comparison

| Operation | VFrames | Pandas |
|-----------|---------|--------|
| Create | `ctx.read_auto()` | `pd.read_csv()` |
| Head | `df.head(n)` | `df.head(n)` |
| Tail | `df.tail(n)` | `df.tail(n)` |
| Shape | `df.shape()` | `df.shape` |
| Columns | `df.columns()` | `df.columns` |
| Select | `df.select(['a','b'])` | `df[['a','b']]` |
| Filter | `df.filter('col', '> 5')` | `df[df['col'] > 5]` |
| Add column | `df.add_column('new', 'a+b')` | `df['new'] = df['a'] + df['b']` |
| Delete | `df.delete_column('col')` | `df.drop(columns=['col'])` |
| Rename | `df.rename({...})` | `df.rename(columns={...})` |
| Group by | `df.group_by([...], {...})` | `df.groupby(...).agg(...)` |
| Sort | `df.sort_values([...])` | `df.sort_values(by=[...])` |
| Merge | `df1.merge(df2, {...})` | `pd.merge(df1, df2, ...)` |
| Fill NA | `df.fillna({...})` | `df.fillna(...)` |
| Export CSV | `df.to_csv(...)` | `df.to_csv(...)` |

---

## Error Handling

VFrames uses V's error handling with `!` and `or` blocks:

```v
df := ctx.read_auto('file.csv') or {
    eprintln('Failed to load: ${err.msg()}')
    return
}

// Or with default value
df := ctx.read_auto('file.csv') or { vframes.empty() }
```

---

## Performance Tips

1. **Use Persisted Context**: For large datasets, use persisted context:
   ```v
   mut ctx := vframes.init(location: 'data.db')
   ```

2. **Chain Operations**: VFrames supports method chaining:
   ```v
   df := ctx.read_auto('data.csv')!
       .add_column('new_col', 'old_col * 2')!
       .filter('new_col', '> 100')!
       .sort_values(['new_col'], SortValuesOptions{})
   ```

3. **Use SQL Directly**: For complex operations, use `query()`:
   ```v
   df2 := df.query('SELECT col1, SUM(col2) as total GROUP BY col1', DFConfig{})!
   ```

---

## Next Steps

- Check [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md) for available functions
- Explore [examples/](examples/) for more use cases
- Contribute by adding new functions or fixing bugs
