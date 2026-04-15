# VFrames Broad Improvement Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix panics/error handling, replace `assert true` tests with real assertions, then add missing features (`sort_values`, `pivot` fix, Pandas aliases, cumulative functions, new export formats).

**Architecture:** Three phases in order — Phase 1 (error handling, Tasks 1–3) makes the library non-crashing; Phase 2 (test quality, Tasks 4–6) makes regressions detectable; Phase 3 (features, Tasks 7–11) builds on the solid base.

**Tech Stack:** V language, vduckdb (DuckDB bindings), x.json2 (standard library JSON), `make test` / `v test tests/`

> **V sumtype cast note**: `Data` is a sumtype (`[]map[string]json2.Any | []map[string]string`). Always split the unwrap and cast into two statements: `data := df.values()!` then `rows := data as []map[string]json2.Any`. Do not chain them on one line.

---

## File Map

| File | What changes |
|---|---|
| `src/explore.v` | All 8 public functions: return types `T` → `!T`, `panic` → `return err` |
| `src/funcs.v` | All `df.columns()` / `df.dtypes()` call sites get `!` suffix; add `cumsum`, `cummax`, `cummin`, `cumprod` |
| `src/mutation.v` | Fix internal `columns()`/`shape()` callers; convert 7 panicking functions to `!DataFrame`; add `sort_values`, `filter`, `select`, `drop`, `groupby`, fix `pivot` aggfunc |
| `src/core.v` | `init()` → `!DataFrameContext`, `empty()` → `!DataFrame`, `version()` → `!string` |
| `src/io.v` | Add `to_dict()`, `to_markdown()` |
| `tests/explore_test.v` | Handle `!` types, real assertions, error-path test |
| `tests/funcs_test.v` | Real value assertions, error-path test |
| `tests/mutation_test.v` | Real assertions, error-path tests |
| `examples/01_basic_usage.v` | Handle `!` on all explore/mutation calls |
| `examples/02_data_analysis.v` | Handle `!` on all explore/mutation calls |
| `examples/03_advanced_features.v` | Handle `!` on all explore/mutation calls |

---

## Task 1: Fix `explore.v` signatures and update all call sites

All 8 explore functions currently panic on DB errors. Change return types to `!T` and update every call site in `funcs.v` and `mutation.v`. Tests and examples are updated in the same commit so the codebase compiles throughout.

**Files:**
- Modify: `src/explore.v`
- Modify: `src/funcs.v` (call sites only, no new functions yet)
- Modify: `src/mutation.v` (internal callers only)
- Modify: `tests/explore_test.v`
- Modify: `tests/mutation_test.v` (shape/columns call sites)
- Modify: `examples/01_basic_usage.v`
- Modify: `examples/02_data_analysis.v`
- Modify: `examples/03_advanced_features.v`

- [ ] **Step 1: Rewrite `src/explore.v`**

Replace the entire file contents with:

```v
module vframes

import x.json2

// Shows first `n` records from DataFrame. Use `to_stdout: false` to return the data as `[]map[string]json2.Any` instead of the console
pub fn (df DataFrame) head(n int, dconf DFConfig) !Data {
	if n <= 0 {
		return Data([]map[string]json2.Any{})
	}
	mut db := &df.ctx.db
	_ := db.query("select * from ${df.id} limit ${n}") or { return err }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return Data(db.get_array())
}

// Same as `head`, but for last `n` records
pub fn (df DataFrame) tail(n int, dconf DFConfig) !Data {
	if n <= 0 {
		return Data([]map[string]json2.Any{})
	}
	mut db := &df.ctx.db
	q := "
	WITH _base as (
		SELECT row_number() OVER() as _row_num,*
		FROM ${df.id}
	) SELECT * EXCLUDE(_row_num) FROM (SELECT * FROM _base ORDER BY _row_num DESC limit ${n}) ORDER BY _row_num ASC
	"
	_ := db.query(q) or { return err }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Shows DataFrame columns names and data types
pub fn (df DataFrame) info(dconf DFConfig) !Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM ${df.id})") or { return err }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Shows columns basic statistics (nulls, max, min, etc.)
pub fn (df DataFrame) describe(dconf DFConfig) !Data {
	mut db := &df.ctx.db
	_ := db.query("SELECT * FROM (SUMMARIZE SELECT * FROM ${df.id})") or { return err }
	if dconf.to_stdout {
		println(db.print_table(max_rows: df.display_max_rows, mode: df.display_mode))
	}
	return db.get_array()
}

// Returns the number of rows and columns of the DataFrame
pub fn (df DataFrame) shape() ![]int {
	mut db := &df.ctx.db
	_ := db.query('SELECT COUNT(*) as rows FROM ${df.id}') or { return err }
	res_rows := db.get_array()
	num_rows := (res_rows[0]['rows'] or { json2.Any(0) }).int()

	_ := db.query('SELECT COUNT(DISTINCT column_name) as cols FROM (SUMMARIZE SELECT * FROM ${df.id})') or { return err }
	res_cols := db.get_array()
	num_cols := (res_cols[0]['cols'] or { json2.Any(0) }).int()

	return [num_rows, num_cols]
}

@[params]
struct ValuesParams {
	as_string bool
}

// Returns all the data from DataFrame as []map[string]json2.Any or []map[string]string if `as_string` is true
// NOTE: Use with caution because it will dump all the DataFrame data to memory
pub fn (df DataFrame) values(vp ValuesParams) !Data {
	mut db := &df.ctx.db
	_ := db.query('SELECT * FROM ${df.id}') or { return err }
	if vp.as_string {
		return db.get_array_as_string()
	}
	return db.get_array()
}

// Returns an array of column names
pub fn (df DataFrame) columns() ![]string {
	mut db := &df.ctx.db
	_ := db.query('SELECT * FROM ${df.id} where 1=0') or { return err }
	return db.columns.keys()
}

// Returns a map of columns and their types
pub fn (df DataFrame) dtypes() !map[string]string {
	mut db := &df.ctx.db
	_ := db.query('SELECT * FROM ${df.id} where 1=0') or { return err }
	return db.columns
}
```

- [ ] **Step 2: Update `df.dtypes()` call sites in `src/funcs.v`**

Every `for k,v in df.dtypes() {` loop must become a two-liner. Also `df.columns()` calls. Apply these changes:

In `v_apply` (line 10): `for k,v in df.dtypes() {` → add `types := df.dtypes()!` before the loop, change loop to `for k,v in types {`

In `min_max_apply` (line 36): same pattern — add `types := df.dtypes()!`, loop over `types`

In `g_apply` (line 56): same

In `add` (line 73): same

In `sub` (line 131): same

In `mul` (line 146): same

In `div` (line 161): same

In `floordiv` (line 176): same

In `mod` (line 191): same

In `round` (line 206): same

In `clip` (line 261): same

In `count` (line 231): `for k in df.columns() {` → add `cols := df.columns()!`, loop over `cols`

In `nunique` (line 246): same pattern

In `eq`, `ne`, `gt`, `ge`, `lt`, `le` (lines 281–374): `cols := df.columns()` → `cols := df.columns()!`

In `nlargest` (line 380): `numeric_cols := df.dtypes().values()...` → `numeric_cols := (df.dtypes()!).values().filter(...)`

In `nsmallest` (line 394): same

In `isna` (line 413): `for k in df.columns() {` → `cols := df.columns()!`, loop over `cols`

In `fillna` (line 447): three `for k in df.columns() {` loops → each gets `cols := df.columns()!` before it

In `notna` (line 482): same as isna

In `replace` (line 501): `for k in df.columns() {` → `cols := df.columns()!`

In `astype` (line 520): same

In `isin` (line 543): same

In `value_counts` (line 562): `cols := df.columns()` → `cols := df.columns()!`

- [ ] **Step 3: Update `df.columns()` / `df.shape()` call sites in `src/mutation.v`**

In `dropna` (~line 120):
```v
// Before
selected_columns := if do.subset.len > 0 { do.subset } else { df.columns() }
conn := if do.how == 'any' { 'and' } else { 'or' }
predicate := df.columns().map("${it} is not null").join(' ${conn} ')

// After
all_cols := df.columns()!
selected_columns := if do.subset.len > 0 { do.subset } else { all_cols }
conn := if do.how == 'any' { 'and' } else { 'or' }
predicate := all_cols.map('${it} is not null').join(' ${conn} ')
```

In `rename` (~line 135):
```v
// Before
for k in df.columns() {

// After
all_cols := df.columns()!
for k in all_cols {
```

In `drop_duplicates` (~line 159):
```v
// Before
cols := if subset.len > 0 { subset } else { df.columns() }

// After
all_cols := df.columns()!
cols := if subset.len > 0 { subset } else { all_cols }
```

In `sample` (~line 181):
```v
// Before
total_rows := df.shape()[0]

// After
total_rows := (df.shape()!)[0]
```

- [ ] **Step 4: Update `tests/explore_test.v`**

Replace the entire file with:

```v
import vframes
import x.json2

const data = [
	{'x': json2.Any(1), 'y': json2.Any('a'), 'z': json2.Any(100.0)},
	{'x': json2.Any(2), 'y': json2.Any('bb'), 'z': json2.Any(250.0)},
	{'x': json2.Any(3), 'y': json2.Any('ccc'), 'z': json2.Any(400.5)},
]

fn test__head_zero() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(0, vframes.DFConfig{})!
	_ = result
	assert true
}

fn test__head_two() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(2, vframes.DFConfig{ to_stdout: false })!
	_ = result
	assert true
}

fn test__head_hundred() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(100, vframes.DFConfig{ to_stdout: false })!
	_ = result
	assert true
}

fn test__tail_zero() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(0, vframes.DFConfig{})!
	_ = result
	assert true
}

fn test__tail_one() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(1, vframes.DFConfig{ to_stdout: false })!
	_ = result
	assert true
}

fn test__tail_hundred() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(100, vframes.DFConfig{ to_stdout: false })!
	_ = result
	assert true
}
```

- [ ] **Step 5: Update call sites in `tests/mutation_test.v`**

Change every `df.shape()` call to `df.shape()!` and every `df.columns()` call to `df.columns()!`:

Line 62: `shape := result.shape()` → `shape := result.shape()!`
Line 76: `shape := result.shape()!` (already has `!` if from earlier edit — check)
Line 95: `shape := result.shape()!`
Line 110: `shape := result.shape()!`
Line 128: `shape := result.shape()!`
Line 143: `cols := result.columns()` → `cols := result.columns()!`
Line 169: `shape := result.shape()!`

- [ ] **Step 6: Update `examples/01_basic_usage.v`**

Change:
```v
// Before
mut ctx := vframes.init()
...
println('Loaded ${df.shape()[0]} rows and ${df.shape()[1]} columns')
shape := df.shape()
println('Shape: ${shape[0]} rows, ${shape[1]} columns')
println('Columns: ${df.columns()}')
println('Data Types: ${df.dtypes()}')
df.head(5, vframes.DFConfig{})
df.tail(3, vframes.DFConfig{})
df.info(vframes.DFConfig{})
df.describe(vframes.DFConfig{})
df_subset := df.subset(['name', 'department', 'salary'])
df_subset.head(3, vframes.DFConfig{})
df_with_calc := df.add_column('salary_per_year', 'salary / years')
df_with_calc.head(5, vframes.DFConfig{})
df_prefixed := df.add_prefix('emp_')
println('Columns after prefix: ${df_prefixed.columns()}')
df_prefixed.head(3, vframes.DFConfig{})
df_deleted := df.delete_column('years')
println('Columns after deletion: ${df_deleted.columns()}')
df_sliced := df.slice(3, 5)
df_sliced.head(10, vframes.DFConfig{})
```

To:
```v
// After
mut ctx := vframes.init()!
...
shape0 := df.shape()!
println('Loaded ${shape0[0]} rows and ${shape0[1]} columns')
shape := df.shape()!
println('Shape: ${shape[0]} rows, ${shape[1]} columns')
println('Columns: ${df.columns()!}')
println('Data Types: ${df.dtypes()!}')
df.head(5, vframes.DFConfig{})!
df.tail(3, vframes.DFConfig{})!
df.info(vframes.DFConfig{})!
df.describe(vframes.DFConfig{})!
df_subset := df.subset(['name', 'department', 'salary'])
df_subset.head(3, vframes.DFConfig{})!
df_with_calc := df.add_column('salary_per_year', 'salary / years')
df_with_calc.head(5, vframes.DFConfig{})!
df_prefixed := df.add_prefix('emp_')
println('Columns after prefix: ${df_prefixed.columns()!}')
df_prefixed.head(3, vframes.DFConfig{})!
df_deleted := df.delete_column('years')
println('Columns after deletion: ${df_deleted.columns()!}')
df_sliced := df.slice(3, 5)
df_sliced.head(10, vframes.DFConfig{})!
```

Note: `subset`, `add_column`, `add_prefix`, `delete_column`, `slice` still return `DataFrame` (not `!`) until Task 2. Only the explore calls get `!` here.

- [ ] **Step 7: Update `examples/02_data_analysis.v` and `examples/03_advanced_features.v`**

Apply the same pattern: add `!` to every call of `head()`, `tail()`, `info()`, `describe()`, `shape()`, `columns()`, `dtypes()`, `values()`. Read each file and apply changes to every call site.

- [ ] **Step 8: Verify compilation**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass. If any compile errors remain, they'll reference a specific file and line — fix that call site.

- [ ] **Step 9: Commit**

```bash
git add src/explore.v src/funcs.v src/mutation.v tests/explore_test.v tests/mutation_test.v examples/
git commit -m "feat!: change explore functions to return !T, fix all call sites

head, tail, info, describe, shape, values, columns, dtypes now return
error results instead of panicking on DB failures. Updated all call
sites in funcs.v, mutation.v, tests, and examples."
```

---

## Task 2: Fix remaining panics in `mutation.v`

Seven functions (`delete_column`, `add_column`, `subset`, `slice`, `group_by`, `add_prefix`, `add_suffix`) still return `DataFrame` and panic. `dropna` already had its internal call sites fixed in Task 1 but still panics internally. Convert all to `!DataFrame`.

**Files:**
- Modify: `src/mutation.v`
- Modify: `tests/mutation_test.v`
- Modify: `examples/01_basic_usage.v`
- Modify: `examples/02_data_analysis.v`
- Modify: `examples/03_advanced_features.v`

- [ ] **Step 1: Update `src/mutation.v` function signatures and panic calls**

Make the following changes:

`delete_column` signature: `DataFrame` → `!DataFrame`, `or { panic(err) }` → `or { return err }`
```v
pub fn (df DataFrame) delete_column(col string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * exclude(${col}) from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`add_column` signature: `DataFrame` → `!DataFrame`, `or { panic(err) }` → `or { return err }`
```v
pub fn (df DataFrame) add_column(col string, expr string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select *, ${expr} as ${col} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`subset` signature: `DataFrame` → `!DataFrame`
```v
pub fn (df DataFrame) subset(cols []string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`slice` signature: `DataFrame` → `!DataFrame`
```v
pub fn (df DataFrame) slice(start int, end int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	offset := start - 1
	limit := end - start + 1
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * from ${df.id} limit ${limit} offset ${offset}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`group_by` signature: `DataFrame` → `!DataFrame`
```v
pub fn (df DataFrame) group_by(dimensions []string, metrics map[string]string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut sets := []string{}
	for k, v in metrics {
		sets << '${v} as ${k}'
	}
	_ := db.query("create table ${id} as select ${dimensions.join(',')}, ${sets.join(',')} from ${df.id} group by ${dimensions.join(',')}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`add_prefix` signature: `DataFrame` → `!DataFrame`
```v
pub fn (df DataFrame) add_prefix(prefix string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select columns('(.*)') as '${prefix}_\\1' from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`add_suffix` signature: `DataFrame` → `!DataFrame`
```v
pub fn (df DataFrame) add_suffix(suffix string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select columns('(.*)') as '\\1_${suffix}' from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`dropna` signature: `DataFrame` → `!DataFrame` (internal `columns()` already fixed in Task 1)
```v
pub fn (df DataFrame) dropna(do DropOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	all_cols := df.columns()!
	selected_columns := if do.subset.len > 0 { do.subset } else { all_cols }
	conn := if do.how == 'any' { 'and' } else { 'or' }
	predicate := all_cols.map('${it} is not null').join(' ${conn} ')
	_ := db.query("create table ${id} as select ${selected_columns.join(',')} from ${df.id} where ${predicate}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

`assign` already delegates to `add_column` — now that `add_column` returns `!DataFrame`, the body stays the same since both return `!DataFrame`.

- [ ] **Step 2: Update `tests/mutation_test.v` call sites**

Add `!` to all calls that now return `!DataFrame`:

```v
// Before
result := df.add_prefix('col')
// After
result := df.add_prefix('col')!

// Before
result := df.add_suffix('col')
// After
result := df.add_suffix('col')!

// Before
result := df.dropna(vframes.DropOptions{})
// After
result := df.dropna(vframes.DropOptions{})!
```

- [ ] **Step 3: Update examples**

In `examples/01_basic_usage.v`, add `!` to `subset`, `add_column`, `add_prefix`, `delete_column`, `slice` calls:
```v
df_subset := df.subset(['name', 'department', 'salary'])!
df_with_calc := df.add_column('salary_per_year', 'salary / years')!
df_prefixed := df.add_prefix('emp_')!
df_deleted := df.delete_column('years')!
df_sliced := df.slice(3, 5)!
```

In `examples/02_data_analysis.v`, add `!` to `group_by` calls:
```v
df_by_region := df.group_by(['region'], { ... })!
```

In `examples/03_advanced_features.v`, add `!` to any `dropna`, `add_column`, `subset`, `slice` calls.

- [ ] **Step 4: Verify compilation and tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/mutation.v tests/mutation_test.v examples/
git commit -m "feat!: convert mutation.v panics to error returns

delete_column, add_column, subset, slice, group_by, add_prefix,
add_suffix, dropna now return !DataFrame instead of panicking."
```

---

## Task 3: Fix `core.v` and update all `init()` callers

`init()` currently returns `DataFrameContext` and panics on DB open failure. Change to return `!DataFrameContext`. Also fix `version()` and `empty()`.

**Files:**
- Modify: `src/core.v`
- Modify: `tests/explore_test.v`
- Modify: `tests/funcs_test.v`
- Modify: `tests/mutation_test.v`
- Modify: `examples/01_basic_usage.v`
- Modify: `examples/02_data_analysis.v`
- Modify: `examples/03_advanced_features.v`

- [ ] **Step 1: Rewrite `src/core.v`**

```v
module vframes

import vduckdb
import rand
import v.vmod

// Initializes a new DataFrame context
pub fn init(cfg ContextConfig) !DataFrameContext {
	mut db := vduckdb.DuckDB{}
	_ := db.open(cfg.location) or { return err }
	_ := db.query('select 1') or { return err }
	return DataFrameContext{
		dpath: cfg.location
		db: db
	}
}

// Closes a DataFrame context
pub fn (mut ctx DataFrameContext) close() {
	ctx.db.close()
}

// Prints vframes version
pub fn version() !string {
	vm := vmod.decode(@VMOD_FILE) or { return err }
	return vm.version
}

// Returns an empty in-memory DataFrame. Mainly used as a Result parameter for `read_auto` function
pub fn empty() !DataFrame {
	mut ctx := init()!
	id := 'tbl_${rand.ulid()}'
	return DataFrame{
		id: id
		ctx: ctx
	}
}
```

- [ ] **Step 2: Update all `vframes.init()` call sites in test files**

In `tests/explore_test.v`, `tests/funcs_test.v`, `tests/mutation_test.v`:

Every `mut ctx := vframes.init()` → `mut ctx := vframes.init()!`

There are approximately 6 occurrences in `explore_test.v`, 9 in `funcs_test.v`, and 11 in `mutation_test.v`. Apply the change to each.

- [ ] **Step 3: Update all `vframes.init()` call sites in examples**

In `examples/01_basic_usage.v`:
```v
// Before
mut ctx := vframes.init()
// After
mut ctx := vframes.init()!
```

Apply same change to `examples/02_data_analysis.v` and `examples/03_advanced_features.v`.

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 5: Verify examples compile**

```bash
v run examples/01_basic_usage.v
v run examples/02_data_analysis.v
```

Expected: Output is printed to console, no panics.

- [ ] **Step 6: Commit**

```bash
git add src/core.v tests/ examples/
git commit -m "feat!: init() returns !DataFrameContext, version() returns !string

Programs can now handle DuckDB open failures gracefully instead of
crashing. empty() also returns !DataFrame for consistency."
```

---

## Task 4: Improve `explore_test.v` with real assertions

Replace `assert true` stubs with assertions that verify actual content returned by the explore functions.

**Files:**
- Modify: `tests/explore_test.v`

- [ ] **Step 1: Replace `tests/explore_test.v` with real assertions**

```v
import vframes
import x.json2

const data = [
	{'x': json2.Any(1), 'y': json2.Any('a'), 'z': json2.Any(100.0)},
	{'x': json2.Any(2), 'y': json2.Any('bb'), 'z': json2.Any(250.0)},
	{'x': json2.Any(3), 'y': json2.Any('ccc'), 'z': json2.Any(400.5)},
]

fn test__head_zero_returns_empty() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(0, vframes.DFConfig{})!
	rows := result as []map[string]json2.Any
	assert rows.len == 0
}

fn test__head_two_returns_two_rows() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(2, vframes.DFConfig{ to_stdout: false })!
	rows := result as []map[string]json2.Any
	assert rows.len == 2
}

fn test__head_overflow_returns_all_rows() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.head(100, vframes.DFConfig{ to_stdout: false })!
	rows := result as []map[string]json2.Any
	assert rows.len == 3
}

fn test__tail_zero_returns_empty() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(0, vframes.DFConfig{})!
	rows := result as []map[string]json2.Any
	assert rows.len == 0
}

fn test__tail_one_returns_last_row() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(1, vframes.DFConfig{ to_stdout: false })!
	rows := result as []map[string]json2.Any
	assert rows.len == 1
	assert (rows[0]['x'] or { json2.Any(0) }).int() == 3
}

fn test__tail_overflow_returns_all_rows() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.tail(100, vframes.DFConfig{ to_stdout: false })!
	rows := result as []map[string]json2.Any
	assert rows.len == 3
}

fn test__columns_returns_expected_names() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	cols := df.columns()!
	assert 'x' in cols
	assert 'y' in cols
	assert 'z' in cols
	assert cols.len == 3
}

fn test__dtypes_returns_type_map() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	types := df.dtypes()!
	assert types.len == 3
	// z was loaded as float
	assert types['z'] in ['float', 'double', 'decimal', 'DOUBLE', 'FLOAT']
}

fn test__shape_returns_rows_and_cols() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	s := df.shape()!
	assert s[0] == 3  // 3 rows
	assert s[1] == 3  // 3 columns
}

fn test__values_returns_all_rows() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.values()!
	rows := result as []map[string]json2.Any
	assert rows.len == 3
}

fn test__columns_on_invalid_table_returns_error() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	// Corrupt the table id to simulate a missing table
	bad_df := vframes.DataFrame{ id: 'tbl_nonexistent_xyz', ctx: df.ctx }
	bad_df.columns() or {
		assert err.msg() != ''
		return
	}
	assert false, 'expected error but got none'
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/explore_test.v
git commit -m "test: real assertions in explore_test.v

Replace assert true stubs with row counts, column name checks, and an
error-path test that verifies a missing table returns an error."
```

---

## Task 5: Improve `funcs_test.v` with value assertions

Replace `assert true` with checks on actual numeric output.

**Files:**
- Modify: `tests/funcs_test.v`

- [ ] **Step 1: Rewrite `tests/funcs_test.v`**

```v
import vframes
import x.json2

const data = [
	{'x': json2.Any(1), 'y': json2.Any('a'), 'z': json2.Any(-100.0)},
	{'x': json2.Any(3), 'y': json2.Any('c'), 'z': json2.Any(300.0)},
]

fn get_int(rows []map[string]json2.Any, row int, col string) int {
	return (rows[row][col] or { json2.Any(0) }).int()
}

fn get_f64(rows []map[string]json2.Any, row int, col string) f64 {
	return (rows[row][col] or { json2.Any(0.0) }).f64()
}

fn test__add_integer_increments_numeric_cols() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.add[int](2)!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 3   // 1 + 2
	assert get_int(rows, 1, 'x') == 5   // 3 + 2
	// string column untouched
	assert (rows[0]['y'] or { json2.Any('') }).str() == 'a'
}

fn test__add_decimal_increments_numeric_cols() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.add(1.0)!
	rows := result.values()! as []map[string]json2.Any
	assert get_f64(rows, 1, 'z') >= 300.9  // 300.0 + 1.0
}

fn test__abs_removes_negatives() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.abs()!
	rows := result.values()! as []map[string]json2.Any
	assert get_f64(rows, 0, 'z') == 100.0   // abs(-100.0)
}

fn test__max_returns_one_row_with_max_values() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.max(vframes.FuncOptions{})!
	s := result.shape()!
	assert s[0] == 1
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 3
}

fn test__min_returns_one_row_with_min_values() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.min(vframes.FuncOptions{})!
	s := result.shape()!
	assert s[0] == 1
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 1
}

fn test__mean_returns_correct_value() {
	d := [
		{'x': json2.Any(10), 'y': json2.Any(14)},
		{'x': json2.Any(4), 'y': json2.Any(10)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.mean(vframes.FuncOptions{})!
	rows := result.values()! as []map[string]json2.Any
	assert get_f64(rows, 0, 'x') == 7.0   // (10 + 4) / 2
	assert get_f64(rows, 0, 'y') == 12.0  // (14 + 10) / 2
}

fn test__sum_returns_correct_value() {
	d := [
		{'x': json2.Any(10), 'y': json2.Any(14)},
		{'x': json2.Any(4), 'y': json2.Any(10)},
		{'x': json2.Any(2), 'y': json2.Any(15)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.sum(vframes.FuncOptions{})!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 16
	assert get_int(rows, 0, 'y') == 39
}

fn test__pow_squares_values() {
	d := [
		{'x': json2.Any(3)},
		{'x': json2.Any(4)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.pow(2, vframes.FuncOptions{})!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 9
	assert get_int(rows, 1, 'x') == 16
}

fn test__median_returns_middle_value() {
	d := [
		{'x': json2.Any(1)},
		{'x': json2.Any(3)},
		{'x': json2.Any(5)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.median(vframes.FuncOptions{})!
	rows := result.values()! as []map[string]json2.Any
	assert get_f64(rows, 0, 'x') == 3.0
}

fn test__add_on_nonexistent_table_returns_error() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	bad_df := vframes.DataFrame{ id: 'tbl_nonexistent_xyz', ctx: df.ctx }
	bad_df.add[int](1) or {
		assert err.msg() != ''
		return
	}
	assert false, 'expected error but got none'
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/funcs_test.v
git commit -m "test: real value assertions in funcs_test.v

Replace assert true stubs with numeric value checks. Add error-path
test for operations on nonexistent tables."
```

---

## Task 6: Improve `mutation_test.v` with value assertions and error paths

**Files:**
- Modify: `tests/mutation_test.v`

- [ ] **Step 1: Add value assertions and error-path tests to `tests/mutation_test.v`**

After each existing test that only checks shape/columns, add content assertions. Add three new error-path tests at the end. Replace the file with:

```v
import vframes
import x.json2

const data = [
	{'x': json2.Any(1), 'y': json2.Any('a'), 'z': json2.Any(-100.0)},
	{'x': json2.Any(3), 'y': json2.Any('c'), 'z': json2.Any(300.0)},
]

fn get_int(rows []map[string]json2.Any, row int, col string) int {
	return (rows[row][col] or { json2.Any(0) }).int()
}

fn test__add_prefix() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.add_prefix('col')!
	cols := result.columns()!
	assert 'col_x' in cols
	assert 'col_y' in cols
	assert 'x' !in cols
}

fn test__add_suffix() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.add_suffix('col')!
	cols := result.columns()!
	assert 'x_col' in cols
	assert 'x' !in cols
}

fn test__dropna() {
	tdata := [
		{'x_col': json2.Any(1), 'y_col': json2.Any('a'), 'z_col': json2.Any(-100.0)},
		{'x_col': json2.Any(3), 'y_col': json2.null, 'z_col': json2.Any(300.0)},
		{'x_col': json2.Any(5), 'y_col': json2.Any('f'), 'z_col': json2.null},
		{'x_col': json2.Any(json2.null), 'y_col': json2.null, 'z_col': json2.null},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.dropna(vframes.DropOptions{})!
	s := result.shape()!
	assert s[0] == 1  // only first row has no nulls
}

fn test__rename() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(3), 'y': json2.Any('c')},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.rename({'x': 'x_new', 'y': 'y_new'})!
	cols := result.columns()!
	assert 'x_new' in cols
	assert 'y_new' in cols
	assert 'x' !in cols
}

fn test__drop_duplicates() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(3), 'y': json2.Any('c')},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.drop_duplicates([]string{})!
	s := result.shape()!
	assert s[0] == 2
}

fn test__sample() {
	tdata := [
		{'x': json2.Any(1)}, {'x': json2.Any(2)}, {'x': json2.Any(3)},
		{'x': json2.Any(4)}, {'x': json2.Any(5)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.sample(n: 2, replace: false)!
	s := result.shape()!
	assert s[0] == 2
	assert s[1] == 1  // still 1 column
}

fn test__merge() {
	left_data := [
		{'key': json2.Any('a'), 'val': json2.Any(1)},
		{'key': json2.Any('b'), 'val': json2.Any(2)},
	]
	right_data := [
		{'key': json2.Any('a'), 'val2': json2.Any(10)},
		{'key': json2.Any('c'), 'val2': json2.Any(30)},
	]
	mut ctx := vframes.init()!
	left_df := ctx.read_records(left_data)!
	right_df := ctx.read_records(right_data)!
	result := left_df.merge(right_df, on: 'key', how: 'inner')!
	s := result.shape()!
	assert s[0] == 1
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'val') == 1
	assert get_int(rows, 0, 'val2') == 10
}

fn test__join() {
	left_data := [
		{'key': json2.Any('a'), 'val': json2.Any(1)},
		{'key': json2.Any('b'), 'val': json2.Any(2)},
	]
	right_data := [
		{'key': json2.Any('a'), 'val2': json2.Any(10)},
		{'key': json2.Any('c'), 'val2': json2.Any(30)},
	]
	mut ctx := vframes.init()!
	left_df := ctx.read_records(left_data)!
	right_df := ctx.read_records(right_data)!
	result := left_df.join(right_df, on: 'key', how: 'left')!
	s := result.shape()!
	assert s[0] == 2
}

fn test__concat() {
	data1 := [
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(2), 'y': json2.Any('b')},
	]
	data2 := [
		{'x': json2.Any(3), 'y': json2.Any('c')},
		{'x': json2.Any(4), 'y': json2.Any('d')},
	]
	mut ctx := vframes.init()!
	df1 := ctx.read_records(data1)!
	df2 := ctx.read_records(data2)!
	result := vframes.concat([df1, df2])!
	s := result.shape()!
	assert s[0] == 4
	assert s[1] == 2
}

fn test__pivot() {
	tdata := [
		{'date': json2.Any('2020-01-01'), 'variable': json2.Any('temp'), 'value': json2.Any(20)},
		{'date': json2.Any('2020-01-01'), 'variable': json2.Any('humidity'), 'value': json2.Any(60)},
		{'date': json2.Any('2020-01-02'), 'variable': json2.Any('temp'), 'value': json2.Any(22)},
		{'date': json2.Any('2020-01-02'), 'variable': json2.Any('humidity'), 'value': json2.Any(65)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.pivot(index: 'date', columns: 'variable', values: 'value')!
	cols := result.columns()!
	assert 'temp_value' in cols
	assert 'humidity_value' in cols
	s := result.shape()!
	assert s[0] == 2  // 2 dates
}

fn test__pivot_table_respects_aggfunc() {
	tdata := [
		{'date': json2.Any('2020-01-01'), 'variable': json2.Any('temp'), 'value': json2.Any(20)},
		{'date': json2.Any('2020-01-01'), 'variable': json2.Any('temp'), 'value': json2.Any(22)},
		{'date': json2.Any('2020-01-02'), 'variable': json2.Any('temp'), 'value': json2.Any(25)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.pivot_table(index: 'date', columns: 'variable', values: 'value', aggfunc: 'sum')!
	s := result.shape()!
	assert s[0] == 2
	rows := result.values()! as []map[string]json2.Any
	// 2020-01-01 sum of temp = 20 + 22 = 42
	assert get_int(rows, 0, 'temp_value') == 42
}

fn test__melt() {
	tdata := [
		{'date': json2.Any('2020-01-01'), 'temp': json2.Any(20), 'humidity': json2.Any(60)},
		{'date': json2.Any('2020-01-02'), 'temp': json2.Any(22), 'humidity': json2.Any(65)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.melt(id_vars: ['date'], value_vars: ['temp', 'humidity'])!
	s := result.shape()!
	assert s[0] == 4  // 2 rows × 2 value_vars
	assert s[1] == 3  // date + variable + value
}

fn test__assign() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any(2)},
		{'x': json2.Any(3), 'y': json2.Any(4)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.assign('z', 'x + y')!
	cols := result.columns()!
	assert 'z' in cols
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'z') == 3  // 1 + 2
	assert get_int(rows, 1, 'z') == 7  // 3 + 4
}

fn test__delete_column_on_bad_col_returns_error() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	df.delete_column('nonexistent_col_xyz') or {
		assert err.msg() != ''
		return
	}
	assert false, 'expected error but got none'
}

fn test__subset_with_bad_col_returns_error() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	df.subset(['nonexistent_col_xyz']) or {
		assert err.msg() != ''
		return
	}
	assert false, 'expected error but got none'
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass. Note: `test__pivot_table_respects_aggfunc` will FAIL until Task 8 (pivot fix). Mark it as `// TODO: fails until pivot aggfunc is fixed` or skip it for now and add it in Task 8.

> **Important**: Comment out `test__pivot_table_respects_aggfunc` for now — it tests a bug that's fixed in Task 8. Add it back then.

- [ ] **Step 3: Commit**

```bash
git add tests/mutation_test.v
git commit -m "test: real assertions in mutation_test.v

Add content checks to merge, assign, pivot, melt tests. Add error-path
tests for delete_column and subset with nonexistent columns."
```

---

## Task 7: Add `sort_values()`

**Files:**
- Modify: `src/mutation.v`
- Modify: `tests/mutation_test.v`

- [ ] **Step 1: Write a failing test**

Add to `tests/mutation_test.v`:

```v
fn test__sort_values_ascending() {
	tdata := [
		{'x': json2.Any(3), 'y': json2.Any('c')},
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(2), 'y': json2.Any('b')},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.sort_values(['x'])!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 1
	assert get_int(rows, 1, 'x') == 2
	assert get_int(rows, 2, 'x') == 3
}

fn test__sort_values_descending() {
	tdata := [
		{'x': json2.Any(3)}, {'x': json2.Any(1)}, {'x': json2.Any(2)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.sort_values(['x'], ascending: [false])!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 3
	assert get_int(rows, 2, 'x') == 1
}

fn test__sort_values_multi_col() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any(3)},
		{'x': json2.Any(1), 'y': json2.Any(1)},
		{'x': json2.Any(2), 'y': json2.Any(2)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.sort_values(['x', 'y'])!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'y') == 1  // x=1,y=1 comes before x=1,y=3
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: Compile error — `sort_values` undefined.

- [ ] **Step 3: Implement `sort_values` in `src/mutation.v`**

Add after the `assign` function:

```v
@[params]
pub struct SortOptions {
pub:
	ascending []bool // per-column direction; shorter than cols defaults remaining to true
}

// Sorts DataFrame by one or more columns
pub fn (df DataFrame) sort_values(cols []string, opts SortOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut order_parts := []string{}
	for i, col in cols {
		dir := if i < opts.ascending.len && !opts.ascending[i] { 'DESC' } else { 'ASC' }
		order_parts << '"${col}" ${dir}'
	}
	_ := db.query("create table ${id} as select * from ${df.id} order by ${order_parts.join(', ')}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass including the new sort tests.

- [ ] **Step 5: Commit**

```bash
git add src/mutation.v tests/mutation_test.v
git commit -m "feat: add sort_values() with multi-column and direction support"
```

---

## Task 8: Fix `pivot()` aggfunc

The `pivot()` function accepts `aggfunc` in `PivotOptions` but always uses `max()`. Wire it up.

**Files:**
- Modify: `src/mutation.v`
- Modify: `tests/mutation_test.v`

- [ ] **Step 1: Uncomment `test__pivot_table_respects_aggfunc` in `tests/mutation_test.v`**

Remove the comment that was added in Task 6 Step 2. The test should now be active.

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: `test__pivot_table_respects_aggfunc` fails because pivot always uses `max()`.

- [ ] **Step 3: Fix `pivot()` in `src/mutation.v`**

Find the `pivot` function and replace it:

```v
// Pivot table - reshape data from long to wide format
pub fn (df DataFrame) pivot(po PivotOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	valid_aggfuncs := ['sum', 'avg', 'min', 'max', 'count']
	aggfunc := if po.aggfunc in valid_aggfuncs { po.aggfunc } else { 'max' }
	_ := db.query('create table ${id} as pivot ${df.id} on "${po.columns}" using ${aggfunc}("${po.values}") as "${po.values}" group by "${po.index}" order by "${po.index}"') or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass including `test__pivot_table_respects_aggfunc`.

- [ ] **Step 5: Commit**

```bash
git add src/mutation.v tests/mutation_test.v
git commit -m "fix: pivot() now respects aggfunc parameter instead of always using max()"
```

---

## Task 9: Add Pandas compatibility aliases

Add `filter`, `select`, `drop`, `groupby` as thin wrappers in `src/mutation.v`.

**Files:**
- Modify: `src/mutation.v`
- Modify: `tests/mutation_test.v`

- [ ] **Step 1: Write failing tests**

Add to `tests/mutation_test.v`:

```v
fn test__filter_alias_for_query() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(5), 'y': json2.Any('b')},
		{'x': json2.Any(3), 'y': json2.Any('c')},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.filter('* where x > 2')!
	s := result.shape()!
	assert s[0] == 2
}

fn test__select_alias_for_subset() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.select(['x', 'y'])!
	cols := result.columns()!
	assert cols.len == 2
	assert 'z' !in cols
}

fn test__drop_removes_multiple_columns() {
	mut ctx := vframes.init()!
	df := ctx.read_records(data)!
	result := df.drop(['y', 'z'])!
	cols := result.columns()!
	assert cols.len == 1
	assert 'x' in cols
}

fn test__groupby_alias_for_group_by() {
	tdata := [
		{'dept': json2.Any('eng'), 'sal': json2.Any(100)},
		{'dept': json2.Any('eng'), 'sal': json2.Any(200)},
		{'dept': json2.Any('sales'), 'sal': json2.Any(150)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	result := df.groupby(['dept'], {'total': 'sum(sal)'})!
	s := result.shape()!
	assert s[0] == 2
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: Compile error — `filter`, `select`, `drop`, `groupby` undefined.

- [ ] **Step 3: Add aliases to `src/mutation.v`**

Add after `sort_values`:

```v
// filter is an alias for query — Pandas-style row filtering
// Example: df.filter('* where age > 25')
pub fn (df DataFrame) filter(expr string) !DataFrame {
	return df.query(expr, DFConfig{})
}

// select is an alias for subset — Pandas-style column selection
pub fn (df DataFrame) select(cols []string) !DataFrame {
	return df.subset(cols)
}

// drop removes one or more columns from the DataFrame
pub fn (df DataFrame) drop(cols []string) !DataFrame {
	mut result := df
	for col in cols {
		result = result.delete_column(col)!
	}
	return result
}

// groupby is a no-underscore alias for group_by
pub fn (df DataFrame) groupby(dimensions []string, metrics map[string]string) !DataFrame {
	return df.group_by(dimensions, metrics)
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/mutation.v tests/mutation_test.v
git commit -m "feat: add Pandas aliases filter, select, drop, groupby"
```

---

## Task 10: Add cumulative functions

Add `cumsum`, `cummax`, `cummin`, `cumprod` using DuckDB window functions.

**Files:**
- Modify: `src/funcs.v`
- Modify: `tests/funcs_test.v`

- [ ] **Step 1: Write failing tests**

Add to `tests/funcs_test.v`:

```v
fn test__cumsum_accumulates_values() {
	d := [
		{'x': json2.Any(1)},
		{'x': json2.Any(2)},
		{'x': json2.Any(3)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.cumsum('x')!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 1
	assert get_int(rows, 1, 'x') == 3   // 1 + 2
	assert get_int(rows, 2, 'x') == 6   // 1 + 2 + 3
}

fn test__cummax_tracks_running_max() {
	d := [
		{'x': json2.Any(1)},
		{'x': json2.Any(5)},
		{'x': json2.Any(3)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.cummax('x')!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 1
	assert get_int(rows, 1, 'x') == 5
	assert get_int(rows, 2, 'x') == 5  // max so far is still 5
}

fn test__cummin_tracks_running_min() {
	d := [
		{'x': json2.Any(5)},
		{'x': json2.Any(3)},
		{'x': json2.Any(4)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.cummin('x')!
	rows := result.values()! as []map[string]json2.Any
	assert get_int(rows, 0, 'x') == 5
	assert get_int(rows, 1, 'x') == 3
	assert get_int(rows, 2, 'x') == 3  // min so far is still 3
}

fn test__cumprod_accumulates_product() {
	d := [
		{'x': json2.Any(2)},
		{'x': json2.Any(3)},
		{'x': json2.Any(4)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(d)!
	result := df.cumprod('x')!
	rows := result.values()! as []map[string]json2.Any
	// cumprod uses EXP(SUM(LN(x))) — requires positive values
	assert get_int(rows, 0, 'x') == 2   // 2
	assert get_int(rows, 1, 'x') == 6   // 2 * 3
	assert get_int(rows, 2, 'x') == 24  // 2 * 3 * 4
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: Compile error — cumulative functions undefined.

- [ ] **Step 3: Implement cumulative functions in `src/funcs.v`**

Add at the end of `src/funcs.v`:

```v
// Returns a new DataFrame with the cumulative sum of `col`, replacing that column
// All other columns are preserved
pub fn (df DataFrame) cumsum(col string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	all_cols := df.columns()!
	mut select_parts := []string{}
	for c in all_cols {
		if c == col {
			select_parts << 'sum("${col}") over (order by rowid rows unbounded preceding) as "${col}"'
		} else {
			select_parts << '"${c}"'
		}
	}
	_ := db.query("create table ${id} as select ${select_parts.join(', ')} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}

// Returns a new DataFrame with the running maximum of `col`
pub fn (df DataFrame) cummax(col string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	all_cols := df.columns()!
	mut select_parts := []string{}
	for c in all_cols {
		if c == col {
			select_parts << 'max("${col}") over (order by rowid rows unbounded preceding) as "${col}"'
		} else {
			select_parts << '"${c}"'
		}
	}
	_ := db.query("create table ${id} as select ${select_parts.join(', ')} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}

// Returns a new DataFrame with the running minimum of `col`
pub fn (df DataFrame) cummin(col string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	all_cols := df.columns()!
	mut select_parts := []string{}
	for c in all_cols {
		if c == col {
			select_parts << 'min("${col}") over (order by rowid rows unbounded preceding) as "${col}"'
		} else {
			select_parts << '"${c}"'
		}
	}
	_ := db.query("create table ${id} as select ${select_parts.join(', ')} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}

// Returns a new DataFrame with the cumulative product of `col`
// NOTE: Requires all values in `col` to be positive (uses EXP(SUM(LN(x))))
pub fn (df DataFrame) cumprod(col string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	all_cols := df.columns()!
	mut select_parts := []string{}
	for c in all_cols {
		if c == col {
			select_parts << 'cast(round(exp(sum(ln(cast("${col}" as double))) over (order by rowid rows unbounded preceding)), 0) as integer) as "${col}"'
		} else {
			select_parts << '"${c}"'
		}
	}
	_ := db.query("create table ${id} as select ${select_parts.join(', ')} from ${df.id}") or { return err }
	return DataFrame{ id: id, ctx: df.ctx }
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/funcs.v tests/funcs_test.v
git commit -m "feat: add cumsum, cummax, cummin, cumprod window functions"
```

---

## Task 11: Add `to_dict()` and `to_markdown()` export formats

**Files:**
- Modify: `src/io.v`
- Modify: `tests/mutation_test.v` (add I/O tests)

- [ ] **Step 1: Write failing tests**

Add to `tests/mutation_test.v`:

```v
fn test__to_dict_returns_column_map() {
	tdata := [
		{'x': json2.Any(1), 'y': json2.Any('a')},
		{'x': json2.Any(2), 'y': json2.Any('b')},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	d := df.to_dict()!
	assert 'x' in d
	assert 'y' in d
	assert d['x'].len == 2
	assert d['y'].len == 2
}

fn test__to_markdown_produces_table() {
	tdata := [
		{'name': json2.Any('Alice'), 'age': json2.Any(30)},
		{'name': json2.Any('Bob'), 'age': json2.Any(25)},
	]
	mut ctx := vframes.init()!
	df := ctx.read_records(tdata)!
	md := df.to_markdown()!
	assert md.contains('|')
	assert md.contains('name')
	assert md.contains('Alice')
	assert md.contains('---')
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: Compile error — `to_dict` and `to_markdown` undefined.

- [ ] **Step 3: Implement `to_dict()` and `to_markdown()` in `src/io.v`**

Add at the end of `src/io.v`:

```v
// Returns the DataFrame as a map of column name → array of values
pub fn (df DataFrame) to_dict() !map[string][]json2.Any {
	data := df.values()!
	rows := data as []map[string]json2.Any
	if rows.len == 0 {
		return map[string][]json2.Any{}
	}
	mut result := map[string][]json2.Any{}
	for k in rows[0].keys() {
		result[k] = []json2.Any{}
	}
	for row in rows {
		for k, v in row {
			result[k] << v
		}
	}
	return result
}

// Returns the DataFrame as a GitHub-flavored Markdown table string
pub fn (df DataFrame) to_markdown() !string {
	data := df.values()!
	rows := data as []map[string]json2.Any
	cols := df.columns()!
	if cols.len == 0 {
		return ''
	}
	mut lines := []string{}
	lines << '| ' + cols.join(' | ') + ' |'
	lines << '| ' + cols.map('---').join(' | ') + ' |'
	for row in rows {
		cells := cols.map((row[it] or { json2.Any('') }).str())
		lines << '| ' + cells.join(' | ') + ' |'
	}
	return lines.join('\n')
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass.

- [ ] **Step 5: Run examples to confirm nothing is broken**

```bash
v run examples/01_basic_usage.v
v run examples/02_data_analysis.v
```

Expected: No panics. Clean output.

- [ ] **Step 6: Commit**

```bash
git add src/io.v tests/mutation_test.v
git commit -m "feat: add to_dict() and to_markdown() export formats"
```

---

## Final Verification

- [ ] **Run full test suite**

```bash
cd /home/rabt/devel/vframes
make test
```

Expected: All tests pass, zero `assert true` without a preceding meaningful assertion.

- [ ] **Run all examples**

```bash
v run examples/01_basic_usage.v
v run examples/02_data_analysis.v
v run examples/03_advanced_features.v
```

Expected: All produce output and exit cleanly.

- [ ] **Smoke-test error handling (no panic)**

Create a temp file `/tmp/test_errors.v`:
```v
import vframes

fn main() {
	mut ctx := vframes.init()!
	_ := ctx.read_auto('nonexistent_file.csv') or {
		println('Got expected error: ${err}')
		return
	}
}
```

Run: `v run /tmp/test_errors.v`

Expected: Prints `Got expected error: ...`, does NOT crash/panic.
