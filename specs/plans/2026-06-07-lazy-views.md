# VFrames Lazy Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace eager per-operation `CREATE TABLE` materialization with `CREATE VIEW` so chained transformations stop copying data, eliminating the orphan-table memory problem.

**Architecture:** A new private `derive()` helper creates a view per transformation and tracks chain depth; base `read_*` operations keep creating real tables. `collect()` materializes a chain on demand, and a depth threshold emits a one-time warning. Behavior and error timing are preserved (DuckDB binds view queries at `CREATE VIEW` time), so the existing test suite is the safety net.

**Tech Stack:** V language (0.5.1), vduckdb (SQL-string interface), x.json2, `v test tests/`.

> **V sumtype cast note**: `Data` is a sumtype. Unwrap then cast in two statements: `data := df.values()!` then `rows := data as []map[string]json2.Any`.

> **Migration recipe (used in Tasks 3, 5):** Each transformation currently has this shape:
> ```v
> id := 'tbl_${rand.ulid()}'
> mut db := &df.ctx.db
> ... build `cols` / body ...
> _ := db.query("create table ${id} as <BODY>") or { return err }   // or `!`
> return DataFrame{ id: id, ctx: df.ctx }
> ```
> Replace the `id := ...`, `mut db := &df.ctx.db`, the `db.query(...)` line, and the `return DataFrame{...}` with a single:
> ```v
> return df.derive('<BODY>', df.depth)
> ```
> where `<BODY>` is exactly the SQL that followed `create table ${id} as` (it still references `${df.id}` as its source). Keep the `cols`/`types`/`all_cols` building logic unchanged. This is behavior-preserving: the result is identical, only stored as a view.

---

## File Map

| File | What changes |
|---|---|
| `src/models.v` | Add `depth int` to `DataFrame`; add `view_depth_warning int` to `ContextConfig` and `DataFrameContext` |
| `src/core.v` | `init()` copies `view_depth_warning` from config into the context |
| `src/lazy.v` *(new)* | `derive()`, `maybe_warn_depth()`, `collect()`, `copy()`, `chain_depth()`, `object_type()`, `is_lazy()` |
| `src/funcs.v` | Migrate all transformation ops to `derive()`; view-safe `eq`-family and `fillna` bfill; remove `import rand` |
| `src/mutation.v` | Migrate all transformation ops to `derive()`; multi-source `merge`/`concat`/`melt`/`pivot`; `query()` error wrap; remove `import rand` |
| `tests/lazy_test.v` *(new)* | View vs table assertions, `collect`/`copy`, depth tracking + warning, `eq`/`bfill` view-safety |
| `README.md` / `TUTORIAL.md` | Document lazy views + `collect()` |
| `CLAUDE.md` | Note views-based model + `derive()` |
| `IMPLEMENTATION_ROADMAP.md` | Mark the intermediate-table memory concern resolved |

---

## Task 1: Add depth + config fields

**Files:**
- Modify: `src/models.v`
- Modify: `src/core.v`

- [ ] **Step 1: Add `depth` to `DataFrame` and `view_depth_warning` to config structs**

In `src/models.v`, update `ContextConfig`:

```v
@[params]
pub struct ContextConfig {
pub:
	location			string = ":memory:"
	view_depth_warning	int = 50
}
```

Update `DataFrameContext` (add the field to the mutable section, after `loaded_extensions`):

```v
@[noinit]
struct DataFrameContext {
	dpath				string
mut:
	db					vduckdb.DuckDB
	loaded_extensions	map[string]bool
	view_depth_warning	int = 50
}
```

Update `DataFrame` (add `depth` after `ctx`):

```v
@[noinit]
pub struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
	ctx					DataFrameContext
	depth				int
pub mut:
	display_mode		string = 'box'
	display_max_rows	int = 100
}
```

- [ ] **Step 2: Wire the config through `init()`**

In `src/core.v`, update `init()` to copy the threshold into the context:

```v
pub fn init(cfg ContextConfig) !DataFrameContext {
	mut db := vduckdb.DuckDB{}
	_ := db.open(cfg.location) or { return err }
	_ := db.query('select 1') or { return err }
	return DataFrameContext{
		dpath: cfg.location
		db: db
		view_depth_warning: cfg.view_depth_warning
	}
}
```

- [ ] **Step 3: Verify the project still compiles and passes**

Run: `v test tests/`
Expected: PASS (additive fields only; defaults preserve behavior).

- [ ] **Step 4: Commit**

```bash
git add src/models.v src/core.v
git commit -m "feat: add depth field and view_depth_warning config"
```

---

## Task 2: The lazy helpers (`derive`, `collect`, `copy`, introspection)

**Files:**
- Create: `src/lazy.v`
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Create `tests/lazy_test.v`:

```v
import vframes
import x.json2

fn test_base_read_is_table() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
	]
	df := ctx.read_records(data)!
	assert df.object_type()! == 'BASE TABLE'
	assert df.is_lazy()! == false
	assert df.chain_depth() == 0
}

fn test_collect_materializes() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
	]
	df := ctx.read_records(data)!
	c := df.collect()!
	assert c.object_type()! == 'BASE TABLE'
	assert c.chain_depth() == 0
	assert c.shape()![0] == 2

	// copy() is an alias for collect()
	cp := df.copy()!
	assert cp.shape()![0] == 2
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/lazy_test.v`
Expected: FAIL — `unknown method object_type` / `collect` / `copy` / `chain_depth` / `is_lazy`.

- [ ] **Step 3: Implement the helpers**

Create `src/lazy.v`:

```v
module vframes

import rand
import x.json2

// derive creates a VIEW backing a new DataFrame from a SQL query body.
// The body is the SELECT (or other table-producing statement) that follows
// `CREATE VIEW <id> AS`. Depth = parent_depth + 1; a warning fires once when
// the chain first exceeds the configured threshold.
fn (df DataFrame) derive(query_body string, parent_depth int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	db.query('CREATE VIEW ${id} AS ${query_body}') or { return err }
	depth := parent_depth + 1
	df.ctx.maybe_warn_depth(depth)
	return DataFrame{
		id: id
		ctx: df.ctx
		depth: depth
	}
}

// maybe_warn_depth emits a one-time warning when depth first crosses the threshold.
fn (ctx DataFrameContext) maybe_warn_depth(depth int) {
	if ctx.view_depth_warning > 0 && depth == ctx.view_depth_warning + 1 {
		eprintln('vframes: view-chain depth ${depth} exceeds ${ctx.view_depth_warning} — call .collect() to materialize and keep query plans shallow')
	}
}

// collect materializes the current view chain into a real table, returning a
// table-backed DataFrame with depth reset to 0. Use for expensive/reused chains.
pub fn (df DataFrame) collect() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	db.query('CREATE TABLE ${id} AS SELECT * FROM ${df.id}') or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
		depth: 0
	}
}

// copy returns an independent materialized snapshot (alias for collect).
pub fn (df DataFrame) copy() !DataFrame {
	return df.collect()
}

// chain_depth returns how many lazy transformations deep this DataFrame is.
pub fn (df DataFrame) chain_depth() int {
	return df.depth
}

// object_type returns the DuckDB catalog type of the backing object:
// 'VIEW' for lazy intermediates, 'BASE TABLE' for materialized data.
pub fn (df DataFrame) object_type() !string {
	mut db := &df.ctx.db
	db.query("select table_type from information_schema.tables where table_name = '${df.id}'") or {
		return err
	}
	rows := db.get_array()
	if rows.len == 0 {
		return ''
	}
	return (rows[0]['table_type'] or { json2.Any('') }).str()
}

// is_lazy reports whether this DataFrame is backed by a view (not yet materialized).
pub fn (df DataFrame) is_lazy() !bool {
	return df.object_type()! == 'VIEW'
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/lazy_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lazy.v tests/lazy_test.v
git commit -m "feat: add derive/collect/copy and lazy introspection helpers"
```

---

## Task 3: Migrate `funcs.v` single-source operations to views

Apply the **Migration recipe** (top of plan) to every transformation in `funcs.v` **except** the `eq`/`ne`/`gt`/`ge`/`lt`/`le` family and `fillna` (those are Task 4). After all listed functions are migrated, remove the now-unused `import rand`.

**Files:**
- Modify: `src/funcs.v`
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/lazy_test.v`:

```v
fn test_transformation_is_view() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(10), 'y': json2.Any(100.0)},
		{'x': json2.Any(20), 'y': json2.Any(200.0)},
	]
	df := ctx.read_records(data)!

	added := df.add(5)!
	assert added.object_type()! == 'VIEW'
	assert added.is_lazy()! == true
	assert added.chain_depth() == 1

	// chaining increases depth
	chained := added.mul(2)!
	assert chained.chain_depth() == 2

	// result is still correct
	rows := chained.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['x'] or { json2.Any(0) }.int() == 30 // (10+5)*2
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/lazy_test.v`
Expected: FAIL — `added.object_type()` returns `'BASE TABLE'` (still a table), assertion fails.

- [ ] **Step 3: Migrate the standard-pattern functions**

For each of these, the `<BODY>` is exactly `select ${cols.join(',')} from ${df.id}` (the `cols`/`types`/`all_cols` building logic is unchanged) — replace the create/return block with `return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)`:

- `v_apply`, `min_max_apply`, `g_apply` (internal helpers)
- `add`, `sub`, `mul`, `div`, `floordiv`, `mod`, `round`, `clip`
- `count`, `nunique`
- `isna`, `notna`
- `replace`, `astype`, `isin`
- `agg`
- `rank`, `quantile`
- `shift`, `diff`, `pct_change`

Example — `add` becomes:

```v
pub fn (df DataFrame) add[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'${k}+${n.str()} as "${k}"'
		} else {
			k
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}
```

The cumulative functions use `', '` as the join separator; migrate them with that exact body:

- `cumsum`, `cummax`, `cummin`, `cumprod` → `return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)`

Example — `cumsum` becomes:

```v
pub fn (df DataFrame) cumsum() !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'sum("${k}") over (rows between unbounded preceding and current row) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)
}
```

- [ ] **Step 4: Migrate the non-standard-body functions**

These have a different `<BODY>`; migrate each with the exact body shown:

- `corr` → `return df.derive('select ${corr_cols.join(',')} from ${df.id}', df.depth)` (keep the `cols.len < 2 { return df }` guard and `corr_cols` building unchanged)
- `cov` → `return df.derive('select ${cov_cols.join(',')} from ${df.id}', df.depth)` (keep guard + `cov_cols` building)
- `nlargest` → `return df.derive('select * from ${df.id} order by ${first_numeric} desc limit ${n}', df.depth)` (keep the `numeric_cols.len == 0 { return df }` guard)
- `nsmallest` → `return df.derive('select * from ${df.id} order by ${first_numeric} asc limit ${n}', df.depth)` (keep the guard)
- `value_counts` → `return df.derive('select "${first_col}", count(*) as count from ${df.id} group by "${first_col}" order by count desc', df.depth)` (keep the `cols.len == 0 { return df }` guard)
- `apply` → `return df.derive('select ${func_expr} from ${df.id}', df.depth)`
- `rolling` → `return df.derive('select ${agg_func}("${col}") over (${frame}) as "${col}_${func}" from ${df.id}', df.depth)`

Example — `nlargest` becomes:

```v
pub fn (df DataFrame) nlargest(n int) !DataFrame {
	types := df.dtypes()!
	numeric_cols := types.keys().filter(types[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if numeric_cols.len == 0 {
		return df
	}
	first_numeric := numeric_cols[0]
	return df.derive('select * from ${df.id} order by ${first_numeric} desc limit ${n}', df.depth)
}
```

- [ ] **Step 5: Run the full suite**

Run: `v test tests/`
Expected: `lazy_test.v` `test_transformation_is_view` now PASSES; all other files still PASS (behavior preserved). `funcs.v` still imports `rand` (used by `eq`-family/`fillna`, migrated in Task 4), so no unused-import error yet.

- [ ] **Step 6: Commit**

```bash
git add src/funcs.v tests/lazy_test.v
git commit -m "feat: migrate funcs.v single-source ops to views"
```

---

## Task 4: View-safe `eq`-family and `fillna`; finish `funcs.v`

The `eq`/`ne`/`gt`/`ge`/`lt`/`le` family joins on `using (rowid)`, and `fillna`'s `bfill` branch orders by `rowid` — neither works when the source is a view. Rewrite both to derive row order with `row_number()` inside a subquery, then route through `derive()`. After this task, remove `import rand` from `funcs.v`.

**Files:**
- Modify: `src/funcs.v`
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/lazy_test.v`:

```v
fn test_eq_on_views() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'a': json2.Any(1), 'b': json2.Any(1)},
		{'a': json2.Any(2), 'b': json2.Any(9)},
	]
	df := ctx.read_records(data)!
	// compare a view against a view (both are transformations)
	left := df.add(0)!
	right := df.add(0)!
	res := left.eq(right)!
	assert res.object_type()! == 'VIEW'
	rows := res.values(vframes.ValuesParams{})! as []map[string]json2.Any
	assert rows[0]['a'] or { json2.Any(false) }.bool() == true
	assert rows.len == 2
}

fn test_bfill_on_view() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(1), 'y': json2.Any(json2.null)},
		{'x': json2.Any(json2.null), 'y': json2.Any(2)},
	]
	df := ctx.read_records(data)!
	// bfill applied to a view (a transformed frame), exercising the rowid rewrite
	v := df.add(0)!
	filled := v.bfill()!
	assert filled.shape()![0] == 2
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/lazy_test.v`
Expected: FAIL — `eq` on views errors (no `rowid` column on a view) and/or `bfill` on a view errors.

- [ ] **Step 3: Rewrite the `eq`-family**

Replace each of `eq`, `ne`, `gt`, `ge`, `lt`, `le`. They are identical except the operator. For `eq` (operator `=`):

```v
pub fn (df DataFrame) eq(other DataFrame) !DataFrame {
	cols := df.columns()!
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1."${col}" = t2."${col}") as "${col}"'
	}
	body := 'with _t1 as (select *, row_number() over () as _rn from ${df.id}), ' +
		'_t2 as (select *, row_number() over () as _rn from ${other.id}) ' +
		'select ${select_cols.join(',')} from _t1 t1 join _t2 t2 using (_rn)'
	parent := if df.depth > other.depth { df.depth } else { other.depth }
	return df.derive(body, parent)
}
```

Apply the same structure to the others, changing only the operator in the `select_cols` line:
- `ne` → `!=`
- `gt` → `>`
- `ge` → `>=`
- `lt` → `<`
- `le` → `<=`

- [ ] **Step 4: Rewrite `fillna` to build a body and use `derive()`**

Replace `fillna` so each branch builds a `body` string (the `bfill` branch uses a `row_number()` subquery instead of `rowid`), then derive once:

```v
pub fn (df DataFrame) fillna(fo FillnaOptions) !DataFrame {
	all_cols := df.columns()!
	mut body := ''
	if fo.method == 'ffill' {
		mut cols := []string{}
		for k in all_cols {
			cols << 'last_value("${k}" ignore nulls) over () as "${k}"'
		}
		body = 'select ${cols.join(',')} from ${df.id}'
	} else if fo.method == 'bfill' {
		mut cols := []string{}
		for k in all_cols {
			cols << 'first_value("${k}" ignore nulls) over (order by _rn desc) as "${k}"'
		}
		body = 'select ${cols.join(',')} from (select *, row_number() over () as _rn from ${df.id})'
	} else {
		mut cols := []string{}
		for k in all_cols {
			cols << 'coalesce("${k}", ${fo.value}) as "${k}"'
		}
		body = 'select ${cols.join(',')} from ${df.id}'
	}
	return df.derive(body, df.depth)
}
```

(`ffill()` and `bfill()` aliases that call `fillna` need no change.)

- [ ] **Step 5: Remove the now-unused `import rand`**

At the top of `src/funcs.v`, delete the line `import rand` (no function in the file generates a ulid anymore).

- [ ] **Step 6: Run the full suite**

Run: `v test tests/`
Expected: all PASS, including the two new tests. No unused-import error.

- [ ] **Step 7: Commit**

```bash
git add src/funcs.v tests/lazy_test.v
git commit -m "feat: make eq-family and fillna view-safe; finish funcs.v migration"
```

---

## Task 5: Migrate `mutation.v` single-source operations to views

Apply the **Migration recipe** to the single-source transformations in `mutation.v`. `query()` needs its custom error message preserved. Multi-source ops (`merge`/`concat`/`melt`/`pivot`) are Task 6, so leave `import rand` in place until then.

**Files:**
- Modify: `src/mutation.v`
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/lazy_test.v`:

```v
fn test_mutation_ops_are_views() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'a': json2.Any(1), 'b': json2.Any(10)},
		{'a': json2.Any(2), 'b': json2.Any(20)},
		{'a': json2.Any(3), 'b': json2.Any(30)},
	]
	df := ctx.read_records(data)!

	filtered := df.filter('a > 1')!
	assert filtered.object_type()! == 'VIEW'
	assert filtered.shape()![0] == 2

	sub := df.subset(['a'])!
	assert sub.object_type()! == 'VIEW'
	assert sub.columns()!.len == 1

	sorted := df.sort_values(['b'], ascending: false)!
	assert sorted.object_type()! == 'VIEW'
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/lazy_test.v`
Expected: FAIL — `filtered.object_type()` is `'BASE TABLE'`.

- [ ] **Step 3: Migrate the standard single-source functions**

Migrate each with the `<BODY>` being exactly the SQL that followed `create table ${id} as`:

- `delete_column` → `'select * exclude(${col}) from ${df.id}'`
- `add_column` → `'select *, ${expr} as ${col} from ${df.id}'`
- `subset` → `'select ${cols.join(',')} from ${df.id}'`
- `slice` → `'select * from ${df.id} limit ${limit} offset ${offset}'` (keep `offset`/`limit` computation)
- `group_by` → `'select ${dimensions.join(',')}, ${sets.join(',')} from ${df.id} group by ${dimensions.join(',')}'` (keep `sets` building)
- `add_prefix` → `"select columns('(.*)') as '${prefix}_\\1' from ${df.id}"`
- `add_suffix` → `"select columns('(.*)') as '\\1_${suffix}' from ${df.id}"`
- `dropna` → `'select ${selected_columns.join(',')} from ${df.id} where ${predicate}'` (keep `selected_columns`/`predicate` building)
- `rename` → `'select ${cols.join(',')} from ${df.id}'` (keep `cols` building)
- `drop_duplicates` → `'select distinct ${cols_str} from ${df.id}'` (keep `cols_str` building)
- `sample` → `'select * from ${df.id} using sample ${sample_size}${replacement}'` (keep the `total_rows`/`sample_size`/`replacement` computation, including the `df.shape()!` call)
- `drop` → `'SELECT * EXCLUDE (${exclude}) FROM ${df.id}'` (keep `exclude` building)
- `sort_values` → `'SELECT * FROM ${df.id} ORDER BY ${order_cols}'` (keep `order_cols`/`direction` building)

Example — `subset` becomes:

```v
pub fn (df DataFrame) subset(cols []string) !DataFrame {
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}
```

Example — `sample` becomes (note the retained shape call):

```v
pub fn (df DataFrame) sample(so SampleOptions) !DataFrame {
	total_rows := (df.shape()!)[0]
	sample_size := if so.n > 0 { so.n } else { int(f64(total_rows) * so.frac) }
	replacement := if so.replace { ' with replacement' } else { '' }
	return df.derive('select * from ${df.id} using sample ${sample_size}${replacement}', df.depth)
}
```

- [ ] **Step 4: Migrate `query()` preserving its error message**

`query()` builds three possible SQL strings then runs one query. Build a `body` and derive with the custom error wrap:

```v
pub fn (df DataFrame) query(q string, dconf DFConfig) !DataFrame {
	q_upper := q.to_upper()
	body := if where_idx := q_upper.index(' WHERE ') {
		cols := q[..where_idx].trim_space()
		cond := q[where_idx + 7..]
		select_cols := if cols == '' || cols == '*' { '*' } else { cols }
		'SELECT ${select_cols} FROM ${df.id} WHERE ${cond}'
	} else if q_upper.contains('>') || q_upper.contains('<') || q_upper.contains(' = ')
		|| q_upper.contains(' IN ') || q_upper.contains(' IS ') || q_upper.contains(' LIKE ') {
		'SELECT * FROM ${df.id} WHERE ${q}'
	} else {
		'SELECT ${q} FROM ${df.id}'
	}
	return df.derive(body, df.depth) or {
		return error('Invalid query syntax: ${err.msg()}')
	}
}
```

- [ ] **Step 5: Run the full suite**

Run: `v test tests/`
Expected: all PASS (the new `test_mutation_ops_are_views` included). `import rand` is still used by the multi-source ops, so no unused-import error.

- [ ] **Step 6: Commit**

```bash
git add src/mutation.v tests/lazy_test.v
git commit -m "feat: migrate mutation.v single-source ops to views"
```

---

## Task 6: Migrate multi-source `mutation.v` ops; finish `mutation.v`

Migrate `merge`, `concat`, `melt`, and `pivot`. These reference multiple sources (or the same source multiple times), which is view-safe because sources are named objects. Depth is `max` over inputs. After this task, remove `import rand` from `mutation.v`.

**Files:**
- Modify: `src/mutation.v`
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/lazy_test.v`:

```v
fn test_multi_source_ops_are_views() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	left := ctx.read_records([
		{'id': json2.Any(1), 'name': json2.Any('Alice')},
		{'id': json2.Any(2), 'name': json2.Any('Bob')},
	])!
	right := ctx.read_records([
		{'id': json2.Any(1), 'score': json2.Any(90)},
		{'id': json2.Any(2), 'score': json2.Any(80)},
	])!

	merged := left.merge(right, on: 'id')!
	assert merged.object_type()! == 'VIEW'
	assert merged.shape()![0] == 2

	stacked := vframes.concat([left, left])!
	assert stacked.object_type()! == 'VIEW'
	assert stacked.shape()![0] == 4

	wide := ctx.read_records([
		{'k': json2.Any('r1'), 'm1': json2.Any(1), 'm2': json2.Any(2)},
	])!
	melted := wide.melt(id_vars: ['k'], value_vars: ['m1', 'm2'])!
	assert melted.object_type()! == 'VIEW'
	assert melted.shape()![0] == 2
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/lazy_test.v`
Expected: FAIL — `merged.object_type()` is `'BASE TABLE'`.

- [ ] **Step 3: Migrate `merge`**

```v
pub fn (df DataFrame) merge(other DataFrame, mo MergeOptions) !DataFrame {
	how_sql := match mo.how {
		'left' { 'left join' }
		'right' { 'right join' }
		'outer', 'full' { 'full outer join' }
		'cross' { 'cross join' }
		else { 'inner join' }
	}
	suffix_left := if mo.left_on != '' { mo.left_on } else { mo.on }
	suffix_right := if mo.right_on != '' { mo.right_on } else { mo.on }
	body := 'select * from ${df.id} t1 ${how_sql} ${other.id} t2 on t1."${suffix_left}" = t2."${suffix_right}"'
	parent := if df.depth > other.depth { df.depth } else { other.depth }
	return df.derive(body, parent)
}
```

- [ ] **Step 4: Migrate `concat` (free function)**

```v
pub fn concat(dfs []DataFrame) !DataFrame {
	if dfs.len == 0 {
		return empty()
	}
	if dfs.len == 1 {
		return dfs[0]
	}
	table_names := dfs.map(it.id).join(', ')
	mut max_d := 0
	for d in dfs {
		if d.depth > max_d {
			max_d = d.depth
		}
	}
	return dfs[0].derive('select * from ${table_names}', max_d)
}
```

- [ ] **Step 5: Migrate `melt`**

```v
pub fn (df DataFrame) melt(mo MeltOptions) !DataFrame {
	id_cols := mo.id_vars.map('"${it}"').join(', ')
	mut queries := []string{}
	for val_col in mo.value_vars {
		queries << 'select ${id_cols}, \'${val_col}\' as "${mo.var_name}", "${val_col}" as "${mo.value_name}" from ${df.id}'
	}
	return df.derive(queries.join(' union all '), df.depth)
}
```

- [ ] **Step 6: Migrate `pivot` (wrap the PIVOT statement in a subquery)**

```v
pub fn (df DataFrame) pivot(po PivotOptions) !DataFrame {
	aggfunc := if po.aggfunc != '' { po.aggfunc } else { 'max' }
	body := 'select * from (PIVOT ${df.id} ON "${po.columns}" USING ${aggfunc}("${po.values}") AS "${po.values}" GROUP BY "${po.index}" ORDER BY "${po.index}")'
	return df.derive(body, df.depth)
}
```

(`pivot_table` delegates to `pivot` — no change.)

- [ ] **Step 7: Remove the now-unused `import rand`**

At the top of `src/mutation.v`, delete the line `import rand`.

- [ ] **Step 8: Run the full suite**

Run: `v test tests/`
Expected: all PASS, including `test_multi_source_ops_are_views`. No unused-import error.

- [ ] **Step 9: Commit**

```bash
git add src/mutation.v tests/lazy_test.v
git commit -m "feat: migrate multi-source mutation ops to views; finish mutation.v"
```

---

## Task 7: Depth-warning behavior and collect-resets-depth

**Files:**
- Test: `tests/lazy_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/lazy_test.v`:

```v
fn test_depth_and_collect_reset() {
	// low threshold so we can exercise the warning path without huge chains
	mut ctx := vframes.init(view_depth_warning: 3)!
	defer { ctx.close() }

	data := [
		{'x': json2.Any(1)},
		{'x': json2.Any(2)},
	]
	mut df := ctx.read_records(data)!
	assert df.chain_depth() == 0

	// build a chain past the threshold; should not error (warning only)
	for _ in 0 .. 5 {
		df = df.add(0)!
	}
	assert df.chain_depth() == 5

	// collect resets depth and materializes
	c := df.collect()!
	assert c.chain_depth() == 0
	assert c.object_type()! == 'BASE TABLE'
	assert c.shape()![0] == 2
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `v test tests/lazy_test.v`
Expected: PASS (the mechanism was implemented in Tasks 1–3; this test locks the behavior). A warning line is printed to stderr when depth crosses 3 — that is expected and does not fail the test.

> If this test fails because depth does not increment as expected, revisit `derive()` in `src/lazy.v` (Task 2) — each transformation must pass `df.depth` as `parent_depth`.

- [ ] **Step 3: Commit**

```bash
git add tests/lazy_test.v
git commit -m "test: lock view-chain depth tracking and collect reset"
```

---

## Task 8: Documentation + final verification

**Files:**
- Modify: `README.md`, `TUTORIAL.md`, `CLAUDE.md`, `IMPLEMENTATION_ROADMAP.md`

- [ ] **Step 1: Update `README.md`**

Add to the Features list (after the immutable-design bullet):

```markdown
- **Lazy by default** — transformations build DuckDB views (no data copies); call `df.collect()` to materialize a result
```

Add a subsection after the "Immutability" core concept:

```markdown
### Lazy views & `collect()`

Each transformation returns a new DataFrame backed by a DuckDB **view**, not a
copied table — so chaining operations costs no extra memory. The query runs only
at a materialization point (`head`, `values`, `to_csv`, `shape`, …).

```v
result := df.filter('age > 25')!.add_column('bonus', 'salary*0.1')!  // views, no data copied
final  := result.collect()!   // materialize into a real table for reuse
```

`collect()` (and its alias `copy()`) snapshot a chain into an independent table —
useful for an expensive intermediate you reuse, or to keep a result alive while
the rest is discarded. Inspect state with `df.is_lazy()!` / `df.object_type()!`.
Very deep chains print a one-time hint to call `collect()`; tune or disable it
with `init(view_depth_warning: N)` (`0` disables).
```

- [ ] **Step 2: Update `TUTORIAL.md`**

Add a short section after "Initialization" explaining the same lazy/`collect()` model with a code example matching the tutorial's style (a chained transform, then `.collect()`), and note that `view_depth_warning` is configurable via `init()`.

- [ ] **Step 3: Update `CLAUDE.md`**

In the "Core Design" section, update the immutability bullet to:

```markdown
- **Immutable, lazy operations**: Every transformation returns a new `DataFrame` backed by a DuckDB **view** (via the `derive()` helper in `lazy.v`) — no data is copied until a materialization point or `collect()`. Base `read_*` ops create real tables.
```

Add a row to the source-files table:

```markdown
| `lazy.v` | View-based laziness: `derive()`, `collect()`, `copy()`, `chain_depth()`, `object_type()`, `is_lazy()` |
```

- [ ] **Step 4: Update `IMPLEMENTATION_ROADMAP.md`**

In the "Memory Management" section, replace the "Consider implementing" list with a note that intermediate tables are now views (no data copies) and `collect()` materializes on demand, resolving the orphan-table concern.

- [ ] **Step 5: Run the full suite and a clean compile**

Run: `v test tests/`
Expected: all test files PASS.

Run: `v -shared -o /tmp/vframes_lazy.so src/`
Expected: exit 0, no errors.

- [ ] **Step 6: Commit**

```bash
git add README.md TUTORIAL.md IMPLEMENTATION_ROADMAP.md
git commit -m "docs: document lazy views and collect()"
```

---

## Self-Review Notes

**Spec coverage:**
- Core model change (CREATE VIEW) → Tasks 3, 4, 5, 6. ✓
- `derive()` helper → Task 2. ✓
- Base reads stay tables → unchanged by design (io.v/io_db.v untouched); asserted in Task 2 `test_base_read_is_table`. ✓
- Depth tracking + crossing-point warning → Tasks 1, 2, 7. ✓
- `collect()` + `copy()`→collect → Task 2 (note: `copy` is new, not pre-existing). ✓
- Error semantics preserved → relies on CREATE VIEW binding; `query()` custom message preserved in Task 5; behavior suite is the net. ✓
- No per-view free / cleanup via close → nothing added (correct by omission). ✓
- Testing (view vs table, collect, reuse, depth, error timing) → Tasks 2–7; reuse case covered by `test_eq_on_views` (two frames off one base) and existing suite. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code or an exact body string + a worked example. The bulk migrations use a defined recipe plus per-function body strings (not "similar to" hand-waving).

**Type consistency:** `derive(query_body string, parent_depth int) !DataFrame`, `collect()`, `copy()`, `chain_depth() int`, `object_type() !string`, `is_lazy() !bool`, `maybe_warn_depth(depth int)` are used consistently across tasks. `view_depth_warning` named identically in `ContextConfig`, `DataFrameContext`, `init()`, and tests.

**Known sequencing:** `import rand` stays in `funcs.v` until Task 4 and in `mutation.v` until Task 6 (both files keep ulid-generating multi-source/special ops until then) — removing it earlier would break compilation. Flagged inline in Tasks 3/4/5/6.
