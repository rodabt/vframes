# VFrames Lazy Views — Design

**Date:** 2026-06-07
**Status:** Approved (design); pending implementation plan
**Scope:** Replace eager per-operation table materialization with DuckDB views
(lazy storage) to eliminate the orphan-table memory problem.

## Background

Every VFrames transformation is implemented as `CREATE TABLE tbl_<ulid> AS
SELECT ...`. Because the library is immutable, each step creates a new physical
table and leaves the previous one behind. Nothing issues `DROP TABLE`; the only
reclamation is `ctx.close()`. A chained pipeline therefore accumulates a full
data copy per step inside the DuckDB instance — the "orphan-table" problem. In a
loop or a long-lived context this grows unbounded (and bloats the file for
persisted contexts).

The fix: store intermediates as **views** (a stored query, not data). Views copy
no rows, so the memory leak disappears, while `DataFrame.id` remains a named
object so existing SQL-building call sites keep working.

### Key facts grounding this design

- All 82 source references follow the uniform shape
  `create table ${id} as select ... from ${df.id}`, enabling a mechanical swap.
- No test asserts operation-time errors, and DuckDB binds a view's query at
  `CREATE VIEW` time anyway, so error timing is preserved.
- Self-referencing ops (`merge` = two sources, `concat` = N sources, `melt` = N
  `UNION ALL` over one source, `pivot`) reference sources by name, so they need
  no special handling when the source is a view.

## Goals

- Transformation operations create views, not tables — zero data copies for
  intermediates.
- Preserve current behavior and error timing; the existing test suite passes
  unchanged.
- Provide `collect()` to materialize an expensive/reused chain on demand.
- Warn (once) when a view chain grows deep enough to risk planner blowup.
- Reduce duplication by centralizing intermediate creation in one helper.

## Non-goals (this cycle)

- Pure-lazy SQL-string composition and cross-operation query optimization
  (a possible future cycle).
- Automatic reference counting or automatic catalog cleanup.
- Per-view `free()` (unsafe: views form a dependency chain).

## Design

### A. Core model change

Transformation operations switch from `CREATE TABLE tbl_<ulid> AS <query>` to
`CREATE VIEW tbl_<ulid> AS <query>`. A view stores the query, not the data, so no
intermediate copies rows. `DataFrame.id` stays a named object, so the existing
`from ${df.id}` references and all self-referencing ops work unchanged.

### B. The `derive()` helper (DRY)

Centralize id generation, view creation, depth computation, the depth warning,
and `DataFrame` construction:

```v
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
```

Each transformation shrinks to, e.g.:

```v
return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
```

Multi-source ops pass `parent_depth = max(df.depth, other.depth)` (for `concat`,
the max over all inputs).

Ops whose body is not a plain `SELECT` (e.g. `pivot` uses `PIVOT ...`) wrap it:
`df.derive('select * from (PIVOT ${df.id} ...)', df.depth)`.

### C. What stays materialized

Base ingestion keeps `CREATE TABLE` (materializes once), starting at `depth = 0`:
`read_auto`, `read_csv`, `read_json`, `read_parquet`, `read_excel`,
`read_records`, `read_sql`, `read_table`, `read_database`.

Rationale: a view over `read_csv('file')` would re-read and re-parse the file on
every materialization; a view over `read_table('pg.t')` would re-query the remote
database. Base reads should snapshot once.

Only transformations (all of `funcs.v`; the selection/mutation/reshaping ops in
`mutation.v`) become views.

### D. Depth tracking + warning

- `DataFrame` gains `depth int` (default 0). `derive()` sets child depth =
  parent depth + 1.
- `ContextConfig` gains `view_depth_warning int = 50` (0 disables warnings);
  `DataFrameContext` stores it.
- `maybe_warn_depth(depth)` emits the warning **once, at the crossing point**
  (only when `depth == view_depth_warning + 1`) via `eprintln`:

  ```
  vframes: view-chain depth 51 exceeds 50 — call .collect() to materialize and keep query plans shallow
  ```

  It only warns; it never auto-materializes.

### E. `collect()` and `copy()`

```v
// Materializes the current view chain into a real table and returns a
// table-backed DataFrame with depth reset to 0.
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
```

`copy()` is remapped to `collect()` semantics (an independent snapshot), matching
pandas' deep-copy intent. All other ops remain behaviorally identical to users
(same results; only storage differs).

### F. Error semantics — preserved

DuckDB binds a view's query at `CREATE VIEW` time (it computes the output
schema), so invalid columns/syntax error at the operation call exactly as today.
Lazy storage is achieved without deferring error reporting.

### G. Catalog cleanup

Views are metadata-only and form a dependency chain (each references its parent
by name), so dropping an intermediate would break downstream frames. Therefore no
per-view `free()`. Cleanup relies on `ctx.close()` plus `collect()` (snapshot an
independent table to keep while the rest is discarded at close). The leak being
fixed was data copies; views carry none.

## Error handling

- `derive()` and `collect()` return `!DataFrame`; errors propagate via `!` as in
  the rest of the module.
- A failed `CREATE VIEW` (e.g. invalid column) surfaces the DuckDB error at the
  operation call.

## Testing

Behavior preservation is the primary safety net: the existing 7-file suite
exercises every operation through materialization points (`shape`, `values`,
`head`), so matching results prove the refactor correct. Run the full suite after
each op group.

New targeted tests (`tests/lazy_test.v`):

- A transformation creates a **view** (assert object type via
  `information_schema.tables` / `duckdb_views()`).
- A base read creates a **table**.
- `collect()` turns a view chain into a table; results are identical pre/post.
- Reuse: `base.filter(a)` and `base.filter(b)` both succeed (base not consumed).
- Depth: a chain past the threshold emits the warning once; `collect()` resets
  depth (verify a subsequent chain from the collected frame doesn't re-warn
  immediately).
- An invalid transformation still errors at the operation call (error timing).

## Implementation phases

1. Model + helper: add `depth` field, `view_depth_warning` config, `derive()`,
   `maybe_warn_depth()`, `collect()`.
2. Migrate `funcs.v` operations to `derive()`; run full suite.
3. Migrate `mutation.v` operations (including multi-source `merge`/`concat`/
   `melt`/`pivot`) to `derive()`; run full suite.
4. Remap `copy()` to `collect()`; add `tests/lazy_test.v`; update docs.

## Documentation impact

- README / TUTORIAL: explain lazy views, `collect()`, and when to use it.
- CLAUDE.md: note the views-based immutability model and `derive()` helper.
- IMPLEMENTATION_ROADMAP.md "Memory Management": mark the intermediate-table
  concern resolved via views + `collect()`.
