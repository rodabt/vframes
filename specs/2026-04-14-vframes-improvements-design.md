# VFrames Broad Improvement Pass — Design Spec

**Date:** 2026-04-14  
**Approach:** Foundation-first (Approach 1)  
**Target users:** Both Pandas migrants and V-native developers

---

## Context

VFrames is a Pandas-like DataFrame library for V, backed by DuckDB. It has reached Phase 2 of its roadmap with ~65 functions implemented. However, three classes of issues make it unreliable for real use:

1. Public functions like `head()`, `tail()`, `columns()`, and `init()` call `panic(err)` instead of returning errors — crashing the entire program on any DB failure.
2. Tests exist but most `assert true` without checking actual output, providing false confidence.
3. Several features are documented and referenced in tutorials but never implemented (`sort_values`, `filter`, etc.), and one has a silent bug (`pivot` ignores `aggfunc`).

This pass fixes the foundation first, then adds features on top of a tested, non-crashing base.

---

## Phase 1: Error Handling Overhaul

### Problem
24+ locations use `or { panic(err) }` in public-facing code. All read-only exploration functions (`head`, `tail`, `info`, `describe`, `shape`, `columns`, `dtypes`, `values`) return non-error types and panic internally. `init()` also panics on DuckDB open failure.

### Changes

**`src/explore.v`** — All 8 public functions change signatures:
- `head(n int, dconf DFConfig) Data` → `head(n int, dconf DFConfig) !Data`
- `tail(n int, dconf DFConfig) Data` → `tail(n int, dconf DFConfig) !Data`
- `columns() []string` → `columns() ![]string`
- `dtypes() map[string]string` → `dtypes() !map[string]string`
- `shape() []int` → `shape() ![]int`
- `info(dconf DFConfig) Data` → `info(dconf DFConfig) !Data`
- `describe(dconf DFConfig) Data` → `describe(dconf DFConfig) !Data`
- `values() Data` → `values() !Data`

All internal `or { panic(err) }` become `or { return err }`.

**`src/core.v`** — `init()` changes to return `!DataFrameContext`. All callers must handle with `or { }` or `!`.

**`src/mutation.v` / `src/funcs.v`** — Any remaining `or { panic(err) }` converted to `or { return err }`.

**Convention going forward:** All public functions that execute a DuckDB query return `!T`. Pure in-memory helpers with no DB calls may return `T`.

### Breaking changes
This is a **breaking API change**. All callers in `tests/`, `examples/`, and user code must be updated to propagate or handle the new `!` return types. The examples and tests are updated as part of this pass.

---

## Phase 2: Test Quality Overhaul

### Problem
Most tests call functions and assert `true` without verifying actual output. Error paths are never tested — a panic in production would not be caught by the test suite.

### Changes

**`tests/funcs_test.v`** — Replace `assert true` with value assertions:
- After arithmetic ops (`add`, `sub`, `mul`, `div`), pull result with `values()` and assert specific cell values match expected numeric output.
- After statistical ops (`mean`, `sum`, `std`), assert result values are within expected range.

**`tests/explore_test.v`** — Update for new `!` return types. Add assertions for:
- `columns()` returns expected column names
- `dtypes()` returns correct type strings
- `describe()` output has expected shape (row count = number of stats)

**`tests/mutation_test.v`** — Add after each transformation:
- Column count check (e.g., after `subset`, assert `result.columns()!.len == expected`)
- Row count check (e.g., after `slice`, assert shape matches)
- Spot value check (e.g., after `rename`, assert new column name present, old name absent)

**New: error-path tests** — One test per module that passes an invalid column name or malformed expression and asserts an error is returned (verified with `or { assert err.msg() != '' }`).

**Test data:** All tests use inline fixtures (no external files). Keep existing pattern of constructing small DataFrames from `read_records()`.

---

## Phase 3: Missing Features

### Tier 1 — Documented but broken (fix first)

**`sort_values(cols []string, opts SortOptions)` in `src/mutation.v`**
```v
@[params]
pub struct SortOptions {
pub:
    ascending []bool  // per-column direction; if shorter than cols, remaining default to true
}
```
Translates to `SELECT * FROM {id} ORDER BY col1 [ASC|DESC], col2 [ASC|DESC], ...`. Returns new DataFrame. If `ascending` is empty, all columns sort ascending.

**`pivot()` aggfunc fix in `src/mutation.v`**  
`PivotOptions.aggfunc` is currently accepted but the SQL always uses `max()`. Wire `aggfunc` into the DuckDB `PIVOT ... USING {aggfunc}(...)` clause. Validate that `aggfunc` is one of: `sum`, `avg`, `min`, `max`, `count`.

### Tier 2 — Pandas compatibility aliases (thin wrappers)

Added directly to `src/mutation.v` (no new file — each alias lives next to the function it delegates to):

| New function | Delegates to | Notes |
|---|---|---|
| `filter(expr string)` | `query(expr)` | Pandas `df.query()` equivalent |
| `select(cols []string)` | `subset(cols)` | Pandas `df[['col1','col2']]` equivalent |
| `drop(cols []string)` | Loops `delete_column()` | Generalized to multiple columns |
| `groupby(dims, metrics)` | `group_by(dims, metrics)` | No-underscore alias |

### Tier 3 — New operations

**Cumulative functions** (`src/funcs.v`):
- `cumsum(col string)` — `SUM(col) OVER (ORDER BY rowid ROWS UNBOUNDED PRECEDING)`
- `cummax(col string)` — `MAX(col) OVER (...)`
- `cummin(col string)` — `MIN(col) OVER (...)`
- `cumprod(col string)` — `EXP(SUM(LN(col)) OVER (...))`

Each returns a new DataFrame with the cumulative column added/replaced.

**Additional export formats** (`src/io.v`):
- `to_dict() !map[string][]json2.Any` — returns in-memory map of column → values array
- `to_markdown() !string` — returns GitHub-flavored markdown table as string

### Out of scope for this pass
`loc`/`iloc`, `iterrows`/`itertuples`, time series (`resample`, `asfreq`), index operations (`set_index`, `reset_index`), `explode`, `stack`/`unstack`. These are Phase 5–8 and deserve separate design specs.

---

## Verification

```bash
# All tests must pass after each phase
make test

# Manually run examples to confirm no panics
v run examples/01_basic_usage.v
v run examples/02_data_analysis.v
v run examples/03_advanced_features.v

# Confirm error paths work (should NOT panic)
# Add a temporary snippet:
mut ctx := vframes.init()!
df := ctx.read_auto('nonexistent.csv') or {
    println('Got expected error: ${err}')  // Should print, not crash
    return
}
```

---

## File Checklist

| File | Changes |
|---|---|
| `src/explore.v` | All 8 functions → `!T` return types, remove panics |
| `src/core.v` | `init()` → `!DataFrameContext` |
| `src/mutation.v` | Remove remaining panics; add `sort_values`, `filter`, `select`, `drop`, `groupby` |
| `src/funcs.v` | Remove remaining panics; add `cumsum`, `cummax`, `cummin`, `cumprod` |
| `src/io.v` | Add `to_dict`, `to_markdown` |
| `tests/funcs_test.v` | Real value assertions, error-path tests |
| `tests/explore_test.v` | Update for `!` types, add content assertions |
| `tests/mutation_test.v` | Add count/value checks, error-path tests |
| `examples/*.v` | Update all callers for `!` return types |
