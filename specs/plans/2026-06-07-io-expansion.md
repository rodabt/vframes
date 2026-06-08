# VFrames I/O Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the VFrames I/O subsystem with remote/cloud file loaders, explicit format readers (csv/json/parquet/excel), live database integration via `ATTACH`, and `to_excel`/`to_html`/`to_sql` exports.

**Architecture:** Everything runs through `vduckdb`'s SQL-string interface (`db.query()`) — no new C bindings. A small extension manager auto-installs DuckDB extensions (`httpfs`, `excel`, `postgres`/`mysql`/`sqlite`) on first use and caches them. URI-scheme detection is isolated into pure helpers so routing is testable offline. Work proceeds in four phases: (1) extension manager + local/remote file readers, (2) Excel read/write, (3) database integration, (4) `to_html` + S3 credentials.

**Tech Stack:** V language (0.5.1), vduckdb (DuckDB bindings, SQL-string interface), x.json2 (stdlib JSON), `make test` / `v test tests/`.

> **V sumtype cast note**: `Data` is a sumtype (`[]map[string]json2.Any | []map[string]string`). Always split the unwrap and cast into two statements: `data := df.values()!` then `rows := data as []map[string]json2.Any`. Do not chain them on one line.

> **Network note**: Local CSV/JSON/Parquet readers, `to_html`, and the URI-routing helpers need **no** network. Tests for Excel, DB scanners, and any S3/HTTPS read require DuckDB to `INSTALL` an extension from its repo (one-time network download). Those tests are marked **(network)** and should be run in an environment with connectivity.

> **Receiver/cache note**: ctx-level readers (`read_csv`, `attach`, …) use a `mut ctx DataFrameContext` receiver, so the `loaded_extensions` cache persists. DataFrame-level exports (`to_excel`, `to_sql`) take a non-mut `df DataFrame` receiver; they call `ensure_extension` on a local `mut ctx := df.ctx` copy. The cache write won't persist there, but `INSTALL`/`LOAD` are idempotent, so this is correct (only a redundant no-op LOAD at worst).

---

## File Map

| File | What changes |
|---|---|
| `src/models.v` | Add `loaded_extensions map[string]bool` field to `DataFrameContext` |
| `src/extensions.v` *(new)* | `ensure_extension()` manager; `set_s3_credentials()` + `S3Credentials` |
| `src/io.v` | Add path helpers (`is_remote_path`, `scheme_for_path`); add `read_csv/read_json/read_parquet/read_excel` + option structs; enhance `read_auto` routing; add `to_excel`, `to_html` |
| `src/io_db.v` *(new)* | `DbType`, `AttachOptions`, `attach`, `detach`, `read_sql`, `read_table`, `read_database`, `to_sql` + `ToSqlOptions` |
| `tests/io_test.v` *(new)* | Path helpers, file readers, `read_auto` routing, `to_html` |
| `tests/io_db_test.v` *(new)* | SQLite attach/read/to_sql round-trip (network) |
| `tests/extensions_test.v` *(new)* | Extension caching + S3 credential SQL (network for install) |
| `CLAUDE.md` | Update `io.v` row; add `io_db.v` / `extensions.v` rows |
| `README.md` / `TUTORIAL.md` | Document new readers, DB integration, exports |
| `IMPLEMENTATION_ROADMAP.md` | Mark `to_excel`, `to_html`, `to_sql` complete |

---

# Phase 1 — Extension manager + file readers

## Task 1: Add the extension cache field to `DataFrameContext`

**Files:**
- Modify: `src/models.v`

- [ ] **Step 1: Add the field**

In `src/models.v`, change the `DataFrameContext` struct's mutable section to add the cache map:

```v
@[noinit]
struct DataFrameContext {
	dpath				string
mut:
	db					vduckdb.DuckDB
	loaded_extensions	map[string]bool
}
```

(V zero-initializes the map, so the existing `DataFrameContext{...}` construction sites in `core.v` need no change.)

- [ ] **Step 2: Verify it still compiles**

Run: `v test tests/`
Expected: PASS (all existing tests unchanged — this is an additive field).

- [ ] **Step 3: Commit**

```bash
git add src/models.v
git commit -m "feat: add loaded_extensions cache field to DataFrameContext"
```

---

## Task 2: Pure URI-routing helpers (offline-testable seam)

These functions decide whether a path is remote and which extension a path needs. Isolating them keeps routing logic testable without any DuckDB call or network.

**Files:**
- Create: `src/io.v` additions (append at end of file)
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test**

Create `tests/io_test.v`:

```v
import vframes

fn test_is_remote_path() {
	assert vframes.is_remote_path('https://example.com/data.csv') == true
	assert vframes.is_remote_path('http://example.com/data.csv') == true
	assert vframes.is_remote_path('s3://bucket/data.parquet') == true
	assert vframes.is_remote_path('gs://bucket/data.parquet') == true
	assert vframes.is_remote_path('az://container/data.csv') == true
	assert vframes.is_remote_path('/local/path/data.csv') == false
	assert vframes.is_remote_path('data.csv') == false
}

fn test_scheme_for_path() {
	// remote paths need httpfs
	assert vframes.scheme_for_path('s3://bucket/x.parquet') == 'httpfs'
	assert vframes.scheme_for_path('https://x/y.csv') == 'httpfs'
	// local xlsx needs excel
	assert vframes.scheme_for_path('report.xlsx') == 'excel'
	// plain local files need no extension
	assert vframes.scheme_for_path('data.csv') == ''
	assert vframes.scheme_for_path('data.parquet') == ''
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_test.v`
Expected: FAIL — `unknown function: vframes.is_remote_path`.

- [ ] **Step 3: Implement the helpers**

Append to `src/io.v`:

```v
// Returns true if the path points at a remote/cloud location (needs the httpfs extension).
pub fn is_remote_path(path string) bool {
	prefixes := ['http://', 'https://', 's3://', 'gs://', 'az://', 'azure://', 'r2://']
	for p in prefixes {
		if path.starts_with(p) {
			return true
		}
	}
	return false
}

// Returns the DuckDB extension a path requires before it can be read, or '' if none.
// Remote paths require 'httpfs'; local .xlsx files require 'excel'.
pub fn scheme_for_path(path string) string {
	if is_remote_path(path) {
		return 'httpfs'
	}
	if path.to_lower().ends_with('.xlsx') {
		return 'excel'
	}
	return ''
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/io_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: add URI-routing helpers for I/O"
```

---

## Task 3: The extension manager

**Files:**
- Create: `src/extensions.v`
- Test: `tests/extensions_test.v`

- [ ] **Step 1: Write the failing test (network)**

Create `tests/extensions_test.v`:

```v
import vframes

// (network) Installing an extension hits the DuckDB extension repo on first use.
fn test_ensure_extension_caches() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	// First call installs+loads; second call must be a no-op (cached).
	ctx.ensure_extension_pub('json')!
	assert ctx.extension_loaded('json') == true

	// Calling again does not error.
	ctx.ensure_extension_pub('json')!
	assert ctx.extension_loaded('json') == true
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/extensions_test.v`
Expected: FAIL — `unknown method ensure_extension_pub`.

- [ ] **Step 3: Implement the manager**

Create `src/extensions.v`:

```v
module vframes

// ensure_extension installs (once) and loads the named DuckDB extension.
// Auto-install on first use; cached afterwards so repeated reads pay nothing.
fn (mut ctx DataFrameContext) ensure_extension(name string) ! {
	if ctx.loaded_extensions[name] {
		return
	}
	mut db := &ctx.db
	db.query('INSTALL ${name}') or {
		return error('Failed to install DuckDB extension "${name}": ${err.msg()} — check network connectivity')
	}
	db.query('LOAD ${name}') or {
		return error('Failed to load DuckDB extension "${name}": ${err.msg()}')
	}
	ctx.loaded_extensions[name] = true
}

// ensure_extension_pub is a thin public wrapper used for testing and advanced use.
pub fn (mut ctx DataFrameContext) ensure_extension_pub(name string) ! {
	ctx.ensure_extension(name)!
}

// extension_loaded reports whether an extension has been loaded in this context.
pub fn (ctx DataFrameContext) extension_loaded(name string) bool {
	return ctx.loaded_extensions[name]
}
```

- [ ] **Step 4: Run test to verify it passes (network)**

Run: `v test tests/extensions_test.v`
Expected: PASS (requires connectivity to install `json`; `json` is usually bundled, making this robust).

- [ ] **Step 5: Commit**

```bash
git add src/extensions.v tests/extensions_test.v
git commit -m "feat: add DuckDB extension auto-install manager"
```

---

## Task 4: `read_csv` with options + remote routing

**Files:**
- Modify: `src/io.v`
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/io_test.v`:

```v
import os

fn test_read_csv_basic() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_${os.getpid()}.csv')
	os.write_file(tmp, 'id,name\n1,Alice\n2,Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp)!
	assert df.shape()![0] == 2
	cols := df.columns()!
	assert 'id' in cols
	assert 'name' in cols
}

fn test_read_csv_custom_delimiter() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_semi_${os.getpid()}.csv')
	os.write_file(tmp, 'id;name\n1;Alice\n2;Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp, delimiter: ';')!
	assert df.shape()![1] == 2  // two columns, correctly split
}

fn test_read_csv_type_override() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_type_${os.getpid()}.csv')
	os.write_file(tmp, 'id,name\n1,Alice\n2,Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp, columns: {'id': 'VARCHAR', 'name': 'VARCHAR'})!
	types := df.dtypes()!
	assert types['id'].to_upper().contains('VARCHAR')
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_test.v`
Expected: FAIL — `unknown method read_csv`.

- [ ] **Step 3: Implement `read_csv`**

Add to `src/io.v` (after the imports — note `import rand` is already present):

```v
@[params]
pub struct ReadCsvOptions {
pub:
	delimiter   string            // '' = DuckDB auto-detect
	header      bool = true       // used only when auto_header is false
	auto_header bool = true       // true = let DuckDB detect the header row
	columns     map[string]string // explicit name -> SQL type overrides
	all_varchar bool              // read every column as VARCHAR
	nullstr     string            // string treated as NULL
	sample_size int               // 0 = DuckDB default
}

// Reads a CSV file (local path or remote URL) into a new DataFrame.
// Supports glob patterns (e.g. 'data/*.csv') which DuckDB unions into one table.
pub fn (mut ctx DataFrameContext) read_csv(path string, opts ReadCsvOptions) !DataFrame {
	ext := scheme_for_path(path)
	if ext != '' {
		ctx.ensure_extension(ext)!
	}
	mut args := []string{}
	if opts.delimiter != '' {
		args << "delim='${opts.delimiter}'"
	}
	if !opts.auto_header {
		args << 'header=${opts.header}'
	}
	if opts.all_varchar {
		args << 'all_varchar=true'
	}
	if opts.nullstr != '' {
		args << "nullstr='${opts.nullstr}'"
	}
	if opts.sample_size > 0 {
		args << 'sample_size=${opts.sample_size}'
	}
	if opts.columns.len > 0 {
		mut pairs := []string{}
		for k, v in opts.columns {
			pairs << "'${k}': '${v}'"
		}
		args << 'columns={${pairs.join(', ')}}'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_csv('${path}'${arg_str})") or {
		return error('Failed to read CSV "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/io_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: add read_csv with options and remote routing"
```

---

## Task 5: `read_parquet` and `read_json`

**Files:**
- Modify: `src/io.v`
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/io_test.v`:

```v
fn test_read_parquet_local() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	// Existing fixture shipped in the repo.
	df := ctx.read_parquet('examples/titanic.parquet')!
	assert df.shape()![0] > 0
	assert df.columns()!.len > 0
}

fn test_read_json_local() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rj_${os.getpid()}.json')
	os.write_file(tmp, '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_json(tmp, format: 'array')!
	assert df.shape()![0] == 2
}
```

> Note: `test_read_parquet_local` reads `examples/titanic.parquet` using a path relative to the repo root. `v test tests/` runs with the repo root as the working directory, so this resolves correctly.

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_test.v`
Expected: FAIL — `unknown method read_parquet`.

- [ ] **Step 3: Implement both readers**

Add to `src/io.v`:

```v
@[params]
pub struct ReadParquetOptions {
pub:
	union_by_name    bool // union files by column name when globbing
	hive_partitioning bool
}

// Reads a Parquet file (local or remote) into a new DataFrame. Supports glob patterns.
pub fn (mut ctx DataFrameContext) read_parquet(path string, opts ReadParquetOptions) !DataFrame {
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.union_by_name {
		args << 'union_by_name=true'
	}
	if opts.hive_partitioning {
		args << 'hive_partitioning=true'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_parquet('${path}'${arg_str})") or {
		return error('Failed to read Parquet "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ReadJsonOptions {
pub:
	format  string            // '' (auto) | 'array' | 'newline_delimited' | 'unstructured'
	columns map[string]string // explicit name -> SQL type overrides
}

// Reads a JSON file (local or remote) into a new DataFrame.
pub fn (mut ctx DataFrameContext) read_json(path string, opts ReadJsonOptions) !DataFrame {
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.format != '' {
		args << "format='${opts.format}'"
	}
	if opts.columns.len > 0 {
		mut pairs := []string{}
		for k, v in opts.columns {
			pairs << "'${k}': '${v}'"
		}
		args << 'columns={${pairs.join(', ')}}'
	}
	arg_str := if args.len > 0 { ', ' + args.join(', ') } else { '' }
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_json('${path}'${arg_str})") or {
		return error('Failed to read JSON "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/io_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: add read_parquet and read_json with options"
```

---

## Task 6: Enhance `read_auto` to route by scheme and extension

**Files:**
- Modify: `src/io.v` (the existing `read_auto` function)
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/io_test.v`:

```v
fn test_read_auto_routes_parquet() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	df := ctx.read_auto('examples/titanic.parquet')!
	assert df.shape()![0] > 0
}

fn test_read_auto_local_csv_still_works() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_ra_${os.getpid()}.csv')
	os.write_file(tmp, 'id,name\n1,Alice\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_auto(tmp)!
	assert df.shape()![0] == 1
}
```

- [ ] **Step 2: Run test to verify it fails or regresses**

Run: `v test tests/io_test.v`
Expected: `test_read_auto_routes_parquet` should already pass via DuckDB auto-detect, but a remote `.xlsx` would not load its extension. We update `read_auto` so all routing is explicit and consistent. Run to capture the baseline.

- [ ] **Step 3: Update `read_auto`**

Replace the existing `read_auto` function body in `src/io.v` with:

```v
// Reads a data file from disk or a remote URL, inferring the format.
// Accepted formats: .csv, .json, .parquet, .xlsx. Remote URLs (http/https/s3/...) are supported.
pub fn (mut ctx DataFrameContext) read_auto(filename string) !DataFrame {
	// Local non-glob files must exist; remote and glob paths are validated by DuckDB.
	if !is_remote_path(filename) && !filename.contains('*') && !os.is_file(filename) {
		return error('Incorrect filename: ${filename}')
	}
	lower := filename.to_lower()
	if lower.ends_with('.xlsx') {
		return ctx.read_excel(filename)
	}
	// Ensure httpfs for remote files so the generic reader can fetch them.
	if is_remote_path(filename) {
		ctx.ensure_extension('httpfs')!
	}
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from '${filename}'") or { return err }
	return DataFrame{
		id: id
		ctx: ctx
	}
}
```

> `read_excel` is implemented in Task 7. If executing tasks strictly in order, temporarily comment the `.xlsx` branch and restore it after Task 7, or implement Task 7 before running this task's full suite. Prefer implementing Task 7 next so the branch resolves.

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/io_test.v`
Expected: PASS (with the `.xlsx` branch resolved by Task 7).

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: route read_auto by scheme and extension"
```

---

# Phase 2 — Excel

## Task 7: `read_excel` and `to_excel`

**Files:**
- Modify: `src/io.v`
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test (network)**

Add to `tests/io_test.v`:

```v
// (network) Requires the DuckDB 'excel' extension.
fn test_excel_round_trip() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	data := [
		{'id': 1, 'name': 'Alice'},
		{'id': 2, 'name': 'Bob'},
	].map(|r| {
		mut m := map[string]json2.Any{}
		m['id'] = r['id']
		m['name'] = r['name']
		m
	})
	df := ctx.read_records(data)!

	xlsx_path := os.join_path_single(os.temp_dir(), 'vframes_xl_${os.getpid()}.xlsx')
	defer { os.rm(xlsx_path) or {} }

	df.to_excel(xlsx_path)!
	assert os.is_file(xlsx_path)

	df2 := ctx.read_excel(xlsx_path)!
	assert df2.shape()![0] == 2
}
```

> The `import json2` line at the top of `tests/io_test.v` must be present for this test; add `import x.json2` to the test file's imports if not already there.

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_test.v`
Expected: FAIL — `unknown method to_excel` / `read_excel`.

- [ ] **Step 3: Implement both**

Add to `src/io.v`:

```v
@[params]
pub struct ReadExcelOptions {
pub:
	sheet  string // sheet name; '' = first sheet
	range  string // cell range, e.g. 'A1:D100'; '' = full sheet
	header bool = true
}

// Reads an Excel (.xlsx) file into a new DataFrame using the DuckDB 'excel' extension.
pub fn (mut ctx DataFrameContext) read_excel(path string, opts ReadExcelOptions) !DataFrame {
	ctx.ensure_extension('excel')!
	if is_remote_path(path) {
		ctx.ensure_extension('httpfs')!
	}
	mut args := []string{}
	if opts.sheet != '' {
		args << "sheet='${opts.sheet}'"
	}
	if opts.range != '' {
		args << "range='${opts.range}'"
	}
	args << 'header=${opts.header}'
	arg_str := ', ' + args.join(', ')
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query("create table ${id} as select * from read_xlsx('${path}'${arg_str})") or {
		return error('Failed to read Excel "${path}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

@[params]
pub struct ToExcelOptions {
pub:
	header bool = true
	sheet  string = 'Sheet1'
}

// Exports the DataFrame to an Excel (.xlsx) file using the DuckDB 'excel' extension.
pub fn (df DataFrame) to_excel(path string, opts ToExcelOptions) ! {
	mut ctx := df.ctx
	ctx.ensure_extension('excel')!
	mut db := &df.ctx.db
	header_stmt := if opts.header { 'true' } else { 'false' }
	db.query("COPY (SELECT * FROM ${df.id}) TO '${path}' (FORMAT xlsx, HEADER ${header_stmt}, SHEET '${opts.sheet}')") or {
		return error('Failed to write Excel "${path}": ${err.msg()}')
	}
}
```

- [ ] **Step 4: Run test to verify it passes (network)**

Run: `v test tests/io_test.v`
Expected: PASS (requires the `excel` extension to install).

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: add read_excel and to_excel via the excel extension"
```

---

# Phase 3 — Database integration

## Task 8: `attach` / `detach` + `read_sql` + `read_table`

**Files:**
- Create: `src/io_db.v`
- Test: `tests/io_db_test.v`

- [ ] **Step 1: Write the failing test (network)**

Create `tests/io_db_test.v`:

```v
import vframes
import os

// (network) Requires the DuckDB 'sqlite' extension.
fn test_attach_sqlite_and_read() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	// Build a tiny SQLite db file using DuckDB itself, then read it back.
	db_path := os.join_path_single(os.temp_dir(), 'vframes_sqlite_${os.getpid()}.db')
	defer { os.rm(db_path) or {} }

	ctx.attach(db_path, alias: 'src', db_type: .sqlite, read_only: false)!
	// Seed a table inside the attached sqlite db via read_sql escape hatch.
	_ := ctx.read_sql("CREATE TABLE src.people AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'") or {
		// CREATE returns no rows; tolerate empty-result wrap by re-reading.
		vframes.empty()!
	}

	df := ctx.read_table('src.people')!
	assert df.shape()![0] == 2
	cols := df.columns()!
	assert 'id' in cols
	assert 'name' in cols

	ctx.detach('src')!
}

// (network) read_sql as a general escape hatch against in-memory tables.
fn test_read_sql_escape_hatch() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	df := ctx.read_sql("SELECT 10 AS a, 20 AS b UNION ALL SELECT 30, 40")!
	assert df.shape()![0] == 2
	assert df.shape()![1] == 2
}
```

> The `read_sql` escape-hatch test (`test_read_sql_escape_hatch`) needs **no** network — it runs against DuckDB's core engine.

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_db_test.v`
Expected: FAIL — `unknown method attach` / `read_sql` / `read_table` / `detach`.

- [ ] **Step 3: Implement the DB module**

Create `src/io_db.v`:

```v
module vframes

import rand

pub enum DbType {
	postgres
	mysql
	sqlite
	duckdb
}

fn (t DbType) extension() string {
	return match t {
		.postgres { 'postgres' }
		.mysql { 'mysql' }
		.sqlite { 'sqlite' }
		.duckdb { '' } // built in
	}
}

fn (t DbType) attach_type() string {
	return match t {
		.postgres { 'postgres' }
		.mysql { 'mysql' }
		.sqlite { 'sqlite' }
		.duckdb { 'duckdb' }
	}
}

@[params]
pub struct AttachOptions {
pub:
	alias     string
	db_type   DbType = .duckdb
	read_only bool = true
}

// attach registers an external database (Postgres/MySQL/SQLite/DuckDB) under an alias.
// The relevant scanner extension is auto-installed on first use.
pub fn (mut ctx DataFrameContext) attach(dsn string, opts AttachOptions) ! {
	if opts.alias == '' {
		return error('attach requires a non-empty alias')
	}
	ext := opts.db_type.extension()
	if ext != '' {
		ctx.ensure_extension(ext)!
	}
	read_only_clause := if opts.read_only { ', READ_ONLY' } else { '' }
	mut db := &ctx.db
	db.query("ATTACH '${dsn}' AS ${opts.alias} (TYPE ${opts.db_type.attach_type()}${read_only_clause})") or {
		return error('Failed to attach "${dsn}" as "${opts.alias}": ${err.msg()}')
	}
}

// detach removes a previously attached database alias.
pub fn (mut ctx DataFrameContext) detach(alias string) ! {
	mut db := &ctx.db
	db.query('DETACH ${alias}') or {
		return error('Failed to detach "${alias}": ${err.msg()}')
	}
}

// read_sql runs an arbitrary SELECT (or DDL) and returns the result as a new DataFrame.
// Doubles as a raw-SQL escape hatch against any loaded/attached table.
pub fn (mut ctx DataFrameContext) read_sql(query string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query('CREATE TABLE ${id} AS (${query})') or {
		return error('read_sql failed: ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}

// read_table reads a fully-qualified table (e.g. 'alias.schema.table') into a new DataFrame.
pub fn (mut ctx DataFrameContext) read_table(qualified string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &ctx.db
	db.query('CREATE TABLE ${id} AS SELECT * FROM ${qualified}') or {
		return error('Failed to read table "${qualified}": ${err.msg()}')
	}
	return DataFrame{
		id: id
		ctx: ctx
	}
}
```

> The seed step in `test_attach_sqlite_and_read` uses `read_sql` to run a `CREATE TABLE ... AS`. Because `read_sql` itself wraps the query in `CREATE TABLE <id> AS (...)`, a bare DDL won't wrap cleanly. Adjust the test seed to issue the DDL directly through a dedicated path: replace that seed line with `ctx.exec_sql("CREATE TABLE src.people AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'")!` and add the `exec_sql` helper below.

- [ ] **Step 4: Add `exec_sql` for non-SELECT statements**

Add to `src/io_db.v`:

```v
// exec_sql runs a statement that returns no DataFrame (DDL/DML such as CREATE/INSERT/UPDATE).
pub fn (mut ctx DataFrameContext) exec_sql(stmt string) ! {
	mut db := &ctx.db
	db.query(stmt) or {
		return error('exec_sql failed: ${err.msg()}')
	}
}
```

Then update the seed line in `tests/io_db_test.v` to:

```v
	ctx.exec_sql("CREATE TABLE src.people AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'")!
```

(Remove the earlier `_ := ctx.read_sql(...) or { vframes.empty()! }` block.)

- [ ] **Step 5: Run test to verify it passes (network for sqlite test)**

Run: `v test tests/io_db_test.v`
Expected: PASS (`test_read_sql_escape_hatch` offline; `test_attach_sqlite_and_read` needs the `sqlite` extension).

- [ ] **Step 6: Commit**

```bash
git add src/io_db.v tests/io_db_test.v
git commit -m "feat: add database attach/detach, read_sql, read_table, exec_sql"
```

---

## Task 9: `read_database` convenience + `to_sql`

**Files:**
- Modify: `src/io_db.v`
- Test: `tests/io_db_test.v`

- [ ] **Step 1: Write the failing test (network)**

Add to `tests/io_db_test.v`:

```v
// (network) Requires the DuckDB 'sqlite' extension.
fn test_to_sql_round_trip() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	db_path := os.join_path_single(os.temp_dir(), 'vframes_tosql_${os.getpid()}.db')
	defer { os.rm(db_path) or {} }

	ctx.attach(db_path, alias: 'dst', db_type: .sqlite, read_only: false)!

	src := ctx.read_sql("SELECT 1 AS id, 'X' AS label UNION ALL SELECT 2, 'Y'")!
	src.to_sql('people', alias: 'dst', if_exists: 'replace')!

	back := ctx.read_table('dst.people')!
	assert back.shape()![0] == 2

	ctx.detach('dst')!
}

fn test_read_database_one_shot() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	db_path := os.join_path_single(os.temp_dir(), 'vframes_rdb_${os.getpid()}.db')
	defer { os.rm(db_path) or {} }

	// Seed via a temporary attach.
	ctx.attach(db_path, alias: 'seed', db_type: .sqlite, read_only: false)!
	ctx.exec_sql("CREATE TABLE seed.t AS SELECT 42 AS answer")!
	ctx.detach('seed')!

	df := ctx.read_database(db_path, 'SELECT * FROM onesh.t', alias: 'onesh', db_type: .sqlite)!
	assert df.shape()![0] == 1
}
```

> In `read_database`, the caller writes the query using the same alias they pass in `opts.alias` (here `onesh`). The function attaches under that alias, runs the query, then detaches.

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_db_test.v`
Expected: FAIL — `unknown method to_sql` / `read_database`.

- [ ] **Step 3: Implement both**

Add to `src/io_db.v`:

```v
@[params]
pub struct ToSqlOptions {
pub:
	alias     string // target attached-database alias (required)
	if_exists string = 'fail' // 'fail' | 'replace' | 'append'
}

// to_sql writes the DataFrame into a table inside an attached database.
pub fn (df DataFrame) to_sql(table string, opts ToSqlOptions) ! {
	if opts.alias == '' {
		return error('to_sql requires opts.alias (the attached-database alias)')
	}
	target := '${opts.alias}.${table}'
	mut db := &df.ctx.db
	match opts.if_exists {
		'replace' {
			db.query('DROP TABLE IF EXISTS ${target}') or {
				return error('to_sql failed dropping "${target}": ${err.msg()}')
			}
			db.query('CREATE TABLE ${target} AS SELECT * FROM ${df.id}') or {
				return error('to_sql failed creating "${target}": ${err.msg()}')
			}
		}
		'append' {
			db.query('INSERT INTO ${target} SELECT * FROM ${df.id}') or {
				return error('to_sql failed appending to "${target}": ${err.msg()}')
			}
		}
		else { // 'fail'
			db.query('CREATE TABLE ${target} AS SELECT * FROM ${df.id}') or {
				return error('to_sql failed creating "${target}" (does it already exist?): ${err.msg()}')
			}
		}
	}
}

// read_database attaches a database, runs a query, returns a DataFrame, then detaches.
pub fn (mut ctx DataFrameContext) read_database(dsn string, query string, opts AttachOptions) !DataFrame {
	if opts.alias == '' {
		return error('read_database requires a non-empty alias (referenced in the query)')
	}
	ctx.attach(dsn, opts)!
	df := ctx.read_sql(query) or {
		ctx.detach(opts.alias) or {}
		return err
	}
	ctx.detach(opts.alias)!
	return df
}
```

- [ ] **Step 4: Run test to verify it passes (network)**

Run: `v test tests/io_db_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/io_db.v tests/io_db_test.v
git commit -m "feat: add to_sql and read_database one-shot reader"
```

---

# Phase 4 — `to_html` + S3 credentials

## Task 10: `to_html`

**Files:**
- Modify: `src/io.v`
- Test: `tests/io_test.v`

- [ ] **Step 1: Write the failing test**

Add to `tests/io_test.v`:

```v
fn test_to_html() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	df := ctx.read_sql("SELECT 'Alice' AS name, 30 AS age UNION ALL SELECT 'Bob', 25")!
	html := df.to_html()!
	assert html.contains('<table')
	assert html.contains('<th>name</th>')
	assert html.contains('<td>Alice</td>')
	assert html.contains('</table>')
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/io_test.v`
Expected: FAIL — `unknown method to_html`.

- [ ] **Step 3: Implement `to_html`**

Add to `src/io.v` (mirrors the existing `to_markdown`):

```v
// Returns the DataFrame as an HTML <table> string.
pub fn (df DataFrame) to_html() !string {
	mut db := &df.ctx.db
	db.query('SELECT * FROM ${df.id}') or { return err }
	cols := db.columns.keys()
	rows := db.get_array()

	mut lines := []string{}
	lines << '<table>'
	lines << '  <thead>'
	header_cells := cols.map('<th>${it}</th>').join('')
	lines << '    <tr>${header_cells}</tr>'
	lines << '  </thead>'
	lines << '  <tbody>'
	for row in rows {
		cells := cols.map('<td>${(row[it] or { json2.Any('') }).str()}</td>').join('')
		lines << '    <tr>${cells}</tr>'
	}
	lines << '  </tbody>'
	lines << '</table>'
	return lines.join('\n')
}
```

> `to_markdown` already uses `json2` in `io.v`, so the `import x.json2` line is present. No new import needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `v test tests/io_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/io.v tests/io_test.v
git commit -m "feat: add to_html exporter"
```

---

## Task 11: `set_s3_credentials`

**Files:**
- Modify: `src/extensions.v`
- Test: `tests/extensions_test.v`

- [ ] **Step 1: Write the failing test (network)**

Add to `tests/extensions_test.v`:

```v
// (network) Creating an S3 secret requires the httpfs extension to load.
fn test_set_s3_credentials_creates_secret() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	// Should not error; we only verify the secret is registered, not that it works.
	ctx.set_s3_credentials(key_id: 'AKIATEST', secret: 'secrettest', region: 'us-west-2')!
	assert ctx.extension_loaded('httpfs') == true
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `v test tests/extensions_test.v`
Expected: FAIL — `unknown method set_s3_credentials`.

- [ ] **Step 3: Implement `set_s3_credentials`**

Add to `src/extensions.v`:

```v
@[params]
pub struct S3Credentials {
pub:
	key_id        string
	secret        string
	region        string = 'us-east-1'
	endpoint      string
	session_token string
	url_style     string // 'vhost' | 'path'
}

// set_s3_credentials registers an S3 secret for httpfs-backed remote reads.
// Credentials are passed to DuckDB's secret store and are never logged.
// If credentials are omitted, httpfs falls back to DuckDB's default credential chain.
pub fn (mut ctx DataFrameContext) set_s3_credentials(creds S3Credentials) ! {
	ctx.ensure_extension('httpfs')!
	mut parts := []string{}
	parts << 'TYPE s3'
	if creds.key_id != '' {
		parts << "KEY_ID '${creds.key_id}'"
	}
	if creds.secret != '' {
		parts << "SECRET '${creds.secret}'"
	}
	if creds.region != '' {
		parts << "REGION '${creds.region}'"
	}
	if creds.endpoint != '' {
		parts << "ENDPOINT '${creds.endpoint}'"
	}
	if creds.session_token != '' {
		parts << "SESSION_TOKEN '${creds.session_token}'"
	}
	if creds.url_style != '' {
		parts << "URL_STYLE '${creds.url_style}'"
	}
	mut db := &ctx.db
	db.query('CREATE OR REPLACE SECRET vframes_s3 (${parts.join(', ')})') or {
		return error('Failed to set S3 credentials: ${err.msg()}')
	}
}
```

- [ ] **Step 4: Run test to verify it passes (network)**

Run: `v test tests/extensions_test.v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/extensions.v tests/extensions_test.v
git commit -m "feat: add set_s3_credentials via DuckDB secrets"
```

---

## Task 12: Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `TUTORIAL.md`
- Modify: `IMPLEMENTATION_ROADMAP.md`

- [ ] **Step 1: Update `CLAUDE.md` source-files table**

In the `### Source files (src/)` table, update the `io.v` row and add two rows:

```markdown
| `io.v` | File I/O: `read_auto()`, `read_csv/json/parquet/excel()`, `read_records()`, `to_csv/json/parquet/excel/html()`, `to_dict()`, `to_markdown()` |
| `io_db.v` | Database integration: `attach()`, `detach()`, `read_sql()`, `read_table()`, `read_database()`, `exec_sql()`, `to_sql()` |
| `extensions.v` | DuckDB extension auto-install manager and `set_s3_credentials()` |
```

- [ ] **Step 2: Update `README.md`**

Add a section after the existing I/O documentation:

```markdown
### Loading data

```v
df := ctx.read_csv('data.csv', delimiter: ';')!          // local or remote
df := ctx.read_parquet('s3://bucket/*.parquet')!         // glob + cloud
df := ctx.read_excel('report.xlsx', sheet: 'Q1')!        // Excel
df := ctx.read_json('https://host/data.json')!           // remote JSON
```

### Databases

```v
ctx.attach('host.db', alias: 'src', db_type: .sqlite)!
people := ctx.read_table('src.people')!
people.to_sql('backup', alias: 'src', if_exists: 'replace')!
ctx.detach('src')!

// one-shot
df := ctx.read_database('host.db', 'SELECT * FROM s.t', alias: 's', db_type: .sqlite)!
```

### Cloud credentials

```v
ctx.set_s3_credentials(key_id: '...', secret: '...', region: 'us-west-2')!
```

### Exporting

```v
df.to_excel('out.xlsx')!
html := df.to_html()!
```
```

- [ ] **Step 3: Update `TUTORIAL.md`**

Add equivalent prose examples for the new readers, database integration, and exporters, matching the tutorial's existing style.

- [ ] **Step 4: Update `IMPLEMENTATION_ROADMAP.md`**

In the Phase 6 export table, change the status of `to_excel`, `to_html`, and `to_sql` from `[ ]` to `[X]`.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md TUTORIAL.md IMPLEMENTATION_ROADMAP.md
git commit -m "docs: document I/O expansion (loaders, DB, exports)"
```

---

## Task 13: Full suite verification

**Files:** none (verification only)

- [ ] **Step 1: Run the entire test suite**

Run: `v test tests/`
Expected: PASS for all offline tests. Network-marked tests pass when connectivity is available; if offline, note which were skipped/failed due to extension install.

- [ ] **Step 2: Confirm the module compiles cleanly as a whole**

Run: `v -shared src/` (or `make link && v test tests/`)
Expected: no compilation errors.

- [ ] **Step 3: Final commit (if any doc/cleanup remains)**

```bash
git add -A
git commit -m "chore: finalize I/O expansion" || echo "nothing to commit"
```

---

## Self-Review Notes

**Spec coverage check:**
- Remote & cloud files → Tasks 2, 4, 5 (httpfs routing) + Task 11 (S3 creds). ✓
- Explicit format readers → Tasks 4 (csv), 5 (parquet/json), 7 (excel). ✓
- Database sources → Tasks 8, 9. ✓
- Export gaps → Task 7 (to_excel), Task 9 (to_sql), Task 10 (to_html); `to_arrow` correctly omitted per spec. ✓
- Extension auto-install → Task 3. ✓
- File organization (io.v / io_db.v / extensions.v) → Tasks 3, 8, all io tasks. ✓
- Testing strategy (sqlite-first DB, offline routing seam, guarded excel/cloud) → Tasks 2, 7, 8, 9. ✓

**Type consistency check:**
- `ensure_extension` (private) / `ensure_extension_pub` / `extension_loaded` used consistently across Tasks 3–11.
- `DbType` enum values (`.postgres/.mysql/.sqlite/.duckdb`) and `AttachOptions` (`alias`, `db_type`, `read_only`) consistent across Tasks 8, 9.
- `read_sql` wraps in `CREATE TABLE <id> AS (...)`; `exec_sql` added (Task 8) for DDL/DML that must not be wrapped — resolves the sqlite seed-step issue.
- `ToSqlOptions` (`alias`, `if_exists`) consistent in Task 9 and README.

**Known ordering dependency:** Task 6 (`read_auto`) references `read_excel` from Task 7 — implement Task 7 immediately after Task 6 (or temporarily stub the `.xlsx` branch). Flagged inline in Task 6.
