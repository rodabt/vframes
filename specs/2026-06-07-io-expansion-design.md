# VFrames I/O Expansion — Design

**Date:** 2026-06-07
**Status:** Approved (design); pending implementation plan
**Scope:** Expand the VFrames I/O subsystem with remote/cloud file loaders, explicit
format readers, live database integration, and additional export formats.

## Background

VFrames is a Pandas-like DataFrame library for V backed by an embedded DuckDB
instance. Its transformation/analysis surface (filtering, grouping, joins, pivots,
rolling windows, stats) is already broad. Its **I/O surface is thin**: `io.v`
(93 lines) offers only `read_auto` (local CSV/JSON/Parquet), `read_records`, and
exports to CSV/JSON/Parquet/dict/markdown.

This effort closes the I/O gap. It is the first of several independent improvement
cycles (others: charting, table-lifecycle/memory fix, a lazy `sql()` builder).

### Key constraint: vduckdb is SQL-only

The `vduckdb` binding exposes only a SQL-string interface (`db.query()` plus
`get_array`/`get_array_as_string`/`columns`/`print_table`/`dim`). There is **no
Arrow C-data-interface binding**. Consequences:

1. Every capability in this design is achievable purely through SQL that DuckDB
   already understands (`INSTALL`/`LOAD`, `read_csv(...)`, `ATTACH`, `COPY ... TO`).
   No new C bindings are required.
2. `to_arrow` is **not feasible** and is out of scope.

## Goals

- Read CSV/JSON/Parquet from local paths, HTTPS URLs, and S3 (and other cloud
  schemes) with per-format options and multi-file globbing.
- Read Excel (`.xlsx`) files.
- Read from and write to live databases (Postgres, MySQL, SQLite, DuckDB files)
  via `ATTACH` and the DB scanner extensions.
- Add export formats: `to_excel`, `to_html`, `to_sql`.
- Manage DuckDB extensions transparently (auto-install on first use, cached).

## Non-goals (this cycle)

- `to_arrow` (no binding support).
- A full lazy/chainable `sql()` query builder (`read_sql` provides a basic
  escape hatch; the full builder is a separate cycle).
- The orphan intermediate-table memory fix (separate cycle).
- Production-grade Postgres/MySQL CI testing (covered manually; SQLite is the
  CI-tested DB path).

## Design

### A. File organization

| File | Responsibility |
|------|---------------|
| `io.v` | File readers (`read_auto`, `read_csv/json/parquet/excel`, `read_records`) and file exports (`to_csv/json/parquet/excel/html`, `to_markdown`, `to_dict`) |
| `io_db.v` *(new)* | Database integration: `attach`, `detach`, `read_sql`, `read_table`, `read_database`, `to_sql` |
| `extensions.v` *(new)* | Extension auto-install/load manager and S3 credential handling |

`models.v` changes: add a `loaded_extensions map[string]bool` field to
`DataFrameContext` (mutable section).

### B. Extension manager (foundation)

Auto-install on first use, cached to avoid repeated `INSTALL`/`LOAD`:

```v
fn (mut ctx DataFrameContext) ensure_extension(name string) ! {
    if ctx.loaded_extensions[name] {
        return
    }
    ctx.db.query('INSTALL ${name}') or {
        return error('Failed to install DuckDB extension "${name}": ${err.msg()} — check network connectivity')
    }
    ctx.db.query('LOAD ${name}') or {
        return error('Failed to load DuckDB extension "${name}": ${err.msg()}')
    }
    ctx.loaded_extensions[name] = true
}
```

Extension mapping:

| Need | Extension |
|------|-----------|
| HTTPS / S3 / cloud reads | `httpfs` |
| Excel read/write | `excel` |
| Postgres source | `postgres` |
| MySQL source | `mysql` |
| SQLite source | `sqlite` |
| DuckDB file source | (none — built in) |

### C. File readers

Explicit readers with option structs. `read_auto` is retained and made smarter:
it routes by file extension **and** URI scheme to the appropriate reader with
sensible defaults.

```v
pub fn (mut ctx DataFrameContext) read_csv(path string, opts ReadCsvOptions) !DataFrame
pub fn (mut ctx DataFrameContext) read_json(path string, opts ReadJsonOptions) !DataFrame
pub fn (mut ctx DataFrameContext) read_parquet(path string, opts ReadParquetOptions) !DataFrame
pub fn (mut ctx DataFrameContext) read_excel(path string, opts ReadExcelOptions) !DataFrame
```

`ReadCsvOptions` fields: `delimiter`, `header`, `columns map[string]string`
(explicit name→type overrides), `sample_size`, `all_varchar`, `nullstr`.

`ReadExcelOptions` fields: `sheet`, `range`, `header`.

Behavior:

- **Remote routing:** paths beginning `http://`, `https://`, `s3://`, `gs://`,
  or `az://` trigger `ensure_extension('httpfs')` before the read.
- **Globbing:** glob patterns (e.g. `read_parquet('data/*.parquet')`) are passed
  straight to DuckDB, which unions matching files into one DataFrame.
- Each reader builds a `CREATE TABLE tbl_<ulid> AS SELECT * FROM read_<fmt>(...)`
  statement with the options translated to DuckDB reader arguments.

### D. Database integration (`io_db.v`)

```v
pub enum DbType {
    postgres
    mysql
    sqlite
    duckdb
}

pub fn (mut ctx DataFrameContext) attach(dsn string, opts AttachOptions) !
pub fn (mut ctx DataFrameContext) detach(alias string) !
pub fn (mut ctx DataFrameContext) read_sql(query string) !DataFrame
pub fn (mut ctx DataFrameContext) read_table(qualified string) !DataFrame
pub fn (mut ctx DataFrameContext) read_database(dsn string, query string, opts AttachOptions) !DataFrame
```

`AttachOptions` fields: `alias`, `db_type DbType`, `read_only bool = true`.

- `attach` loads the appropriate scanner extension for `db_type`, then runs
  `ATTACH '<dsn>' AS <alias> (TYPE <type>, READ_ONLY)`.
- `read_sql(query)` wraps any `SELECT` as a DataFrame
  (`CREATE TABLE tbl_<ulid> AS (<query>)`). This also serves as a basic raw-SQL
  escape hatch against any loaded table.
- `read_table('alias.schema.table')` is sugar for `SELECT * FROM <qualified>`.
- `read_database` is a convenience: attach → read → detach, for one-off reads.

### E. Exports

- `to_excel(path string, opts ToExcelOptions) !` — `ensure_extension('excel')`,
  then `COPY (SELECT * FROM <id>) TO '<path>' (FORMAT xlsx, HEADER ...)`.
- `to_html() !string` — pure V, mirrors the existing `to_markdown` (builds an
  HTML `<table>` string; no extension required).
- `to_sql(target string, opts ToSqlOptions) !` — write the DataFrame into an
  attached database: `CREATE TABLE <alias>.<table> AS SELECT * FROM <id>`.
  `opts.if_exists` ∈ `fail` / `replace` / `append`.

### F. S3 / cloud credentials

```v
pub fn (mut ctx DataFrameContext) set_s3_credentials(creds S3Credentials) !
```

`S3Credentials` fields: `key_id`, `secret`, `region` (default `us-east-1`),
`endpoint`, `session_token`, `url_style`.

Implementation: `ensure_extension('httpfs')`, then
`CREATE OR REPLACE SECRET (TYPE s3, KEY_ID '...', SECRET '...', REGION '...', ...)`.
Credentials are never written to logs or error messages. If credentials are not
set, httpfs falls back to DuckDB's default credential chain (environment
variables / instance profile).

## Error handling

- Extension install/load failures surface a clear message naming the extension
  and pointing at network connectivity.
- All readers return `!DataFrame`; all exports return `!`, matching the existing
  module convention.
- DB attach/read failures propagate the underlying DuckDB error message.

## Testing strategy

- **File readers + options:** unit tests using existing fixtures
  (`examples/data.json`, `examples/titanic.parquet`) and small generated CSV
  files; assert row/column counts and option effects (delimiter, type overrides,
  glob union).
- **Database:** **SQLite first** (file-based, no server, CI-friendly): create a
  temp SQLite DB, `attach`, `read_table`/`read_sql`, and `to_sql` round-trip.
  Postgres/MySQL are validated manually and documented as optional.
- **Excel:** generate a small `.xlsx` fixture (the local `xlsx` V module can
  create one in a test helper), then round-trip read/write; skip gracefully when
  the `excel` extension cannot be installed offline.
- **Cloud:** test the URI-scheme **routing** logic without network access (verify
  the correct extension is requested); one optional live test against a public
  HTTPS file, guarded so it does not fail offline CI.
- **`to_html`:** pure string-output unit test.

## Implementation phases

1. Extension manager + file readers (CSV/JSON/Parquet) with options + remote
   URI routing.
2. Excel read (`read_excel`) and write (`to_excel`).
3. Database integration (`attach`/`detach`/`read_sql`/`read_table`/
   `read_database`/`to_sql`), SQLite-first.
4. `to_html` + S3 credentials (`set_s3_credentials`).

## Documentation impact

- Update `README.md` and `TUTORIAL.md` with the new readers, DB integration, and
  export formats.
- Update `CLAUDE.md`'s `io.v` row and add `io_db.v` / `extensions.v` to the
  source-files table.
- Update `IMPLEMENTATION_ROADMAP.md` / `TODO.md` to mark `to_excel`, `to_html`,
  `to_sql` complete.
