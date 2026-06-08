import vframes
import os

// (network) Requires the DuckDB 'sqlite' extension.
fn test_attach_sqlite_and_read() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	db_path := os.join_path_single(os.temp_dir(), 'vframes_sqlite_${os.getpid()}.db')
	defer { os.rm(db_path) or {} }

	ctx.attach(db_path, alias: 'src', db_type: .sqlite, read_only: false)!
	// Seed a table inside the attached sqlite db directly (DDL, not a SELECT).
	ctx.exec_sql("CREATE TABLE src.people AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'")!

	df := ctx.read_table('src.people')!
	assert df.shape()![0] == 2
	cols := df.columns()!
	assert 'id' in cols
	assert 'name' in cols

	ctx.detach('src')!
}

// read_sql as a general escape hatch against in-memory tables (no network needed).
fn test_read_sql_escape_hatch() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	df := ctx.read_sql('SELECT 10 AS a, 20 AS b UNION ALL SELECT 30, 40')!
	assert df.shape()![0] == 2
	assert df.shape()![1] == 2
}
