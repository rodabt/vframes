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

// (network) Requires the DuckDB 'sqlite' extension.
fn test_read_database_one_shot() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	db_path := os.join_path_single(os.temp_dir(), 'vframes_rdb_${os.getpid()}.db')
	defer { os.rm(db_path) or {} }

	// Seed via a temporary attach.
	ctx.attach(db_path, alias: 'seed', db_type: .sqlite, read_only: false)!
	ctx.exec_sql('CREATE TABLE seed.t AS SELECT 42 AS answer')!
	ctx.detach('seed')!

	df := ctx.read_database(db_path, 'SELECT * FROM onesh.t', alias: 'onesh', db_type: .sqlite)!
	assert df.shape()![0] == 1
}
