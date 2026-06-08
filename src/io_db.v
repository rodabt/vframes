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

// read_sql runs an arbitrary SELECT and returns the result as a new DataFrame.
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

// exec_sql runs a statement that returns no DataFrame (DDL/DML such as CREATE/INSERT/UPDATE).
pub fn (mut ctx DataFrameContext) exec_sql(stmt string) ! {
	mut db := &ctx.db
	db.query(stmt) or {
		return error('exec_sql failed: ${err.msg()}')
	}
}

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
