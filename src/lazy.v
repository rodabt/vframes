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
