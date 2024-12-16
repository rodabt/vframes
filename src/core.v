module vframes

import vduckdb
import rand
import v.vmod

// Initializes a new DataFrame context
pub fn init(cfg ContextConfig) DataFrameContext {
	mut db := vduckdb.DuckDB{}
	_ := db.open(cfg.location) or { panic(err) }
	_ := db.query("select 1") or { panic(err) }
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
pub fn version() string {
	vm := vmod.decode(@VMOD_FILE) or { panic(err) }
	return vm.version
}

// Returns an empty in-memory DataFrame. Mainly used as a Result parameter for `read_auto` function
pub fn empty() DataFrame {
	mut ctx := init()
	id := 'tbl_${rand.ulid()}'
	return DataFrame{
		id: id
		ctx: ctx
	}
}