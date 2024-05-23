module vframes

import rodabt.vduckdb
import rand
import v.vmod


pub fn init(cfg ContextConfig) DataFrameContext {
	mut db := vduckdb.DuckDB{}
	_ := db.open(cfg.location) or { panic(err) }
	_ := db.query("select 1") or { panic(err) }
	return DataFrameContext{
		dpath: cfg.location
		db: db 
	}
}

pub fn (mut ctx DataFrameContext) close() {
	ctx.db.close()
}

pub fn version() string {
	vm := vmod.decode(@VMOD_FILE) or { panic(err) }
	return vm.version
}

pub fn empty() DataFrame {
	mut ctx := init()
	id := 'tbl_${rand.ulid()}'
	return DataFrame{
		id: id
		ctx: ctx
	}
}