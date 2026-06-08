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
