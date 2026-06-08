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
