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
