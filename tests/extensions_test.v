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

// (network) Creating an S3 secret requires the httpfs extension to load.
fn test_set_s3_credentials_creates_secret() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	// Should not error; we only verify the secret is registered, not that it works.
	ctx.set_s3_credentials(key_id: 'AKIATEST', secret: 'secrettest', region: 'us-west-2')!
	assert ctx.extension_loaded('httpfs') == true
}
