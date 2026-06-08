import vframes

fn test_is_remote_path() {
	assert vframes.is_remote_path('https://example.com/data.csv') == true
	assert vframes.is_remote_path('http://example.com/data.csv') == true
	assert vframes.is_remote_path('s3://bucket/data.parquet') == true
	assert vframes.is_remote_path('gs://bucket/data.parquet') == true
	assert vframes.is_remote_path('az://container/data.csv') == true
	assert vframes.is_remote_path('/local/path/data.csv') == false
	assert vframes.is_remote_path('data.csv') == false
}

fn test_scheme_for_path() {
	// remote paths need httpfs
	assert vframes.scheme_for_path('s3://bucket/x.parquet') == 'httpfs'
	assert vframes.scheme_for_path('https://x/y.csv') == 'httpfs'
	// local xlsx needs excel
	assert vframes.scheme_for_path('report.xlsx') == 'excel'
	// plain local files need no extension
	assert vframes.scheme_for_path('data.csv') == ''
	assert vframes.scheme_for_path('data.parquet') == ''
}
