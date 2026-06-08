import vframes
import os

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

fn test_read_csv_basic() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_${os.getpid()}.csv')
	os.write_file(tmp, 'id,name\n1,Alice\n2,Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp)!
	assert df.shape()![0] == 2
	cols := df.columns()!
	assert 'id' in cols
	assert 'name' in cols
}

fn test_read_csv_custom_delimiter() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_semi_${os.getpid()}.csv')
	os.write_file(tmp, 'id;name\n1;Alice\n2;Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp, delimiter: ';')!
	assert df.shape()![1] == 2 // two columns, correctly split
}

fn test_read_csv_type_override() {
	mut ctx := vframes.init()!
	defer { ctx.close() }

	tmp := os.join_path_single(os.temp_dir(), 'vframes_rc_type_${os.getpid()}.csv')
	os.write_file(tmp, 'id,name\n1,Alice\n2,Bob\n')!
	defer { os.rm(tmp) or {} }

	df := ctx.read_csv(tmp, columns: {'id': 'VARCHAR', 'name': 'VARCHAR'})!
	types := df.dtypes()!
	assert types['id'].to_upper().contains('VARCHAR')
}
