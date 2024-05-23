module vframes

import os
import rand

@[params]
pub struct Config {
pub mut:
	dpath				string =  os.join_path(os.temp_dir(), 'tmp_${rand.ulid()}.db')
}

@[params]
pub struct LoadOptions {
pub mut:
	filetype			string = 'auto'
	// CSV options
	delimiter			string = ','
	columns				map[string]string
	header				bool = true
	decimal_separator   string = '.'
	escape				string
	ignore_errors		bool
	normalize_names		bool
	names				[]string
	new_line			string
	nullstr				string
	parallel			bool = true
	quote				string
	sample_size			int = 20480
	skip				int
	timestampformat		string
	types				[]string
	dtypes				map[string]string
	union_by_name		bool
	all_varchar			bool
	// vframes only
	row_num				bool
	debug				bool
	skip_empty_columns	bool
	// Parquet options
	extra_filename		bool
	// Record options
}


pub struct DataframeContext {
pub mut:
	dpath				string
	df					map[string]DataFrame
} 

pub struct DataFrame {
pub mut:
	db_url				string = os.join_path(os.temp_dir(), 'df_${rand.ulid()}.db') @[str: skip]
	name				string
	columns				map[string]string
}