module vframes

import os

/***
	LOADERS
**/

// creates a container in dpath if it doesn't exist
pub fn new(dpath string) os.Result {	
	full_path := os.abs_path(dpath)
	result := os.execute('${bin} -s "select 1" ${full_path}')
	return result
}

// Load data from `filepath` to DataFrame `name` inside the context
fn load(dpath string, name string, filepath string, load_opt LoadOptions) os.Result {	
	full_path := os.abs_path(filepath)
	mut opts := []string{}
	mut cmd := ''
	
	if load_opt.delimiter != ',' {
		opts << "delim='${load_opt.delimiter}'"
	} 
	if load_opt.names != []string{} {
		names := load_opt.names.str()
		opts << "names=$names"
	}
	if load_opt.filetype == 'json' {
		cmd = "create or replace table ${name} as select * from '${full_path}'"
	} else {
		str_opts := opts.join(',')
		cmd = if str_opts == '' 
			{"create or replace table ${name} as select * from read_csv('${full_path}', auto_detect=true)"}
		else 
			{"create or replace table ${name} as select * from read_csv('${full_path}',${str_opts}))"}
	}
	
	result := execute(dpath, cmd, [])
	return result
}


/***
	MISC
**/


pub fn (df DataFrame) close() {
	os.rm(df.db_url) or {}
}