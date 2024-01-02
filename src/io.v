module vframes

import os

/***
	LOADERS
**/

// Load data from `filepath` to DataFrame `name` inside the context
fn load(dpath string, name string, filepath string, load_opt LoadOptions) {	
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
	/* if load_opt.names != []string {
		opts << "names={${load_opt.names.join(',')}}"
	}*/
	if load_opt.filetype == 'json' {
		cmd = "create or replace table ${name} as select * from '${full_path}'"
	} else {
		str_opts := opts.join(',')
		cmd = if str_opts == '' 
			{"create or replace table ${name} as select * from read_csv_auto('${full_path}')"}
		else 
			{"create or replace table ${name} as select * from read_csv_auto('${full_path}',${str_opts}))"}
	}
	
	_ := execute(dpath, cmd, [])
}


/***
	MISC
**/


pub fn (df DataFrame) close() {
	os.rm(df.db_url) or {}
}