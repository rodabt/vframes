module vframes

import os
import rand

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
	mut args := '*'
	mut new_sequence := ''

	if load_opt.row_num {
		new_sequence = 'seq_${rand.ulid()}'
		args = "nextval('${new_sequence}') as __row_num,*"
	}

	if load_opt.delimiter != ',' {
		opts << "delim='${load_opt.delimiter}'"
	} 
	if load_opt.names != []string{} {
		names := load_opt.names.str()
		opts << "names=$names"
	}
	if load_opt.ignore_errors {
		opts << "ignore_errors=true"
	}
	if load_opt.normalize_names {
		opts << "normalize_names=true"
	}
	if load_opt.skip > 0 {
		opts << "skip=${load_opt.skip}"
	}
	if load_opt.new_line != '' {
		opts << "new_line='${load_opt.new_line}'"
	}
	if load_opt.all_varchar {
		opts << "all_varchar=true"
	}
	if load_opt.skip_empty_columns {
		args = "nextval('${new_sequence}') as __row_num,columns(col -> col NOT LIKE '_col_%')"
	}
	if load_opt.filetype == 'json' {
		cmd = "create or replace table ${name} as select ${args} from '${full_path}'"
	} else {
		str_opts := opts.join(',')
		cmd = if str_opts == '' 
			{"create or replace table ${name} as select ${args} from read_csv('${full_path}', auto_detect=true)"}
		else 
			{"create or replace table ${name} as select ${args} from read_csv('${full_path}',${str_opts})"}
	}
	if new_sequence != '' {
		_ := execute(dpath, "create sequence ${new_sequence}", [])
	}
	if load_opt.debug {
		dump(cmd)
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