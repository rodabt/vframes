module vframes

import os
import x.json2


/***
	CONTEXT CREATION
**/


// Creates a new DataFrame context
pub fn set_context(config Config) DataframeContext {
	d := DataframeContext{
		dpath: config.dpath
	}
	return d
}


// Inititalizes references to DataFrames in context 
pub fn (mut d DataframeContext) init() map[string]DataFrame {
	return *( unsafe { &d.df } )
}


/***
	CONTEXT I/O
**/


// Load data from `filepath` to DataFrame `name` inside the context
pub fn (mut d DataframeContext) load_from_file(name string, filepath string, load_opt LoadOptions) {	
	load(d.dpath, name, filepath, load_opt)
	d.df[name] = DataFrame{
		db_url: d.dpath,
		name: name
	}	
	d.df[name].columns = d.df[name].get_columns() 
}


// Load data from records to DataFrame `name` inside the context
pub fn (mut d DataframeContext) load_from_records(name string, dict []map[string]json2.Any) {
	tmp_dict := dict.map(it.str())
	os.write_file('tmp.json', tmp_dict.join_lines()) or { panic(err) }
	cmd := "create or replace table ${name} as select * from 'tmp.json'"
	_ := os.execute('${bin} -s ${q(cmd)} ${d.dpath}')
	d.df[name] = DataFrame{
		db_url: d.dpath,
		name: name
	}	
	d.df[name].columns = d.df[name].get_columns()
	os.rm('tmp.json') or { panic(err) }
}

/***
	CONTEXT MISC
**/

pub fn (d DataframeContext) delete() {
}