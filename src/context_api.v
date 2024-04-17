module vframes

import os
import x.json2

// TODO: Catch errors on file missing, query error, and locked database at least

/***
	CONTEXT CREATION AND INITIALIZATION
**/


// Creates a new DataFrame context
//
// A context is a container for multiple DataFrames.
// It must be initialized before use, and optionally it 
// accepts a `Config` struct parameter where you can
// provide a specific location for it. 
//
// Example: 
// ```v
// import vframes

// mut d := vframes.set_context(dpath: '/tmp/container.db')
// ```
pub fn set_context(config Config) DataframeContext {
	d := DataframeContext{
		dpath: config.dpath
	}
	_ := new(d.dpath)
	return d
}

// Creates the container and returns an empty map of DataFrames
pub fn (mut d DataframeContext) init() map[string]DataFrame {
	return *( unsafe { &d.df } )
}


/***
	CONTEXT I/O
**/


// Loads data from `filepath` to DataFrame `name` inside the context
//
// Example:
// ```v
// // https://github.com/datablist/sample-csv-files/raw/main/files/people/people-1000.csv 
// d.load_from_file('df1', 'people-1000.csv')
// d
// vframes.DataframeContext{
//     dpath: '/tmp/container.db'
//     df: {'df1': vframes.DataFrame{
//         name: 'df1'
//         columns: {'Index': 'BIGINT', 'User Id': 'VARCHAR', 'First Name': 'VARCHAR', 'Last Name': 'VARCHAR', 'Sex': 'VARCHAR', 'Email': 'VARCHAR', 'Phone': 'VARCHAR', 'Date of birth': 'DATE', 'Job Title': 'VARCHAR'}
//     }}
// }
// ```
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