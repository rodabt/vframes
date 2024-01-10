module vframes

import x.json2
import rand
import os

@[params]
pub struct DataFrameOption {
pub mut:
	n 				int = 10
	delimiter		string = '|'
}

/***
	DATAFRAME INFO
**/

// Return column names and data types
pub fn (df DataFrame) get_columns() map[string]string {
	// TODO: Return specific data types
	cmd := "select column_name, data_type from information_schema.columns where table_name='${df.name}'"
	data_arr := execute_json(df.db_url, cmd)
	mut cols := map[string]string{}
	for item in data_arr { 
		column_name := (item['column_name'] or { json2.Any('') }).str()
		data_type := (item['data_type'] or { json2.Any('') }).str()
		cols[column_name] = data_type
	}
	return cols
}

pub fn (df DataFrame) columns() []string {
	return df.columns.keys()
}

pub fn (df DataFrame) dtypes() map[string]string {
	return df.columns
}

pub fn (df DataFrame) head(o DataFrameOption) string {
	cmd := 'select * from ${df.name} limit ${o.n}'
	return execute_box(df.db_url, cmd)
}

pub fn (df DataFrame) describe() string {
	cmd := 'summarize ${df.name}'
	return execute_box(df.db_url, cmd)
} 

pub fn (df DataFrame) info() string {
	cmd := 'pragma database_size'
 	result := execute_raw(df.db_url, cmd).split('|')
	return result[1]
}

pub fn (df DataFrame) shape() (int,int) {
	cmd := 'select count(*) as n from ${df.name}'
	num_rows := execute_raw(df.db_url, cmd).int()
 	num_cols := df.columns.len
	return num_rows, num_cols
}

/***
	DATAFRAME I/O
**/

pub fn load_from_file(name string, filepath string, load_opt LoadOptions) !DataFrame {
	mut df := DataFrame{name: name}
	result := load(df.db_url, name, filepath, load_opt)
	if result.exit_code == 1 {
		panic(result.output)
	}
	df.columns = df.get_columns()
	return df
}

pub fn tmp_from_file(filepath string, load_opt LoadOptions) !DataFrame {
	return load_from_file('tmp_${rand.ulid()}', filepath, load_opt)
}

pub fn load_from_records(name string, dict []map[string]json2.Any) DataFrame {
	// TODO: change temp file name
	mut df := DataFrame{name: name}
	tmp_dict := dict.map(it.str())
	os.write_file('tmp.json', tmp_dict.join_lines()) or { panic(err) }
	load(df.db_url, name, 'tmp.json',filetype: 'json')
	df.columns = df.get_columns()
	return df
}

/* pub fn (df DataFrame) insert_from_file(filepath string) {
	// Check if table exists...
	full_path := os.abs_path(filepath)
	cmd := "copy ${df.name} from '${full_path}'"
	execute_raw_nr(df.db_url, cmd)
}*/

pub fn (df DataFrame) to_records() []map[string]json2.Any {
	cmd := 'select * from ${df.name}'
	return execute_json(df.db_url, cmd) 
}

pub fn (df DataFrame) to_csv(filename string, delimiter string) {
	cmd := "copy ${df.name} to '${filename}' (HEADER, DELIMITER '${delimiter}')"
	execute_raw_nr(df.db_url, cmd)
}


/***
	DATAFRAME MUTATIONS
**/


pub fn (mut df DataFrame) add_column(column_name string, position int, expression string) {
	temp_table := 'temp_${rand.ulid()}'
	mut cols := df.columns()
	new_col := '${expression} as ${column_name}'
	cols.insert(position, new_col)
	new_cols := cols.join(",")
	cmd := 'create table ${temp_table} as select ${new_cols} from ${df.name}'
	execute_raw_nr(df.db_url, cmd)
	execute_raw_nr(df.db_url, 'drop table ${df.name}; alter table ${temp_table} rename to ${df.name}')
}


pub fn (mut df DataFrame) delete_column(column_name string) {
	temp_table := 'temp_${rand.ulid()}'
	cols := df.columns().filter(it != column_name)
	new_cols := cols.join(",")
	cmd := 'create table ${temp_table} as select ${new_cols} from ${df.name}'
	execute_raw_nr(df.db_url, cmd)
	execute_raw_nr(df.db_url, 'drop table ${df.name}; alter table ${temp_table} rename to ${df.name}')
}