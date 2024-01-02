module vframes

import rand
import os

// Generates default
fn cmd_default(name string, filepath string) string {
	return "create or replace table ${name} as select * from '${os.abs_path(filepath)}'"
}

fn gen_key() string {
	return rand.uuid_v4()
}

fn q(s string) string {
	return '\"${s}\"'
}

pub fn version() string {
	result := execute_raw(':memory:','select version()')
	return result
}