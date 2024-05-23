module vframes

import os
import x.json2
import io.util

/***
	EXECUTION WRAPPERS
**/

// Main execution cmd. Return os.Result
pub fn execute(filepath string, cmd string, args []string) os.Result {
	str_args := args.join(' ')
	result := os.execute('${bin} ${str_args} -s ${q(cmd)} ${filepath}')
	return result
}

// Executes `cmd` and returns raw result as string with no headers and a `|` as a separator
pub fn execute_raw(filepath string, cmd string) string {
	result := execute(filepath, cmd, ['-list','-separator "|"','-noheader'])
	return result.output
}

// Executes `cmd` without any return
pub fn execute_raw_nr(filepath string, cmd string) {
	_ := execute(filepath, cmd, [])
}

// Executes `cmd` and returns result as boxed. Useful to print to screen 
pub fn execute_box(filepath string, cmd string) string {
	result := execute(filepath, cmd, ['-box'])
	return result.output
}

// Executes commands as a block and returns the result as a list with no header
pub fn execute_block(filepath string, cmd string) string {
	// TODO: Refactor
	mut f, temp_path := util.temp_file(pattern: 'temp_') or { panic(err) }
	f.writeln(cmd) or { panic(err) }
	f.close()
	res := os.execute('${bin} -list -noheader -separator "," ${filepath} < ${temp_path}')
	os.rm(temp_path) or { panic(err) }
	return res.output
}

// Executes `cmd` and returns result as an array of json
pub fn execute_json(filepath string, cmd string) []map[string]json2.Any {
	mut out := []map[string]json2.Any{}
	res := os.execute('${bin} -json -s ${q(cmd)} ${filepath}')
	if res.exit_code == 1 {
		dump(cmd)
		dump(res)
	} else {
		if res.output.len > 0 {
			data_raw := json2.raw_decode(res.output) or { json2.Any('') }
			rows := data_raw.arr()
			for row in rows {
				out << row as map[string]json2.Any
			}
		}
	}	
	return out
}