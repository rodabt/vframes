module vframes

import rand
import x.json2
import vduckdb

type Data = []map[string]json2.Any | []map[string]string

@[params]
pub struct ContextConfig {
pub:
	location			string = ":memory:"
	view_depth_warning	int = 50
}

@[params]
pub struct DFConfig {
pub mut:
	to_stdout			bool = true
}

@[noinit]
struct DataFrameContext {
	dpath				string
mut:
	db					vduckdb.DuckDB
	loaded_extensions	map[string]bool
	view_depth_warning	int = 50
}

@[noinit]
pub struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
	ctx					DataFrameContext
	depth				int
pub mut:
	display_mode		string = 'box'
	display_max_rows	int = 100
}




