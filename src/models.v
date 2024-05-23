module vframes

import rand
import x.json2
import rodabt.vduckdb

type Data = []map[string]json2.Any

@[params]
pub struct ContextConfig {
pub:
	location			string = ":memory:"	
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
}

@[noinit]
pub struct DataFrame {
	id					string = 'tbl_${rand.ulid()}'
	ctx					DataFrameContext
pub mut:
	display_mode		string = 'box'
	display_max_rows	int = 100
}




