module vframes

import rand

// Deletes a column from the DataFrame
pub fn (df DataFrame) delete_column(col string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * exclude(${col}) from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Adds a new column to DataFrame where `expr` should be a valid expression (see examples) 
pub fn (df DataFrame) add_column(col string, expr string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select *, ${expr} as ${col} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns a subset of the DataFrame columns passed as an array
pub fn (df DataFrame) subset(cols []string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns a subset of rows between `start` row and `end` row (both inclusive) 
pub fn (df DataFrame) slice(start int, end int) DataFrame {
	id := 'tbl_${rand.ulid()}'
	offset := start - 1
	limit := end - start + 1
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * from ${df.id} limit ${limit} offset ${offset}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Performs a group by operation where `dimensions` is an array of grouping labels, and metrics is a map of columns metrics and grouping operations (see examples)
pub fn (df DataFrame) group_by(dimensions []string, metrics map[string]string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut sets := []string{}
	for k,v in metrics {
		sets << '${v} as ${k}'
	}
	_ := db.query("create table ${id} as select ${dimensions.join(',')}, ${sets.join(',')} from ${df.id} group by ${dimensions.join(',')}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}


// Allows you to use a valid sql expression with the DataFrame. It returns a DataFrame Result
// Examples: `df.query("value*2 as new_value, lower(name) as lowercase_name")`
pub fn (df DataFrame) query(q string, dconf DFConfig) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query('SELECT ${q} FROM ${df.id}') or { 
		eprintln("Invalid query syntax: ${err.msg()}")
		return error("Invalid query syntax: ${err.msg()}")	
	}
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Adds prefix `prefix` to every column
pub fn (df DataFrame) add_prefix(prefix string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select columns('(.*)') as '${prefix}_\\1' from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Adds suffix `suffix` to every column
pub fn (df DataFrame) add_suffix(suffix string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select columns('(.*)') as '\\1_${suffix}' from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
struct DropOptions {
	axis		int  			// 0: drop rows, 1: drop columns	
	how			string = 'any'	// 'any': drop if any NA values, 'all': drop if all NA values
	thresh		int				// Minimum number of non-NA values to keep
	subset		[]string 		// Subset of columns to consider
	nullstr	    string = 'null'
}

// Drops NA rows or columns from DataFrame. If how is 'any', it drops the row/column if any NA values are present. 
// If how is 'all', it drops the row/column if all NA values are present
// If subset is passed, it only considers the columns passed in the subset as final columns for output
pub fn (df DataFrame) dropna(do DropOptions) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	selected_columns := if do.subset.len > 0 { do.subset } else { df.columns() }
	conn := if do.how == 'any' { 'and' } else { 'or' }
	predicate := df.columns().map("${it} is not null").join(' ${conn} ')
	_ := db.query("create table ${id} as select ${selected_columns.join(',')} from ${df.id} where ${predicate}") or { panic(err) }	
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}