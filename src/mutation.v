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
pub struct DropOptions {
pub:
	axis int // 0: drop rows, 1: drop columns
	how string = 'any' // 'any': drop if any NA values, 'all': drop if all NA values
	thresh int // Minimum number of non-NA values to keep
	subset []string // Subset of columns to consider
	nullstr string = 'null'
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

// Renames columns using a mapper
pub fn (df DataFrame) rename(mapper map[string]string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		new_name := mapper[k]
		if new_name != '' {
			cols << '"${k}" as "${new_name}"'
		} else {
			cols << k
		}
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Rename axis (alias for rename - currently just returns same dataframe)
pub fn (df DataFrame) rename_axis(name string) !DataFrame {
	return df
}

// Removes duplicate rows
pub fn (df DataFrame) drop_duplicates(subset []string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := if subset.len > 0 { subset } else { df.columns() }
	cols_str := cols.map('"${it}"').join(', ')
	_ := db.query("create table ${id} as select distinct ${cols_str} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
pub struct SampleOptions {
pub:
	n int
	frac f64
	replace bool
}

// Returns a random sample of rows
pub fn (df DataFrame) sample(so SampleOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	total_rows := df.shape()[0]
	sample_size := if so.n > 0 { so.n } else { int(f64(total_rows) * so.frac) }
	
	replacement := if so.replace { 'with replacement' } else { '' }
	_ := db.query("create table ${id} as select * from ${df.id} using sample ${sample_size}${replacement}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
pub struct MergeOptions {
pub:
	on string
	how string = 'inner'
	left_on string
	right_on string
}

// Merge two DataFrames (SQL JOIN)
pub fn (df DataFrame) merge(other DataFrame, mo MergeOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	how_sql := match mo.how {
		'left' { 'left join' }
		'right' { 'right join' }
		'outer', 'full' { 'full outer join' }
		'cross' { 'cross join' }
		else { 'inner join' }
	}
	
	suffix_left := if mo.left_on != '' { mo.left_on } else { mo.on }
	suffix_right := if mo.right_on != '' { mo.right_on } else { mo.on }
	
	query := "create table ${id} as select * from ${df.id} t1 ${how_sql} ${other.id} t2 on t1.\"${suffix_left}\" = t2.\"${suffix_right}\""
	_ := db.query(query) or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Join two DataFrames (alias for merge)
pub fn (df DataFrame) join(other DataFrame, mo MergeOptions) !DataFrame {
	return df.merge(other, mo)
}

// Concatenate DataFrames (stack vertically)
pub fn concat(dfs []DataFrame) !DataFrame {
	if dfs.len == 0 {
		return empty()
	}
	if dfs.len == 1 {
		return dfs[0]
	}
	
	id := 'tbl_${rand.ulid()}'
	mut db := &dfs[0].ctx.db
	
	table_names := dfs.map(it.id).join(', ')
	_ := db.query("create table ${id} as select * from ${table_names}") or { return err }
	return DataFrame{
		id: id
		ctx: dfs[0].ctx
	}
}

@[params]
pub struct PivotOptions {
pub:
	index string
	columns string
	values string
	aggfunc string = 'max'
}

// Pivot table - reshape data from long to wide format
pub fn (df DataFrame) pivot(po PivotOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	_ := db.query("create table ${id} as pivot ${df.id} on \"${po.columns}\" using max(\"${po.values}\") as \"${po.values}\" group by \"${po.index}\" order by \"${po.index}\"") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Advanced pivot with aggregation
pub fn (df DataFrame) pivot_table(po PivotOptions) !DataFrame {
	return df.pivot(po)
}

@[params]
pub struct MeltOptions {
pub:
	id_vars []string
	value_vars []string
	var_name string = 'variable'
	value_name string = 'value'
}

// Unpivot DataFrame from wide to long format
pub fn (df DataFrame) melt(mo MeltOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	id_cols := mo.id_vars.map('"${it}"').join(', ')
	mut queries := []string{}
	for val_col in mo.value_vars {
		queries << 'select ${id_cols}, \'${val_col}\' as "${mo.var_name}", "${val_col}" as "${mo.value_name}" from ${df.id}'
	}
	query_str := queries.join(' union all ')
	_ := db.query("create table ${id} as ${query_str}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Add new columns via assignment
pub fn (df DataFrame) assign(col string, expr string) !DataFrame {
	return df.add_column(col, expr)
}