module vframes

import rand

// Deletes a column from the DataFrame
pub fn (df DataFrame) delete_column(col string) !DataFrame {
	return df.derive('select * exclude(${col}) from ${df.id}', df.depth)
}

// Adds a new column to DataFrame where `expr` should be a valid expression (see examples)
pub fn (df DataFrame) add_column(col string, expr string) !DataFrame {
	return df.derive('select *, ${expr} as ${col} from ${df.id}', df.depth)
}

// Returns a subset of the DataFrame columns passed as an array
pub fn (df DataFrame) subset(cols []string) !DataFrame {
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns a subset of rows between `start` row and `end` row (both inclusive)
pub fn (df DataFrame) slice(start int, end int) !DataFrame {
	offset := start - 1
	limit := end - start + 1
	return df.derive('select * from ${df.id} limit ${limit} offset ${offset}', df.depth)
}

// Performs a group by operation where `dimensions` is an array of grouping labels, and metrics is a map of columns metrics and grouping operations (see examples)
pub fn (df DataFrame) group_by(dimensions []string, metrics map[string]string) !DataFrame {
	mut sets := []string{}
	for k, v in metrics {
		sets << '${v} as ${k}'
	}
	return df.derive('select ${dimensions.join(',')}, ${sets.join(',')} from ${df.id} group by ${dimensions.join(',')}',
		df.depth)
}

// Allows you to filter rows or select/transform columns using a SQL expression.
// - Pure condition: `df.query("sales > 40000")` → SELECT * WHERE sales > 40000
// - Select + filter: `df.query("name, age WHERE age > 25")` → SELECT name, age WHERE age > 25
// - Column expressions: `df.query("value*2 as new_value, lower(name) as lower_name")`
pub fn (df DataFrame) query(q string, dconf DFConfig) !DataFrame {
	q_upper := q.to_upper()
	body := if where_idx := q_upper.index(' WHERE ') {
		cols := q[..where_idx].trim_space()
		cond := q[where_idx + 7..]
		select_cols := if cols == '' || cols == '*' { '*' } else { cols }
		'SELECT ${select_cols} FROM ${df.id} WHERE ${cond}'
	} else if q_upper.contains('>') || q_upper.contains('<') || q_upper.contains(' = ')
		|| q_upper.contains(' IN ') || q_upper.contains(' IS ') || q_upper.contains(' LIKE ') {
		'SELECT * FROM ${df.id} WHERE ${q}'
	} else {
		'SELECT ${q} FROM ${df.id}'
	}
	return df.derive(body, df.depth) or {
		return error('Invalid query syntax: ${err.msg()}')
	}
}

// Adds prefix `prefix` to every column
pub fn (df DataFrame) add_prefix(prefix string) !DataFrame {
	return df.derive("select columns('(.*)') as '${prefix}_\\1' from ${df.id}", df.depth)
}

// Adds suffix `suffix` to every column
pub fn (df DataFrame) add_suffix(suffix string) !DataFrame {
	return df.derive("select columns('(.*)') as '\\1_${suffix}' from ${df.id}", df.depth)
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
pub fn (df DataFrame) dropna(do DropOptions) !DataFrame {
	all_cols := df.columns()!
	selected_columns := if do.subset.len > 0 { do.subset } else { all_cols }
	conn := if do.how == 'any' { 'and' } else { 'or' }
	predicate := all_cols.map('${it} is not null').join(' ${conn} ')
	return df.derive('select ${selected_columns.join(',')} from ${df.id} where ${predicate}', df.depth)
}

// Renames columns using a mapper
pub fn (df DataFrame) rename(mapper map[string]string) !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		new_name := mapper[k]
		if new_name != '' {
			cols << '"${k}" as "${new_name}"'
		} else {
			cols << k
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Rename axis (alias for rename - currently just returns same dataframe)
pub fn (df DataFrame) rename_axis(name string) !DataFrame {
	return df
}

// Removes duplicate rows
pub fn (df DataFrame) drop_duplicates(subset []string) !DataFrame {
	all_cols := df.columns()!
	cols := if subset.len > 0 { subset } else { all_cols }
	cols_str := cols.map('"${it}"').join(', ')
	return df.derive('select distinct ${cols_str} from ${df.id}', df.depth)
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
	total_rows := (df.shape()!)[0]
	sample_size := if so.n > 0 { so.n } else { int(f64(total_rows) * so.frac) }

	replacement := if so.replace { 'with replacement' } else { '' }
	return df.derive('select * from ${df.id} using sample ${sample_size}${replacement}', df.depth)
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
	how_sql := match mo.how {
		'left' { 'left join' }
		'right' { 'right join' }
		'outer', 'full' { 'full outer join' }
		'cross' { 'cross join' }
		else { 'inner join' }
	}

	suffix_left := if mo.left_on != '' { mo.left_on } else { mo.on }
	suffix_right := if mo.right_on != '' { mo.right_on } else { mo.on }

	body := 'select * from ${df.id} t1 ${how_sql} ${other.id} t2 on t1."${suffix_left}" = t2."${suffix_right}"'
	parent := if df.depth > other.depth { df.depth } else { other.depth }
	return df.derive(body, parent)
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

	table_names := dfs.map(it.id).join(', ')
	mut max_d := 0
	for d in dfs {
		if d.depth > max_d {
			max_d = d.depth
		}
	}
	return dfs[0].derive('select * from ${table_names}', max_d)
}

@[params]
pub struct PivotOptions {
pub:
	index string
	columns string
	values string
	aggfunc string = 'max'
}

// Pivot table - reshape data from long to wide format.
// NOTE: pivot materializes a real table (depth reset to 0). Auto-discovering
// PIVOT cannot be expressed as a view because its output columns are derived
// from the data, which DuckDB disallows in `CREATE VIEW`.
pub fn (df DataFrame) pivot(po PivotOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	aggfunc := if po.aggfunc != '' { po.aggfunc } else { 'max' }
	_ := db.query('CREATE TABLE ${id} AS PIVOT ${df.id} ON "${po.columns}" USING ${aggfunc}("${po.values}") AS "${po.values}" GROUP BY "${po.index}" ORDER BY "${po.index}"') or {
		return err
	}
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
	id_cols := mo.id_vars.map('"${it}"').join(', ')
	mut queries := []string{}
	for val_col in mo.value_vars {
		queries << 'select ${id_cols}, \'${val_col}\' as "${mo.var_name}", "${val_col}" as "${mo.value_name}" from ${df.id}'
	}
	return df.derive(queries.join(' union all '), df.depth)
}

// Add new columns via assignment
pub fn (df DataFrame) assign(col string, expr string) !DataFrame {
	return df.add_column(col, expr)!
}

// Pandas alias: filter rows using a SQL condition (alias for query with condition)
pub fn (df DataFrame) filter(condition string) !DataFrame {
	return df.query(condition)
}

// Pandas alias: select a subset of columns (alias for subset)
pub fn (df DataFrame) select_cols(cols []string) !DataFrame {
	return df.subset(cols)
}

// Pandas alias: drop one or more columns (alias for delete_column, supports multiple)
pub fn (df DataFrame) drop(cols []string) !DataFrame {
	exclude := cols.map('"${it}"').join(', ')
	return df.derive('SELECT * EXCLUDE (${exclude}) FROM ${df.id}', df.depth)
}

// Pandas alias: group_by (alias with same signature)
pub fn (df DataFrame) groupby(dimensions []string, metrics map[string]string) !DataFrame {
	return df.group_by(dimensions, metrics)
}

@[params]
pub struct SortOptions {
pub:
	ascending bool = true
}

// Sorts the DataFrame by one or more columns
pub fn (df DataFrame) sort_values(cols []string, so SortOptions) !DataFrame {
	direction := if so.ascending { 'ASC' } else { 'DESC' }
	order_cols := cols.map('"${it}" ${direction}').join(', ')
	return df.derive('SELECT * FROM ${df.id} ORDER BY ${order_cols}', df.depth)
}
