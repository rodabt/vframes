module vframes

import rand

// Internal: Apply function 'func' to numeric values
fn (df DataFrame) v_apply(func string, args ...string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		if v in ['integer','decimal','float','bigint','double','hugeint'] {
			cols <<  if args.len > 0 { '${func}("${k}",${args.join(',')}) as "${k}"' } else { '${func}("${k}") as "${k}"' }
		} else {
			cols << k
		}
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}")!
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
pub struct FuncOptions {
	axis		int = 1
	skipna		bool = true
}

// Internal: Apply grouping function 'func' to numeric values
fn (df DataFrame) min_max_apply(func string, fo FuncOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	order_by := if func == 'min' { 'desc' } else { 'asc' }
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { 
			'${func}("${k}") as "${k}"'
		} else { 
			'last("${k}" order by "${k}" ${order_by}) as "${k}"' 
		}
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}")!
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculates the `func` value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
// NOTE: Only returns the numeric values
fn (df DataFrame) g_apply(func string, fo FuncOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		if v in ['integer','decimal','float','bigint','double','hugeint'] { 
			cols << '${func}("${k}") as "${k}"'
		}
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Adds value `n` to all numeric values
pub fn (df DataFrame) add[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '${k}+${n.str()} as "${k}"'} else { k }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculates the absolute value for each element of the DataFrame
pub fn (df DataFrame) abs() !DataFrame {
	new_df := df.v_apply('abs') or { return err }
	return new_df
}

// Calculates the max value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) max(fo FuncOptions) !DataFrame {
	new_df := df.min_max_apply('max', fo) or { return err }
	return new_df
}

// Calculates the max value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) min(fo FuncOptions) !DataFrame {
	new_df := df.min_max_apply('min', fo) or { return err }
	return new_df
}

// Calculates the mean value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) mean(fo FuncOptions) !DataFrame {
	new_df := df.g_apply('mean', fo)!
	return new_df
}

// Calculates the median value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) median(fo FuncOptions) !DataFrame {
	new_df := df.g_apply('median', fo)!
	return new_df
}


// Calculates the sum for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) sum(fo FuncOptions) !DataFrame {
	new_df := df.g_apply('sum', fo)!
	return new_df
}

// Calculates the exponential power (`element^n`) for each element of the Dataframe
pub fn (df DataFrame) pow(n int, fo FuncOptions) !DataFrame {
	new_df := df.v_apply('pow', n.str()) or { return err }
	return new_df
}

// Subtracts value `n` from all numeric values
pub fn (df DataFrame) sub[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '\"${k}\"-${n.str()} as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Multiplies all numeric values by `n`
pub fn (df DataFrame) mul[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '\"${k}\"*${n.str()} as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Divides all numeric values by `n`
pub fn (df DataFrame) div[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '\"${k}\"/${n.str()} as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Divides all numeric values by `n` (floor division)
pub fn (df DataFrame) floordiv[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { 'floor(${k}/${n.str()}) as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns modulo of all numeric values with `n`
pub fn (df DataFrame) mod[T](n T) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '\"${k}\" % ${n.str()} as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Rounds all numeric values to `decimals` decimal places
pub fn (df DataFrame) round(decimals int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { 'round(${k},${decimals}) as "${k}"'} else { '\"${k}\"' }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns the standard deviation for each numeric column
pub fn (df DataFrame) std() !DataFrame {
	return df.g_apply('stddev', FuncOptions{})
}

// Returns the variance for each numeric column
pub fn (df DataFrame) var() !DataFrame {
	return df.g_apply('variance', FuncOptions{})
}

// Returns the number of non-null values in each column
pub fn (df DataFrame) count() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << 'count(${k}) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns the number of unique values in each column
pub fn (df DataFrame) nunique() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << 'count(distinct ${k}) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Clips (limits) values in numeric columns to a range [min_val, max_val]
pub fn (df DataFrame) clip(min_val f64, max_val f64) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { 
			'greatest(${min_val}, least(${max_val}, ${k})) as "${k}"' 
		} else { 
			k 
		}
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns rows where condition is true (inverse of where)
pub fn (df DataFrame) mask(condition string) !DataFrame {
	return df.query('* where not (${condition})', DFConfig{})
}

// Element-wise equality comparison
pub fn (df DataFrame) eq(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} = t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Element-wise inequality comparison
pub fn (df DataFrame) ne(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} != t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Element-wise greater than comparison
pub fn (df DataFrame) gt(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} > t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Element-wise greater than or equal comparison
pub fn (df DataFrame) ge(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} >= t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Element-wise less than comparison
pub fn (df DataFrame) lt(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} < t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Element-wise less than or equal comparison
pub fn (df DataFrame) le(other DataFrame) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1.${col} <= t2.${col}) as "${col}"'
	}
	_ := db.query("create table ${id} as select ${select_cols.join(',')} from ${df.id} t1 join ${other.id} t2 using (rowid)") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns rows with largest `n` values in numeric columns
pub fn (df DataFrame) nlargest(n int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	numeric_cols := df.dtypes().values().filter(it in ['integer','decimal','float','bigint','double','hugeint'])
	if numeric_cols.len == 0 {
		return df
	}
	first_numeric := numeric_cols[0]
	_ := db.query("create table ${id} as select * from ${df.id} order by ${first_numeric} desc limit ${n}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns rows with smallest `n` values in numeric columns
pub fn (df DataFrame) nsmallest(n int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	numeric_cols := df.dtypes().values().filter(it in ['integer','decimal','float','bigint','double','hugeint'])
	if numeric_cols.len == 0 {
		return df
	}
	first_numeric := numeric_cols[0]
	_ := db.query("create table ${id} as select * from ${df.id} order by ${first_numeric} asc limit ${n}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns a boolean DataFrame indicating where values are null
pub fn (df DataFrame) isna() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << '("${k}" is null) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Alias for isna
pub fn (df DataFrame) isnull() !DataFrame {
	return df.isna()
}

// Fills NA/null values with a specified value
@[params]
pub struct FillnaOptions {
pub:
	value string = '0' // Value to fill NA with (can be a string for SQL expression)
	method string // 'ffill' for forward fill, 'bfill' for backward fill
	limit int // Maximum number of consecutive NA values to fill
}

// Fills NA/null values in the DataFrame
// If `value` is provided, fills with that value
// If `method` is 'ffill', fills with the previous non-null value
// If `method` is 'bfill', fills with the next non-null value
pub fn (df DataFrame) fillna(fo FillnaOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	if fo.method == 'ffill' {
		mut cols := []string{}
		for k in df.columns() {
			cols << 'last_value("${k}" ignore nulls) over () as "${k}"'
		}
		_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	} else if fo.method == 'bfill' {
		mut cols := []string{}
		for k in df.columns() {
			cols << 'first_value("${k}" ignore nulls) over (order by rowid desc) as "${k}"'
		}
		_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	} else {
		mut cols := []string{}
		for k in df.columns() {
			cols << 'coalesce("${k}", ${fo.value}) as "${k}"'
		}
		_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	}
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Forward fill - fills NA values with the previous non-null value
pub fn (df DataFrame) ffill() !DataFrame {
	return df.fillna(method: 'ffill')
}

// Backward fill - fills NA values with the next non-null value
pub fn (df DataFrame) bfill() !DataFrame {
	return df.fillna(method: 'bfill')
}

// Alias for isnull
pub fn (df DataFrame) notna() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << '("${k}" is not null) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Alias for notna
pub fn (df DataFrame) notnull() !DataFrame {
	return df.notna()
}

// Replaces values in the DataFrame. `to_replace` is the value to find, `value` is the replacement
pub fn (df DataFrame) replace(to_replace string, value string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << 'replace("${k}", \'${to_replace}\', \'${value}\') as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Converts column types. `dtype_map` is a map of column names to target types
pub fn (df DataFrame) astype(dtype_map map[string]string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		target_type := dtype_map[k]
		if target_type != '' {
			match target_type {
				'string' { cols << 'cast("${k}" as VARCHAR) as "${k}"' }
				'int', 'integer' { cols << 'cast("${k}" as INTEGER) as "${k}"' }
				'float' { cols << 'cast("${k}" as DOUBLE) as "${k}"' }
				'bool', 'boolean' { cols << 'cast("${k}" as BOOLEAN) as "${k}"' }
				'decimal' { cols << 'cast("${k}" as DECIMAL) as "${k}"' }
				else { cols << k }
			}
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

// Returns a boolean DataFrame indicating whether each element is in the list of values
pub fn (df DataFrame) isin(values []string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	values_list := values.map('\'${it}\'').join(', ')
	mut cols := []string{}
	for k in df.columns() {
		cols << '("${k}" in (${values_list})) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Returns a DataFrame with counts of unique values
pub fn (df DataFrame) value_counts() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns()
	if cols.len == 0 {
		return df
	}
	first_col := cols[0]
	_ := db.query("create table ${id} as select \"${first_col}\", count(*) as count from ${df.id} group by \"${first_col}\" order by count desc") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Aggregate functions. `agg_dict` is a map of column names to aggregation functions
// Supported functions: sum, mean, median, min, max, count, std, var
pub fn (df DataFrame) agg(agg_dict map[string]string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for col, func_name in agg_dict {
		match func_name {
			'sum' { cols << 'sum("${col}") as "${col}_sum"' }
			'mean', 'avg' { cols << 'mean("${col}") as "${col}_mean"' }
			'median' { cols << 'median("${col}") as "${col}_median"' }
			'min' { cols << 'min("${col}") as "${col}_min"' }
			'max' { cols << 'max("${col}") as "${col}_max"' }
			'count' { cols << 'count("${col}") as "${col}_count"' }
			'std', 'stddev' { cols << 'stddev("${col}") as "${col}_std"' }
			'var', 'variance' { cols << 'variance("${col}") as "${col}_var"' }
			else { cols << '"${col}"' }
		}
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Alias for agg
pub fn (df DataFrame) aggregate(agg_dict map[string]string) !DataFrame {
	return df.agg(agg_dict)
}

// Apply a function to each element (requires SQL expression)
pub fn (df DataFrame) apply(func_expr string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${func_expr} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Map a function to each element (alias for apply)
pub fn (df DataFrame) map(func_expr string) !DataFrame {
	return df.apply(func_expr)
}

@[params]
pub struct RankOptions {
	method string = 'average' // 'average', 'min', 'max', 'first', 'dense'
	ascending bool = true
	na_option string = 'keep' // 'keep', 'top', 'bottom'
}

// Returns ranks of values
pub fn (df DataFrame) rank(ro RankOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	
	rank_func := match ro.method {
		'min' { 'min' }
		'max' { 'max' }
		'first' { 'row_number' }
		'dense' { 'dense_rank' }
		else { 'rank' }
	}
	
	order := if ro.ascending { 'asc' } else { 'desc' }
	na_behavior := match ro.na_option {
		'top' { 'nulls first' }
		'bottom' { 'nulls last' }
		else { 'nulls last' }
	}
	
	for k in df.columns() {
		cols << '${rank_func}() over (order by "${k}" ${order} ${na_behavior}) as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculate quantiles for numeric columns
pub fn (df DataFrame) quantile(q f64) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k, v in df.dtypes() {
		if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			cols << 'quantile("${k}", ${q}) as "${k}"'
		}
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculate correlation matrix for numeric columns
pub fn (df DataFrame) corr() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns().filter(df.dtypes()[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if cols.len < 2 {
		return df
	}
	mut corr_cols := []string{}
	for i, col1 in cols {
		for col2 in cols[i..] {
			corr_cols << 'corr("${col1}", "${col2}") as "${col1}_${col2}"'
		}
	}
	_ := db.query("create table ${id} as select ${corr_cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculate covariance matrix for numeric columns
pub fn (df DataFrame) cov() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	cols := df.columns().filter(df.dtypes()[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if cols.len < 2 {
		return df
	}
	mut cov_cols := []string{}
	for i, col1 in cols {
		for col2 in cols[i..] {
			cov_cols << 'covar_pop("${col1}", "${col2}") as "${col1}_${col2}"'
		}
	}
	_ := db.query("create table ${id} as select ${cov_cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
pub struct RollingOptions {
	window int = 3 // window size
	min_periods int = 1 // minimum number of observations
	center bool // center the window
}

// Rolling window calculations
pub fn (df DataFrame) rolling(col string, func string, ro RollingOptions) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	
	frame := if ro.center {
		'rows between ${-ro.window / 2} preceding and ${ro.window / 2} following'
	} else {
		'rows between ${ro.window - 1} preceding and current row'
	}
	
	agg_func := match func {
		'sum' { 'sum' }
		'mean', 'avg' { 'avg' }
		'min' { 'min' }
		'max' { 'max' }
		'count' { 'count' }
		'std' { 'stddev' }
		else { 'sum' }
	}
	
	_ := db.query("create table ${id} as select ${agg_func}(\"${col}\") over (${frame}) as \"${col}_${func}\" from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Shift values by specified periods
pub fn (df DataFrame) shift(periods int) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << 'lag("${k}", ${periods}) over () as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculate differences between consecutive rows
pub fn (df DataFrame) diff() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << '"${k}" - lag("${k}", 1) over () as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculate percentage change between consecutive rows
pub fn (df DataFrame) pct_change() !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k in df.columns() {
		cols << '("${k}" - lag("${k}", 1) over ()) / nullif(lag("${k}", 1) over (), 0) * 100 as "${k}"'
	}
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { return err }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

