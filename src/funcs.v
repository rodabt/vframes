module vframes

// Internal: Apply function 'func' to numeric values
fn (df DataFrame) v_apply(func string, args ...string) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			cols << if args.len > 0 {
				'${func}("${k}",${args.join(',')}) as "${k}"'
			} else {
				'${func}("${k}") as "${k}"'
			}
		} else {
			cols << k
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

@[params]
pub struct FuncOptions {
	axis   int  = 1
	skipna bool = true
}

// Internal: Apply grouping function 'func' to numeric values
fn (df DataFrame) min_max_apply(func string, fo FuncOptions) !DataFrame {
	order_by := if func == 'min' { 'desc' } else { 'asc' }
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'${func}("${k}") as "${k}"'
		} else {
			'last("${k}" order by "${k}" ${order_by}) as "${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Calculates the `func` value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
// NOTE: Only returns the numeric values
fn (df DataFrame) g_apply(func string, fo FuncOptions) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			cols << '${func}("${k}") as "${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Adds value `n` to all numeric values
pub fn (df DataFrame) add[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'${k}+${n.str()} as "${k}"'
		} else {
			k
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
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
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'"${k}"-${n.str()} as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Multiplies all numeric values by `n`
pub fn (df DataFrame) mul[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'"${k}"*${n.str()} as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Divides all numeric values by `n`
pub fn (df DataFrame) div[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'"${k}"/${n.str()} as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Divides all numeric values by `n` (floor division)
pub fn (df DataFrame) floordiv[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'floor(${k}/${n.str()}) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns modulo of all numeric values with `n`
pub fn (df DataFrame) mod[T](n T) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'"${k}" % ${n.str()} as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Rounds all numeric values to `decimals` decimal places
pub fn (df DataFrame) round(decimals int) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'round(${k},${decimals}) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
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
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << 'count(${k}) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns the number of unique values in each column
pub fn (df DataFrame) nunique() !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << 'count(distinct ${k}) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Clips (limits) values in numeric columns to a range [min_val, max_val]
pub fn (df DataFrame) clip(min_val f64, max_val f64) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'greatest(${min_val}, least(${max_val}, ${k})) as "${k}"'
		} else {
			k
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns rows where condition is true (inverse of where)
pub fn (df DataFrame) mask(condition string) !DataFrame {
	return df.query('* where not (${condition})', DFConfig{})
}

// Internal: element-wise comparison of two DataFrames using `op`, aligned by row
// position via POSITIONAL JOIN (deterministic and view-safe — no rowid needed).
fn (df DataFrame) compare(other DataFrame, op string) !DataFrame {
	cols := df.columns()!
	mut select_cols := []string{}
	for col in cols {
		select_cols << '(t1."${col}" ${op} t2."${col}") as "${col}"'
	}
	body := 'select ${select_cols.join(',')} from ${df.id} t1 positional join ${other.id} t2'
	parent := if df.depth > other.depth { df.depth } else { other.depth }
	return df.derive(body, parent)
}

// Element-wise equality comparison
pub fn (df DataFrame) eq(other DataFrame) !DataFrame {
	return df.compare(other, '=')
}

// Element-wise inequality comparison
pub fn (df DataFrame) ne(other DataFrame) !DataFrame {
	return df.compare(other, '!=')
}

// Element-wise greater than comparison
pub fn (df DataFrame) gt(other DataFrame) !DataFrame {
	return df.compare(other, '>')
}

// Element-wise greater than or equal comparison
pub fn (df DataFrame) ge(other DataFrame) !DataFrame {
	return df.compare(other, '>=')
}

// Element-wise less than comparison
pub fn (df DataFrame) lt(other DataFrame) !DataFrame {
	return df.compare(other, '<')
}

// Element-wise less than or equal comparison
pub fn (df DataFrame) le(other DataFrame) !DataFrame {
	return df.compare(other, '<=')
}

// Returns rows with largest `n` values in numeric columns
pub fn (df DataFrame) nlargest(n int) !DataFrame {
	types := df.dtypes()!
	numeric_cols := types.keys().filter(types[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if numeric_cols.len == 0 {
		return df
	}
	first_numeric := numeric_cols[0]
	return df.derive('select * from ${df.id} order by ${first_numeric} desc limit ${n}', df.depth)
}

// Returns rows with smallest `n` values in numeric columns
pub fn (df DataFrame) nsmallest(n int) !DataFrame {
	types := df.dtypes()!
	numeric_cols := types.keys().filter(types[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if numeric_cols.len == 0 {
		return df
	}
	first_numeric := numeric_cols[0]
	return df.derive('select * from ${df.id} order by ${first_numeric} asc limit ${n}', df.depth)
}

// Returns a boolean DataFrame indicating where values are null
pub fn (df DataFrame) isna() !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << '("${k}" is null) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Alias for isna
pub fn (df DataFrame) isnull() !DataFrame {
	return df.isna()
}

// Fills NA/null values with a specified value
@[params]
pub struct FillnaOptions {
pub:
	value  string = '0' // Value to fill NA with (can be a string for SQL expression)
	method string        // 'ffill' for forward fill, 'bfill' for backward fill
	limit  int           // Maximum number of consecutive NA values to fill
}

// Fills NA/null values in the DataFrame
// If `value` is provided, fills with that value
// If `method` is 'ffill', fills with the previous non-null value
// If `method` is 'bfill', fills with the next non-null value
pub fn (df DataFrame) fillna(fo FillnaOptions) !DataFrame {
	all_cols := df.columns()!
	mut body := ''
	if fo.method == 'ffill' {
		mut cols := []string{}
		for k in all_cols {
			cols << 'last_value("${k}" ignore nulls) over () as "${k}"'
		}
		body = 'select ${cols.join(',')} from ${df.id}'
	} else if fo.method == 'bfill' {
		mut cols := []string{}
		for k in all_cols {
			cols << 'first_value("${k}" ignore nulls) over (order by _rn desc) as "${k}"'
		}
		body = 'select ${cols.join(',')} from (select *, row_number() over () as _rn from ${df.id})'
	} else {
		mut cols := []string{}
		col_types := df.dtypes()!
		for k in all_cols {
			col_type := col_types[k] or { 'VARCHAR' }
			cols << 'coalesce("${k}", cast(${fo.value} as ${col_type})) as "${k}"'
		}
		body = 'select ${cols.join(',')} from ${df.id}'
	}
	return df.derive(body, df.depth)
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
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << '("${k}" is not null) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Alias for notna
pub fn (df DataFrame) notnull() !DataFrame {
	return df.notna()
}

// Replaces values in the DataFrame. `to_replace` is the value to find, `value` is the replacement
pub fn (df DataFrame) replace(to_replace string, value string) !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << 'replace("${k}", \'${to_replace}\', \'${value}\') as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Converts column types. `dtype_map` is a map of column names to target types
pub fn (df DataFrame) astype(dtype_map map[string]string) !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
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
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns a boolean DataFrame indicating whether each element is in the list of values
pub fn (df DataFrame) isin(values []string) !DataFrame {
	values_list := values.map('\'${it}\'').join(', ')
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << '("${k}" in (${values_list})) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Returns a DataFrame with counts of unique values
pub fn (df DataFrame) value_counts() !DataFrame {
	cols := df.columns()!
	if cols.len == 0 {
		return df
	}
	first_col := cols[0]
	return df.derive('select "${first_col}", count(*) as count from ${df.id} group by "${first_col}" order by count desc',
		df.depth)
}

// Aggregate functions. `agg_dict` is a map of column names to aggregation functions
// Supported functions: sum, mean, median, min, max, count, std, var
pub fn (df DataFrame) agg(agg_dict map[string]string) !DataFrame {
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
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Alias for agg
pub fn (df DataFrame) aggregate(agg_dict map[string]string) !DataFrame {
	return df.agg(agg_dict)
}

// Apply a function to each element (requires SQL expression)
pub fn (df DataFrame) apply(func_expr string) !DataFrame {
	return df.derive('select ${func_expr} from ${df.id}', df.depth)
}

// Map a function to each element (alias for apply)
pub fn (df DataFrame) map(func_expr string) !DataFrame {
	return df.apply(func_expr)
}

@[params]
pub struct RankOptions {
	method    string = 'average' // 'average', 'min', 'max', 'first', 'dense'
	ascending bool   = true
	na_option string = 'keep' // 'keep', 'top', 'bottom'
}

// Returns ranks of values
pub fn (df DataFrame) rank(ro RankOptions) !DataFrame {
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

	all_cols := df.columns()!
	for k in all_cols {
		cols << '${rank_func}() over (order by "${k}" ${order} ${na_behavior}) as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Calculate quantiles for numeric columns
pub fn (df DataFrame) quantile(q f64) !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			cols << 'quantile("${k}", ${q}) as "${k}"'
		}
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Calculate correlation matrix for numeric columns
pub fn (df DataFrame) corr() !DataFrame {
	types := df.dtypes()!
	cols := types.keys().filter(types[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if cols.len < 2 {
		return df
	}
	mut corr_cols := []string{}
	for i, col1 in cols {
		for col2 in cols[i..] {
			corr_cols << 'corr("${col1}", "${col2}") as "${col1}_${col2}"'
		}
	}
	return df.derive('select ${corr_cols.join(',')} from ${df.id}', df.depth)
}

// Calculate covariance matrix for numeric columns
pub fn (df DataFrame) cov() !DataFrame {
	types := df.dtypes()!
	cols := types.keys().filter(types[it] in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'])
	if cols.len < 2 {
		return df
	}
	mut cov_cols := []string{}
	for i, col1 in cols {
		for col2 in cols[i..] {
			cov_cols << 'covar_pop("${col1}", "${col2}") as "${col1}_${col2}"'
		}
	}
	return df.derive('select ${cov_cols.join(',')} from ${df.id}', df.depth)
}

@[params]
pub struct RollingOptions {
	window      int  = 3 // window size
	min_periods int  = 1 // minimum number of observations
	center      bool     // center the window
}

// Rolling window calculations
pub fn (df DataFrame) rolling(col string, func string, ro RollingOptions) !DataFrame {
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

	return df.derive('select ${agg_func}("${col}") over (${frame}) as "${col}_${func}" from ${df.id}',
		df.depth)
}

// Shift values by specified periods
pub fn (df DataFrame) shift(periods int) !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << 'lag("${k}", ${periods}) over () as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Calculate differences between consecutive rows
pub fn (df DataFrame) diff() !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << '"${k}" - lag("${k}", 1) over () as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Calculate percentage change between consecutive rows
pub fn (df DataFrame) pct_change() !DataFrame {
	mut cols := []string{}
	all_cols := df.columns()!
	for k in all_cols {
		cols << '("${k}" - lag("${k}", 1) over ()) / nullif(lag("${k}", 1) over (), 0) * 100 as "${k}"'
	}
	return df.derive('select ${cols.join(',')} from ${df.id}', df.depth)
}

// Cumulative sum of each numeric column
pub fn (df DataFrame) cumsum() !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'sum("${k}") over (rows between unbounded preceding and current row) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)
}

// Cumulative maximum of each numeric column
pub fn (df DataFrame) cummax() !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'max("${k}") over (rows between unbounded preceding and current row) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)
}

// Cumulative minimum of each numeric column
pub fn (df DataFrame) cummin() !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'min("${k}") over (rows between unbounded preceding and current row) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)
}

// Cumulative product of each numeric column
pub fn (df DataFrame) cumprod() !DataFrame {
	mut cols := []string{}
	types := df.dtypes()!
	for k, v in types {
		cols << if v in ['integer', 'decimal', 'float', 'bigint', 'double', 'hugeint'] {
			'exp(sum(ln(abs("${k}"))) over (rows between unbounded preceding and current row)) as "${k}"'
		} else {
			'"${k}"'
		}
	}
	return df.derive('select ${cols.join(', ')} from ${df.id}', df.depth)
}
