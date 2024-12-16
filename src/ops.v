module vframes

import rand

// Internal: Apply function 'func' to numeric values
fn (df DataFrame) v_apply(func string) !DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '${func}("${k}") as "${k}"'} else { k }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}")!
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

@[params]
struct FuncOptions {
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
fn (df DataFrame) g_apply(func string, fo FuncOptions) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		if v in ['integer','decimal','float','bigint','double','hugeint'] { 
			cols << '${func}("${k}") as "${k}"'
		}
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Adds value `n` to all numeric values
pub fn (df DataFrame) add[T](n T) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	mut cols := []string{}
	for k,v in df.dtypes() {
		cols << if v in ['integer','decimal','float','bigint','double','hugeint'] { '${k}+${n.str()} as "${k}"'} else { k }
	} 
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

// Calculates the absolute value for each element of the DataFrame
pub fn (df DataFrame) abs() DataFrame {
	new_df := df.v_apply('abs') or { panic(err) }
	return new_df
}

// Calculates the max value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) max(fo FuncOptions) DataFrame {
	new_df := df.min_max_apply('max', fo) or { panic(err) }
	return new_df
}

// Calculates the max value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) min(fo FuncOptions) DataFrame {
	new_df := df.min_max_apply('min', fo) or { panic(err) }
	return new_df
}

// Calculates the mean value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) mean(fo FuncOptions) DataFrame {
	new_df := df.g_apply('mean', fo)
	return new_df
}

// Calculates the median value for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) median(fo FuncOptions) DataFrame {
	new_df := df.g_apply('median', fo)
	return new_df
}


// Calculates the sum for each of the rows (`axis: 0`) or columns (`axis: 1`. default) of the DataFrame
pub fn (df DataFrame) sum(fo FuncOptions) DataFrame {
	new_df := df.g_apply('sum', fo)
	return new_df
}

