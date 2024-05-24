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
