module vframes

import rand

pub fn (df DataFrame) delete_column(col string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select * exclude(${col}) from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

pub fn (df DataFrame) add_column(col string, exp string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select *, ${exp} as ${col} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

pub fn (df DataFrame) subset(cols []string) DataFrame {
	id := 'tbl_${rand.ulid()}'
	mut db := &df.ctx.db
	_ := db.query("create table ${id} as select ${cols.join(',')} from ${df.id}") or { panic(err) }
	return DataFrame{
		id: id
		ctx: df.ctx
	}
}

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
