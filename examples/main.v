import vframes

fn printlne(s string) {
	println('\n${s}\n')
}

fn main() {
	
	printlne("VFrames version: ${vframes.version()}")
	
	printlne("Initialize context in memory")
	mut ctx := vframes.init() // location: 'ctx.db'

	printlne("Load 500.000 records from a CSV")
	df := ctx.read_auto('people-500000.csv')!

	printlne("Print first 5 records:")
	df.head(5)

	printlne("Assign first 10 records to variable x as []map[string]json2.Any")
	data := df.head(10, to_stdout: false)
	println(data)
	
	printlne("Print last 5 records:")
	df.tail(5)
	
	printlne("DataFrame info:")
	df.info()
	
	printlne("DataFrame shape: ${df.shape()}")
	
	printlne("Describe DataFrame:")
	df.describe()
	
	printlne("Create new DF with new column 'new_col'=Index*5, and select a subset of columns (Email, Phone, new_col):")
	df2 := df
		.add_column('new_col', 'Index*5')
		.subset(['Email','Phone','new_col'])	
	df2.head(10)

	printlne("Delete Email from new DF:")
	df3 := df2.delete_column('Email')
	df3.head(10)

	printlne("Load parquet (Titanic):")
	df4 := ctx.read_auto('titanic.parquet')!
	df4.head(10)
	
	printlne("Describe:")
	df4.describe()

	printlne("Average of Age and Fare by Sex and Embarked:")
	df5 := df4.group_by(['Sex','Embarked'],{"age_avg": "avg(Age)", "avg_fare": "avg(Fare)"})
	df5.head(10)

	printlne("Slice(2,3) of first DataFrame:")
	df6 := df.slice(2,3)
	df6.head(10)

    println("Reading a JSON file:")
    df7 := ctx.read_auto("data.json")!
    df7.head(100)	

	printlne("Error control: try to load a non valid file")
	_ := ctx.read_auto('no_valid.csv') or { 
		eprintln(err.msg())
		vframes.empty()
	}

	ctx.close()
}

