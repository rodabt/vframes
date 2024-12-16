# vframes 0.1.1

A DataFrame library inspired by Python's Pandas. Should work on Linux, Windows and Mac (still testing). Uses the powerful DuckDB database as a backend.

This is still a WIP. More functions, documentation, tutorials, and examples will be added soon.

## Dependencies

[VDuckDB wrapper library](https://github.com/rodabt/vduckdb)

## Installation

```bash
v install https://github.com/rodabt/vduckdb
v install https://github.com/rodabt/vframes
```

## Basic usage example

Make sure the files people-500000.csv, titanic.parquet, and data.json are in the same directory as your .v file (check the examples dir)

```v
import vframes

// A convenience function for better printing
fn printlne(s string) {
    println('\n${s}\n')
}

fn main() {

    printlne("VFrames version: ${vframes.version()}")

    printlne("First initialize a new context. If no arguments are give, memory is used")
    mut ctx := vframes.init()
    
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
    df7.head(10)

    printlne("Error control: try to load a non valid file")
    _ := ctx.read_auto('no_valid.csv') or { 
        eprintln(err.msg())
        vframes.empty()
    }

    ctx.close()
}
```

## Considerations

- VFrames uses DuckDB under the hood through the VDuckDB wrapper library, so in theory all operations allowed by DuckDB should be supported by VFrames eventually.
- Currently by design DataFrames are inmutable, so when mutating you should create a new DataFrame to store each new result

## Initial settings

### DataFrameContext

To use the library you must first initialize a DataFrame to define which kind of storage you will use for DataFrames, like this:

```v
mut ctx := vframes.init()                               // In memory
mut ctx := vframes.init(location: 'mycontext.db')       // Persisted to 'mycontext.db'
```

### DataFrame

If you want to suppress console output for functions returning `Data`, set the optional parameter `to_stdout` to `false` (see examples dir).

## Accepted data file formats

The structure of most CSV, Parquet, and JSON files is infered automatically. In the future, there will be options to fine tune loading parameters such as delimiters, column renames, partial loading, etc.

## Current functions

Last updated on 2024-12-15

- [X] columns
- [X] dtypes
- [X] empty
- [X] groupby
- [X] head
- [X] info
- [X] query
- [X] shape
- [X] tail
- [X] values
- [X] abs
- [X] add
- [X] add_prefix
- [X] add_suffix
- [X] max
- [X] min
- [X] mean
- [X] median
- [X] sum

## Roadmap

Although the library is very inspired by Pandas, **it's purpose is NOT to be a one-on-one replacement**. Some of the funcionalities planned are listed below:

- [ ] DataFrame joins
- [ ] Query deferral
- [ ] Basic plotting

## How to contribute

Comments, bug reports, requests, and pull requests are welcome.
