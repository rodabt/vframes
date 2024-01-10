# vframes

A DataFrame interface that relies on duckdb. Should work on Linux, Windows and Mac

## Basic usage example

```v
import vframes
import x.json2

fn main() {

    // Load default configuration options
	opt := vframes.LoadOptions{}

    // Load from file (autoguess format)
    mut df := vframes.load_from_file('example', 'people-1000.csv', opt)!
    
    println(df.columns())
    println(df.head(n: 5))
    println(df.describe())
    println(df.info())
    println(df.shape())

    // Load from json array
    mut data := []map[string]json2.Any{}
    data << {'x': json2.Any(10), 'y': 5, 'z': 'test'}
    data << {'x': json2.Any(3), 'y': 6, 'z': 'test2'}
    df = vframes.load_from_records('df2', data)
    println(df.head(n: 5))

    df.add_column('w', 0, 'x * y + 10')
    println(df.head(n: 5))
    println(df.to_records())  // []map[string]json2.Any

    defer {
        df.close()
    }
}
```
