import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)},
        {"name": json2.Any("Charlie"), "age": json2.Any(35)}
    ]
    df := ctx.read_records(data)!

    // Sort by values (ascending)
    df2 := df.sort_values(['age'], vframes.SortValuesOptions{ascending: true})

    // Sort by values (descending)
    df3 := df.sort_values(['age'], vframes.SortValuesOptions{ascending: false})

    // Sort by index
    df4 := df.sort_index(true)

    ctx.close()
}
