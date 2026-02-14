import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!

    // Select specific columns
    df2 := df.select(['name', 'age'])

    // Or use subset
    df2 := df.subset(['name', 'age'])

    // Add prefix/suffix
    df2 := df.add_prefix('col_')
    df2 := df.add_suffix('_col')

    ctx.close()
}
