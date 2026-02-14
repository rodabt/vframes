import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!

    // Add column with expression
    df2 := df.add_column('age_doubled', 'age * 2')

    // Delete column
    df2 := df.delete_column('name')

    // Rename columns
    df2 := df.rename({'name': 'full_name'})

    ctx.close()
}
