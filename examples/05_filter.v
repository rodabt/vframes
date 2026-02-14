import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!

    // Filter by condition
    df2 := df.filter('age', '> 25')!

    // Query with SQL-like syntax
    df2 := df.query('age > 25')!

    // Isin filter (create boolean mask)
    mask := df.isin(['Alice', 'Bob'])!

    ctx.close()
}
