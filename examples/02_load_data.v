import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()

    // From records (no external file needed)
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!
    println('Created DataFrame with ${df.shape()[0]} rows')

    // Or from file (CSV, JSON, Parquet)
    // df := ctx.read_auto('data.csv')!

    ctx.close()
}
