import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!

    // To CSV
    df.to_csv('output.csv', vframes.ToCsvOptions{})!

    // To JSON
    df.to_json('output.json')!

    // To Parquet
    df.to_parquet('output.parquet')!

    ctx.close()
}
