import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30)},
        {"name": json2.Any("Bob"), "age": json2.Any(25)}
    ]
    df := ctx.read_records(data)!

    df.head(5)
    df.tail(5)
    println('Shape: ${df.shape()}')
    println('Columns: ${df.columns()}')
    println('Dtypes: ${df.dtypes()}')
    df.describe()
    df.info()

    ctx.close()
}
