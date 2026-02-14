import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data1 := [
        {"id": json2.Any(1), "name": json2.Any("Alice")},
        {"id": json2.Any(2), "name": json2.Any("Bob")}
    ]
    df1 := ctx.read_records(data1)!

    data2 := [
        {"id": json2.Any(1), "age": json2.Any(30)},
        {"id": json2.Any(2), "age": json2.Any(25)}
    ]
    df2 := ctx.read_records(data2)!

    // Merge (SQL join)
    df3 := df1.merge(df2, vframes.MergeOptions{on: 'id'})

    // Join
    df4 := df1.join(df2, vframes.JoinOptions{on: 'id'})

    // Concatenate
    data3 := [
        {"id": json2.Any(3), "name": json2.Any("Charlie")}
    ]
    df3 := ctx.read_records(data3)!
    df5 := df1.concat([df3], vframes.ConcatOptions{})

    ctx.close()
}
