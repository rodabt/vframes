import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"name": json2.Any("Alice"), "age": json2.Any(30), "city": json2.null},
        {"name": json2.Any("Bob"), "age": json2.null, "city": json2.Any("NYC")}
    ]
    df := ctx.read_records(data)!

    // Drop rows with NA
    df2 := df.dropna(vframes.DropOptions{how: 'any'})

    // Fill NA with value
    df3 := df.fillna(vframes.FillnaOptions{value: "'Unknown'"})

    // Forward fill
    df4 := df.ffill()

    // Backward fill
    df5 := df.bfill()

    // Check for NA
    mask := df.isna()

    ctx.close()
}
