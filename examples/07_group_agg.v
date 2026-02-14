import vframes
import x.json2

fn main() {
    mut ctx := vframes.init()
    
    data := [
        {"dept": json2.Any("Sales"), "salary": json2.Any(50000), "age": json2.Any(30)},
        {"dept": json2.Any("Sales"), "salary": json2.Any(60000), "age": json2.Any(35)},
        {"dept": json2.Any("IT"), "salary": json2.Any(70000), "age": json2.Any(28)}
    ]
    df := ctx.read_records(data)!

    // Group by and aggregate
    df2 := df.group_by(['dept'], {
        'avg_salary': 'avg(salary)',
        'count': 'count(*)'
    })

    // Multiple aggregations
    df3 := df.agg({
        'salary': 'mean',
        'age': 'max'
    })

    ctx.close()
}
