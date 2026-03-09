use fsqlite_func::{FunctionRegistry, datetime::register_datetime_builtins};
use fsqlite_types::value::SqliteValue;

#[test]
fn test_strftime_null() {
    let mut reg = FunctionRegistry::new();
    register_datetime_builtins(&mut reg);
    let args = vec![
        SqliteValue::Text("%Y-%m".to_owned()),
        SqliteValue::Text("2024-01-15".to_owned()),
    ];
    let func = reg.find_scalar("strftime", args.len() as i32).unwrap();
    let res = func.invoke(&args).unwrap();
    println!("strftime: {:?}", res);
}
