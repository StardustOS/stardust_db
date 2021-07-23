use stardust_db::{error::Result, temporary_database::TemporaryDatabase};

fn main() {
    match execute() {
        Err(e) => println!("Error: {}", e),
        _ => ()
    }
}

fn execute() -> Result<()> {
    let db = TemporaryDatabase::new()?;
    db.execute_query("CREATE TABLE test (name string, age int);
                                        INSERT INTO test VALUES ('Josh', 24), ('Jim', 17)")?;
    let mut result = db.execute_query("SELECT * FROM test")?;
    let result = result.pop().unwrap();
    for row in result.iter() {
        let name = row.get_value_index(0).unwrap();
        let age = row.get_value_named("age").unwrap();
        println!("name: {}, age: {}", name, age)
    }
    Ok(())
}
