use sled::open;
use stardust_db::error::Result;

fn main() {
    //std::io::stdout().flush().unwrap();
    // _ = std::io::stdout();
    if let Err(e) = get_results() {
        println!("Error: {}", e)
    }
}

/*fn get_results() -> Result<()> {
    let queries = [
        "CREATE TABLE people (name String, age Int)",
        "INSERT INTO people VALUES ('Josh', 23), ('Jim', 45)",
        "SELECT * FROM people",
        "DROP TABLE people"
    ];
    let mut db = Database::open("test.db")?;
    for query in queries.iter() {
        for result in db.execute_query(query)? {
            print!("{}\n{}", query, result)
        }
    }
    Ok(())
}
*/

fn get_results() -> Result<()> {
    let _ = std::io::stdout();
    //let _ = File::create("test.txt").unwrap();
    let db = open("test.db")?;
    println!("Db opened");
    drop(db);
    println!("Dropped");
    //Err(Error::Execution(ExecutionError::NoTable("people".into())))
    Ok(())
}
