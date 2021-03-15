use stardust_db::{error::Result, Database};
use std::io::{self, Write};

fn main() {
    match get_results() {
        Err(e) => println!("Error opening database: {}", e),
        _ => (),
    }
}

fn get_results() -> Result<()> {
    let mut sql = String::new();
    let mut db = Database::open("D:\\Documents\\ComputerScience\\CS5099\\test.db")?;
    loop {
        sql.clear();
        print!(">");
        io::stdout().flush().expect("Error flushing stdout");
        io::stdin()
            .read_line(&mut sql)
            .expect("Error reading from stdin");
        if sql.trim() == "exit" {
            break;
        }
        match db.execute_query(sql.as_str()) {
            Ok(relations) => {
                for result in relations {
                    print!("{}", result)
                }
            }
            Err(e) => println!("Error: {}", e),
        }
    }
    Ok(())
}
