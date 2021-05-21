use stardust_db::{error::Result, temporary_database::TemporaryDatabase, Database};
use std::{
    io::{self, Write},
    ops::Deref,
};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let path = args.get(1).map(|s| s.as_str());
    if let Err(e) = get_results(path) {
        println!("Error opening database: {}", e)
    }
}

enum Db {
    Ordinary(Database),
    Temporary(TemporaryDatabase),
}

impl Deref for Db {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        match self {
            Db::Ordinary(db) => db,
            Db::Temporary(db) => db,
        }
    }
}

fn get_results(path: Option<&str>) -> Result<()> {
    let mut sql = String::new();
    let db = match path {
        Some(path) => Db::Ordinary(Database::open(path)?),
        None => Db::Temporary(TemporaryDatabase::new()?),
    };
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
            Err(e) => println!("{}", e),
        }
    }
    Ok(())
}
