use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use stardust_db::interpreter::Interpreter;
use stardust_db::query_process::process_query;
use std::io;

fn main() {
    let mut sql = String::new();
    loop {
        sql.clear();
        io::stdin()
            .read_line(&mut sql)
            .expect("Error reading from stdin");
        if sql.trim() == "exit" {
            break;
        }
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, (&sql).as_str()).unwrap();
        let mut interpreter =
            Interpreter::new("D:\\Documents\\ComputerScience\\CS5099\\test.db").unwrap();
        for statement in statements {
            let query = process_query(statement);
            let result = interpreter.execute(query);
            match result {
                Ok(relation) => print!("{}", relation),
                Err(err) => println!("Error: {}", err),
            }
        }
    }
}
