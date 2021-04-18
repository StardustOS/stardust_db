use std::{
    ffi::CStr,
    os::raw::{c_char, c_int},
    path::Path,
};

use data_types::Value;
use error::{ExecutionError, Result};
use interpreter::{Interpreter, Relation};
use query_process::process_query;
use resolved_expression::ResolvedColumn;
use sqlparser::{dialect::GenericDialect, parser::Parser};
use storage::ColumnName;

pub mod ast;
pub mod data_types;
pub mod error;
pub mod interpreter;
pub mod join_handler;
pub mod query_process;
pub mod resolved_expression;
pub mod storage;
pub mod table_definition;
pub mod table_handler;
pub mod temporary_database;
#[macro_use]
mod utils;

mod foreign_key;
#[cfg(test)]
pub mod tests;

pub struct Database {
    interpreter: Interpreter,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            interpreter: Interpreter::new(path)?,
        })
    }

    pub fn execute_query(&self, sql: &str) -> Result<Vec<Relation>> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, &sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let processed_query = process_query(statement)?;
            results.push(self.interpreter.execute(processed_query)?)
        }
        Ok(results)
    }

    pub fn was_recovered(&self) -> bool {
        self.interpreter.was_recovered()
    }
}

pub const STARDUST_DB_OK: c_int = 0;
pub const STARDUST_DB_INVALID_PATH_UTF_8: c_int = 1;
pub const STARDUST_DB_INVALID_PATH_LOCATION: c_int = 2;

#[repr(C)]
pub struct Db {
    database: *mut Database,
}

#[no_mangle]
pub unsafe extern "C" fn open_database(path: *const c_char, db: *mut Db) -> c_int {
    let path = CStr::from_ptr(path);
    let path = match path.to_str() {
        Ok(path) => path,
        Err(_) => return STARDUST_DB_INVALID_PATH_UTF_8,
    };
    let mut database = match Database::open(path) {
        Ok(db) => db,
        Err(_) => return STARDUST_DB_INVALID_PATH_LOCATION,
    };
    *db = Db {
        database: &mut database,
    };
    STARDUST_DB_OK
}

pub trait GetData {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value>;
}

pub struct Empty;

impl GetData for Empty {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        Err(ExecutionError::NoColumn(column_name.to_string()).into())
    }
}

pub trait TableColumns {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn>;
}

impl TableColumns for Empty {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        Err(ExecutionError::NoColumn(name.to_string()).into())
    }
}
