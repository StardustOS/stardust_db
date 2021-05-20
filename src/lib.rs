use std::path::Path;

use ast::ColumnName;
pub use c_interface::*;
use data_types::Value;
use error::{ExecutionError, Result};
use interpreter::Interpreter;
use query_process::process_query;
use relation::Relation;
use resolved_expression::ResolvedColumn;
use sqlparser::{dialect::Dialect, parser::Parser};

mod ast;
mod data_types;
pub mod error;
mod interpreter;
mod join_handler;
mod query_process;
mod resolved_expression;
mod storage;
mod table_definition;
mod table_handler;
pub mod temporary_database;
#[macro_use]
mod utils;
mod c_interface;
mod foreign_key;
pub mod relation;
#[cfg(test)]
pub mod tests;

#[derive(Debug, Default)]
struct StardustDbDialect;

impl Dialect for StardustDbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '#'
            || ch == '@'
            || ch == '?'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ('0'..='9').contains(&ch)
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }
}

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
        let dialect = StardustDbDialect {};
        let statements = Parser::parse_sql(&dialect, &sql)?;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let processed_query = process_query(statement)?;
            results.push(self.interpreter.execute(processed_query)?)
        }
        Ok(results)
    }
}

pub(crate) trait GetData {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value>;
}

pub(crate) struct Empty;

impl GetData for Empty {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        Err(ExecutionError::NoColumn(column_name.to_string()).into())
    }
}

pub(crate) trait TableColumns {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn>;
}

impl TableColumns for Empty {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        Err(ExecutionError::NoColumn(name.to_string()).into())
    }
}
