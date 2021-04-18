use std::fmt::Display;

use crate::{ast::BinaryOp, data_types::Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResolvedColumn {
    table_name: String,
    column_name: String,
}

impl ResolvedColumn {
    pub fn new(table_name: String, column_name: String) -> Self {
        Self {
            table_name,
            column_name,
        }
    }

    pub fn destructure(self) -> (String, String) {
        (self.table_name, self.column_name)
    }

    pub fn take_column_name(self) -> String {
        self.column_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl Display for ResolvedColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.column_name.fmt(f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Value(Value),
    Identifier(ResolvedColumn),
    BinaryOp(Box<Expression>, BinaryOp, Box<Expression>),
}
