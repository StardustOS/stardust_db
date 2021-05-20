use sqlparser::ast::{Expr, ObjectName};

use crate::ast::Delete;

use super::expression::parse_expression;

pub fn parse_delete(table_name: ObjectName, selection: Option<Expr>) -> Delete {
    Delete::new(table_name.to_string(), selection.map(parse_expression))
}
