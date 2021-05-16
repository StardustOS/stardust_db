use sqlparser::ast::{Expr, ObjectName};

use crate::ast::Delete;

use super::expression::parse_expression;

pub fn parse_delete(table_name: ObjectName, selection: Option<Expr>) -> Delete {
    Delete {
        table_name: table_name.to_string(),
        predicate: selection.map(parse_expression),
    }
}
