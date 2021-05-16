use sqlparser::ast::{Assignment, Expr, ObjectName};

use crate::ast::Update;

use super::expression::parse_expression;

pub fn parse_update(
    name: ObjectName,
    assignments: Vec<Assignment>,
    selection: Option<Expr>,
) -> Update {
    let table_name = name.to_string();
    let assignments = assignments
        .into_iter()
        .map(|a| (a.id.to_string(), parse_expression(a.value)))
        .collect();
    let filter = selection.map(parse_expression);
    Update::new(table_name, assignments, filter)
}
