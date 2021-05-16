use sqlparser::ast::{Expr, Ident, ObjectName, Query, SqliteOnConflict};

use crate::{ast::Insert, error::Result};

use super::select::parse_select_query;

#[allow(clippy::too_many_arguments)]
pub fn parse_insert(
    _or: Option<SqliteOnConflict>,
    table_name: ObjectName,
    columns: Vec<Ident>,
    _overwrite: bool,
    source: Box<Query>,
    _partitioned: Option<Vec<Expr>>,
    _after_columns: Vec<Ident>,
    _table: bool,
) -> Result<Insert> {
    let columns = if columns.is_empty() {
        None
    } else {
        Some(columns.into_iter().map(|c| c.to_string()).collect())
    };
    Ok(Insert::new(
        table_name.to_string(),
        columns,
        parse_select_query(*source)?,
    ))
}
