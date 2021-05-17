mod create_table;
mod delete;
mod drop;
mod expression;
mod insert;
mod select;
mod update;

use crate::{
    ast::*,
    error::Result,
    query_process::{
        create_table::parse_create_table, delete::parse_delete, drop::parse_drop,
        insert::parse_insert, select::parse_select_query, update::parse_update,
    },
};
use sqlparser::ast::Statement;

use self::expression::parse_expression;

pub fn process_query(statement: Statement) -> Result<SqlQuery> {
    Ok(match statement {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
            ..
        } => SqlQuery::CreateTable(parse_create_table(
            name,
            columns,
            constraints,
            if_not_exists,
        )?),
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => SqlQuery::Insert(parse_insert(table_name, columns, source)?),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => SqlQuery::DropTable(parse_drop(object_type, if_exists, names)),
        Statement::Query(q) => SqlQuery::SelectQuery(parse_select_query(*q)?),
        Statement::Delete {
            table_name,
            selection,
        } => SqlQuery::Delete(parse_delete(table_name, selection)),
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => SqlQuery::Update(parse_update(table_name, assignments, selection)),
        _ => unimplemented!("{:?}", statement),
    })
}
