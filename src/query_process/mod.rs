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
            or_replace,
            temporary,
            external,
            if_not_exists,
            name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
        } => SqlQuery::CreateTable(parse_create_table(
            or_replace,
            temporary,
            external,
            if_not_exists,
            name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
        )?),
        Statement::Insert {
            or,
            table_name,
            columns,
            overwrite,
            source,
            partitioned,
            after_columns,
            table,
        } => SqlQuery::Insert(parse_insert(
            or,
            table_name,
            columns,
            overwrite,
            source,
            partitioned,
            after_columns,
            table,
        )?),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            purge,
        } => SqlQuery::DropTable(parse_drop(object_type, if_exists, names, cascade, purge)),
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
        _ => unimplemented!(),
    })
}
