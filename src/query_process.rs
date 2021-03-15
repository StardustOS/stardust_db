use crate::{
    ast::*,
    data_types::{Type, TypeContents},
    storage::ColumnName,
};
use sqlparser::ast::{
    BinaryOperator, ColumnDef, ColumnOption, DataType, Expr, FileFormat, HiveDistributionStyle,
    HiveFormat, Ident, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr, SqlOption,
    SqliteOnConflict, Statement, TableConstraint, TableFactor, Value,
};

pub fn process_query(statement: Statement) -> SqlQuery {
    match statement {
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
        )),
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
        )),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            purge,
        } => SqlQuery::DropTable(parse_drop(object_type, if_exists, names, cascade, purge)),
        Statement::Query(q) => SqlQuery::SelectQuery(parse_select_query(*q)),
        Statement::Delete {
            table_name,
            selection,
        } => SqlQuery::Delete(parse_delete(table_name, selection)),
        _ => unimplemented!(),
    }
}

fn parse_create_table(
    _or_replace: bool,
    _temporary: bool,
    _external: bool,
    _if_not_exists: bool,
    name: ObjectName,
    columns: Vec<ColumnDef>,
    _constraints: Vec<TableConstraint>,
    _hive_distribution: HiveDistributionStyle,
    _hive_formats: Option<HiveFormat>,
    _table_properties: Vec<SqlOption>,
    _with_options: Vec<SqlOption>,
    _file_format: Option<FileFormat>,
    _location: Option<String>,
    _query: Option<Box<Query>>,
    _without_rowid: bool,
    _like: Option<ObjectName>,
) -> CreateTable {
    let name = name.to_string();
    let columns = columns
        .into_iter()
        .map(|c| {
            let mut default = None;
            let mut not_null = false;
            let mut unique = false;
            for column_option in c.options {
                match column_option.option {
                    ColumnOption::Default(expr) => default = Some(parse_expression(expr)),
                    ColumnOption::NotNull => not_null = true,
                    ColumnOption::Unique { .. } => unique = true,
                    _ => unimplemented!("{:?}", column_option.option),
                }
            }
            Column::new(
                c.name.to_string(),
                convert_data_type(c.data_type),
                default,
                not_null,
                unique,
            )
        })
        .collect();
    CreateTable::new(name, columns)
}

fn parse_insert(
    _or: Option<SqliteOnConflict>,
    table_name: ObjectName,
    columns: Vec<Ident>,
    _overwrite: bool,
    source: Box<Query>,
    _partitioned: Option<Vec<Expr>>,
    _after_columns: Vec<Ident>,
    _table: bool,
) -> Insert {
    let columns = if columns.is_empty() {
        None
    } else {
        Some(columns.into_iter().map(|c| c.to_string()).collect())
    };
    Insert::new(table_name.to_string(), columns, parse_select_query(*source))
}

fn parse_select_query(query: Query) -> SelectQuery {
    //println!("{:?}", query);
    match query.body {
        SetExpr::Values(v) => SelectQuery::Values(Values::new(
            v.0.into_iter()
                .map(|row| row.into_iter().map(|col| parse_expression(col)).collect())
                .collect(),
        )),
        SetExpr::Select(s) => SelectQuery::Select(parse_select(*s)),
        _ => unimplemented!("{:?}", query.body),
    }
}

fn parse_expression(expression: Expr) -> Expression {
    match expression {
        Expr::Value(v) => Expression::Value(parse_value(v)),
        //Expr::Wildcard => Expression::Wildcard,
        Expr::Identifier(i) => Expression::Identifier(ColumnName::new(None, i.to_string())),
        Expr::CompoundIdentifier(i) => Expression::Identifier(parse_compound_identifier(i)),
        Expr::BinaryOp { left, op, right } => {
            let left = Box::new(parse_expression(*left));
            let right = Box::new(parse_expression(*right));
            match op {
                BinaryOperator::And => Expression::BinaryOp(left, BinaryOp::And, right),
                BinaryOperator::Or => Expression::BinaryOp(left, BinaryOp::Or, right),
                BinaryOperator::Eq => {
                    Expression::BinaryOp(left, BinaryOp::Comparison(ComparisonOp::Eq), right)
                }
                BinaryOperator::Lt => {
                    Expression::BinaryOp(left, BinaryOp::Comparison(ComparisonOp::Lt), right)
                }
                BinaryOperator::Gt => {
                    Expression::BinaryOp(left, BinaryOp::Comparison(ComparisonOp::Gt), right)
                }
                BinaryOperator::LtEq => {
                    Expression::BinaryOp(left, BinaryOp::Comparison(ComparisonOp::LtEq), right)
                }
                BinaryOperator::GtEq => {
                    Expression::BinaryOp(left, BinaryOp::Comparison(ComparisonOp::GtEq), right)
                }
                _ => unimplemented!("{:?}", op),
            }
        }
        _ => unimplemented!("{:?}", expression),
    }
}

fn parse_compound_identifier(identifier: Vec<Ident>) -> ColumnName {
    match identifier.as_slice() {
        [.., table, column] => ColumnName::new(Some(table.to_string()), column.to_string()),
        [column] => ColumnName::new(None, column.to_string()),
        _ => unimplemented!("{:?}", identifier),
    }
}

fn parse_value(value: Value) -> crate::data_types::Value {
    match value {
        Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
            crate::data_types::Value::TypedValue(TypeContents::String(s))
        }
        Value::Number(s, _) => crate::data_types::Value::TypedValue(TypeContents::Integer(
            s.parse().expect("Number string was not a number"),
        )),
        Value::Null => crate::data_types::Value::Null,
        Value::Boolean(b) => {
            crate::data_types::Value::TypedValue(TypeContents::Integer(if b { 1 } else { 0 }))
        }
        _ => unimplemented!("{:?}", value),
    }
}

fn parse_select(select: Select) -> SelectContents {
    let projections = select
        .projection
        .into_iter()
        .map(|p| match p {
            SelectItem::UnnamedExpr(e) => Projection::Unaliased(parse_expression(e)),
            SelectItem::ExprWithAlias { expr, alias } => {
                Projection::Aliased(parse_expression(expr), alias.to_string())
            }
            SelectItem::Wildcard => Projection::Wildcard,
            SelectItem::QualifiedWildcard(name) => Projection::QualifiedWildcard(name.to_string()),
        })
        .collect();
    /*let from = TableJoins::new(select.from.iter().map(|from| match from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => unimplemented!("{:?}", select.from[0].relation),
    }).collect());*/
    let from = TableJoins::new(match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => unimplemented!("{:?}", select.from[0].relation),
    });
    let selection = select.selection.map(parse_expression);
    SelectContents::new(projections, from, selection)
}

fn parse_drop(
    object_type: ObjectType,
    _if_exists: bool,
    names: Vec<ObjectName>,
    _cascade: bool,
    _purge: bool,
) -> DropTable {
    match object_type {
        ObjectType::Table => {
            let names = names.into_iter().map(|name| name.to_string()).collect();
            DropTable::new(names)
        }
        _ => unimplemented!("{:?}", object_type),
    }
}

fn parse_delete(table_name: ObjectName, selection: Option<Expr>) -> Delete {
    Delete {
        table_name: table_name.to_string(),
        predicate: selection.map(parse_expression),
    }
}

fn convert_data_type(t: DataType) -> Type {
    match t {
        DataType::String => Type::String,
        DataType::Int => Type::Integer,
        _ => unimplemented!("{:?}", t),
    }
}
