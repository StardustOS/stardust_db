use crate::{
    ast::*,
    data_types::{Type, TypeContents},
    error::{Error, ExecutionError, Result},
    storage::ColumnName,
};
use itertools::Itertools;
use sqlparser::ast::{
    self, Assignment, BinaryOperator, ColumnDef, ColumnOption, DataType, Expr, FileFormat,
    HiveDistributionStyle, HiveFormat, Ident, Join, ObjectName, ObjectType, Query,
    ReferentialAction, Select, SelectItem, SetExpr, SqlOption, SqliteOnConflict, Statement,
    TableConstraint, TableFactor, TableWithJoins, Value,
};

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

#[allow(clippy::too_many_arguments)]
fn parse_create_table(
    _or_replace: bool,
    _temporary: bool,
    _external: bool,
    _if_not_exists: bool,
    name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    _hive_distribution: HiveDistributionStyle,
    _hive_formats: Option<HiveFormat>,
    _table_properties: Vec<SqlOption>,
    _with_options: Vec<SqlOption>,
    _file_format: Option<FileFormat>,
    _location: Option<String>,
    _query: Option<Box<Query>>,
    _without_rowid: bool,
    _like: Option<ObjectName>,
) -> Result<CreateTable> {
    let table_name = name.to_string();
    let mut uniques = Vec::new();
    let mut primary_key = None;
    let mut checks = Vec::new();
    let mut check_name_counter: u16 = 0;
    let mut foreign_keys = Vec::new();
    let table_columns = columns
        .into_iter()
        .enumerate()
        .map(|(index, c)| {
            let mut default = None;
            let mut not_null = false;
            let ColumnDef {
                name,
                data_type,
                collation: _collation,
                options,
            } = c;
            let column_name = name.to_string();
            for column_option in options {
                let name = column_option
                    .name
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| name.to_string());
                match column_option.option {
                    ColumnOption::Default(expr) => default = Some(parse_expression(expr)),
                    ColumnOption::NotNull => not_null = true,
                    ColumnOption::Unique { is_primary } => {
                        if is_primary {
                            if primary_key.is_some() {
                                return Err(Error::Execution(ExecutionError::MultiplePrimaryKey(
                                    table_name.clone(),
                                )));
                            } else {
                                primary_key = Some((vec![index], name));
                            }
                        } else {
                            uniques.push((vec![index], name));
                        }
                    }
                    ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                    } => {
                        let referred_columns = match referred_columns.as_slice() {
                            [c] => vec![c.to_string()],
                            _ => {
                                return Err(ExecutionError::IncorrectNumForeignKeyReferredColumns {
                                    expected: 1,
                                    found: referred_columns.len(),
                                }
                                .into())
                            }
                        };
                        foreign_keys.push(ForeignKey::new(
                            name,
                            vec![column_name.clone()],
                            foreign_table.to_string(),
                            referred_columns,
                            on_delete.map(parse_foreign_key_action),
                            on_update.map(parse_foreign_key_action),
                        ))
                    }
                    ColumnOption::Check(e) => {
                        let expression = parse_expression(e);
                        checks.push((expression, name))
                    }
                    _ => unimplemented!("{:?}", column_option.option),
                }
            }
            Ok(Column::new(
                column_name,
                convert_data_type(data_type),
                default,
                not_null,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => {
                let name = name
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| columns.iter().map(|c| c.to_string()).join(", "));
                let unique_set = columns
                    .iter()
                    .map(|column| {
                        let column_name = column.to_string();
                        table_columns
                            .iter()
                            .position(|c| c.name == column_name)
                            .ok_or(Error::Execution(ExecutionError::NoColumn(column_name)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if is_primary {
                    if primary_key.is_some() {
                        return Err(Error::Execution(ExecutionError::MultiplePrimaryKey(
                            table_name,
                        )));
                    } else {
                        primary_key = Some((unique_set, name));
                    }
                } else {
                    uniques.push((unique_set, name));
                }
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => {
                let name = name.map_or_else(
                    || format!("__fkey{}", foreign_keys.len()),
                    |n| n.to_string(),
                );
                let columns = columns
                    .iter()
                    .map(|column| column.to_string())
                    .collect::<Vec<_>>();
                let referred_columns = match referred_columns.as_slice() {
                    [c] => vec![c.to_string()],
                    _ => {
                        return Err(ExecutionError::IncorrectNumForeignKeyReferredColumns {
                            expected: 1,
                            found: referred_columns.len(),
                        }
                        .into())
                    }
                };
                if columns.len() != referred_columns.len() {
                    return Err(ExecutionError::IncorrectNumForeignKeyReferredColumns {
                        expected: columns.len(),
                        found: referred_columns.len(),
                    }
                    .into());
                }
                foreign_keys.push(ForeignKey::new(
                    name,
                    columns,
                    foreign_table.to_string(),
                    referred_columns,
                    on_delete.map(parse_foreign_key_action),
                    on_update.map(parse_foreign_key_action),
                ))
            }
            TableConstraint::Check { name, expr } => {
                let check = parse_expression(*expr);
                let name = name.map_or_else(
                    || {
                        let n = check_name_counter;
                        check_name_counter += 1;
                        format!("__check{}", n)
                    },
                    |n| n.to_string(),
                );
                checks.push((check, name))
            }
        }
    }
    Ok(CreateTable::new(
        table_name,
        table_columns,
        uniques,
        primary_key,
        checks,
        foreign_keys,
    ))
}

#[allow(clippy::too_many_arguments)]
fn parse_insert(
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

fn parse_select_query(query: Query) -> Result<SelectQuery> {
    Ok(match query.body {
        SetExpr::Values(v) => SelectQuery::Values(Values::new(
            v.0.into_iter()
                .map(|row| row.into_iter().map(parse_expression).collect())
                .collect(),
        )),
        SetExpr::Select(s) => SelectQuery::Select(parse_select(*s)?),
        _ => unimplemented!("{:?}", query.body),
    })
}

fn parse_expression(expression: Expr) -> UnresolvedExpression {
    match expression {
        Expr::Value(v) => UnresolvedExpression::Value(parse_value(v)),
        Expr::Identifier(i) => {
            UnresolvedExpression::Identifier(ColumnName::new(None, i.to_string()))
        }
        Expr::CompoundIdentifier(i) => {
            UnresolvedExpression::Identifier(parse_compound_identifier(i))
        }
        Expr::BinaryOp { left, op, right } => {
            let left = Box::new(parse_expression(*left));
            let right = Box::new(parse_expression(*right));
            match op {
                BinaryOperator::And => UnresolvedExpression::BinaryOp(left, BinaryOp::And, right),
                BinaryOperator::Or => UnresolvedExpression::BinaryOp(left, BinaryOp::Or, right),
                BinaryOperator::Eq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Eq),
                    right,
                ),
                BinaryOperator::NotEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::NotEq),
                    right,
                ),
                BinaryOperator::Lt => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Lt),
                    right,
                ),
                BinaryOperator::Gt => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Gt),
                    right,
                ),
                BinaryOperator::LtEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::LtEq),
                    right,
                ),
                BinaryOperator::GtEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::GtEq),
                    right,
                ),
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

fn parse_select(select: Select) -> Result<SelectContents> {
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
    let from = parse_table_joins(select.from.into_iter())?;
    let selection = select.selection.map(parse_expression);
    Ok(SelectContents::new(projections, from, selection))
}

fn parse_table_joins<I>(mut joins: I) -> Result<Option<TableJoins>>
where
    I: Iterator<Item = TableWithJoins>,
{
    joins
        .next()
        .map(|left| parse_table_joins_recursive(parse_with_join(left)?, joins))
        .transpose()
}

fn parse_table_joins_recursive<I>(left: TableJoins, mut joins: I) -> Result<TableJoins>
where
    I: Iterator<Item = TableWithJoins>,
{
    match joins.next() {
        Some(right) => {
            let right = parse_with_join(right)?;
            for name in right.table_names() {
                if left.contains_table(name) {
                    return Err(ExecutionError::DuplicateTableName(name.to_owned()).into());
                }
            }
            let new_left = TableJoins::Join {
                left: Box::new(left),
                right: Box::new(right),
                operator: JoinOperator::Inner,
                constraint: JoinConstraint::None,
            };
            parse_table_joins_recursive(new_left, joins)
        }
        None => Ok(left),
    }
}

fn parse_with_join(join: TableWithJoins) -> Result<TableJoins> {
    let left = parse_table_factor(join.relation);
    parse_joins(left?, join.joins.into_iter())
}

fn parse_table_factor(factor: TableFactor) -> Result<TableJoins> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let name = name.to_string();
            let alias = alias.map(|alias| alias.name.to_string());
            Ok(TableJoins::Table(TableName::new(name, alias)))
        }
        TableFactor::NestedJoin(join) => parse_with_join(*join),
        _ => unimplemented!("{:?}", factor),
    }
}

fn parse_joins(left: TableJoins, mut joins: impl Iterator<Item = Join>) -> Result<TableJoins> {
    match joins.next() {
        None => Ok(left),
        Some(join) => {
            let Join {
                relation,
                join_operator,
            } = join;
            let right = parse_table_factor(relation)?;
            for name in right.table_names() {
                if left.contains_table(name) {
                    return Err(ExecutionError::DuplicateTableName(name.to_owned()).into());
                }
            }
            let (operator, constraint) = match join_operator {
                ast::JoinOperator::Inner(constraint) => {
                    (JoinOperator::Inner, parse_join_constraint(constraint))
                }
                ast::JoinOperator::LeftOuter(constraint) => {
                    (JoinOperator::Left, parse_join_constraint(constraint))
                }
                ast::JoinOperator::RightOuter(constraint) => {
                    (JoinOperator::Right, parse_join_constraint(constraint))
                }
                ast::JoinOperator::CrossJoin => (JoinOperator::Inner, JoinConstraint::None),
                _ => unimplemented!("{:?}", join_operator),
            };
            let left = TableJoins::Join {
                left: Box::new(left),
                right: Box::new(right),
                operator,
                constraint,
            };
            parse_joins(left, joins)
        }
    }
}

fn parse_join_constraint(constraint: ast::JoinConstraint) -> JoinConstraint {
    match constraint {
        ast::JoinConstraint::On(e) => JoinConstraint::On(parse_expression(e)),
        ast::JoinConstraint::None => JoinConstraint::None,
        ast::JoinConstraint::Natural => JoinConstraint::Natural,
        ast::JoinConstraint::Using(cols) => {
            JoinConstraint::Using(cols.into_iter().map(|i| i.to_string()).collect())
        }
    }
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

fn parse_foreign_key_action(action: ReferentialAction) -> ForeignKeyAction {
    match action {
        ReferentialAction::Restrict | ReferentialAction::NoAction => ForeignKeyAction::NoAction,
        ReferentialAction::Cascade => ForeignKeyAction::Cascade,
        ReferentialAction::SetNull => ForeignKeyAction::SetNull,
        ReferentialAction::SetDefault => ForeignKeyAction::SetDefault,
    }
}

fn parse_update(name: ObjectName, assignments: Vec<Assignment>, selection: Option<Expr>) -> Update {
    let table_name = name.to_string();
    let assignments = assignments
        .into_iter()
        .map(|a| (a.id.to_string(), parse_expression(a.value)))
        .collect();
    let filter = selection.map(parse_expression);
    Update::new(table_name, assignments, filter)
}
