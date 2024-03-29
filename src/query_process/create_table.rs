use itertools::Itertools;
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, Ident, ObjectName, ReferentialAction, TableConstraint,
};

use crate::{
    ast::{Column, CreateTable, ForeignKey, ForeignKeyAction},
    data_types::Type,
    error::{Error, ExecutionError, Result},
    query_process::parse_expression,
};

pub fn parse_create_table(
    name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
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
            let mut unique = false;
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
                    ColumnOption::Default(expr) => {
                        if default.is_some() {
                            return Err(ExecutionError::MultipleDefault(column_name).into());
                        }
                        default = Some(parse_expression(expr))
                    }
                    ColumnOption::NotNull => {
                        if not_null {
                            return Err(ExecutionError::MultipleNotNull(column_name).into());
                        }
                        not_null = true
                    }
                    ColumnOption::Unique { is_primary } => {
                        if unique {
                            return Err(ExecutionError::MultipleUnique(column_name).into());
                        }
                        unique = true;
                        if is_primary {
                            if primary_key.is_some() {
                                return Err(
                                    ExecutionError::MultiplePrimaryKey(table_name.clone()).into()
                                );
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
                let mut unique_set = columns
                    .iter()
                    .map(|column| {
                        let column_name = column.to_string();
                        table_columns
                            .iter()
                            .position(|c| c.name == column_name)
                            .ok_or(Error::Execution(ExecutionError::NoColumn(column_name)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                unique_set.sort_unstable();
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
                if columns.len() != referred_columns.len() {
                    return Err(ExecutionError::IncorrectNumForeignKeyReferredColumns {
                        expected: columns.len(),
                        found: referred_columns.len(),
                    }
                    .into());
                }
                let columns = columns.iter().map(Ident::to_string).collect::<Vec<_>>();
                let referred_columns = referred_columns
                    .iter()
                    .map(Ident::to_string)
                    .collect::<Vec<_>>();
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
        if_not_exists,
    ))
}

fn parse_foreign_key_action(action: ReferentialAction) -> ForeignKeyAction {
    match action {
        ReferentialAction::Restrict | ReferentialAction::NoAction => ForeignKeyAction::NoAction,
        ReferentialAction::Cascade => ForeignKeyAction::Cascade,
        ReferentialAction::SetNull => ForeignKeyAction::SetNull,
        ReferentialAction::SetDefault => ForeignKeyAction::SetDefault,
    }
}

fn convert_data_type(t: DataType) -> Type {
    match t {
        DataType::String => Type::String,
        DataType::Int => Type::Integer,
        _ => unimplemented!("{:?}", t),
    }
}
