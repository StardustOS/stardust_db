use std::convert::TryInto;

use once_cell::sync::OnceCell;

use crate::{
    ast::{ForeignKey, ForeignKeyAction, TableName},
    data_types::{IntegerStorage, Type, Value},
    error::{ExecutionError, Result},
    interpreter::Interpreter,
    storage::Columns,
    table_handler::{TableHandler, TableRow},
};

#[derive(Debug)]
pub struct ForeignKeys<'a> {
    handler: &'a TableHandler,
}

impl ForeignKeys<'_> {
    pub fn open(interpreter: &Interpreter) -> Result<Self> {
        static HANDLER: OnceCell<TableHandler> = OnceCell::new();
        let handler = HANDLER.get_or_try_init(|| {
            let mut columns = Columns::new();
            columns.add_column("name".to_owned(), Type::String, Value::Null)?;
            columns.add_column("table".to_owned(), Type::String, Value::Null)?;
            columns.add_column("columns".to_owned(), Type::String, Value::Null)?;
            columns.add_column("referred_table".to_owned(), Type::String, Value::Null)?;
            columns.add_column("referred_columns".to_owned(), Type::String, Value::Null)?;
            columns.add_column("on_delete".to_owned(), Type::Integer, Value::Null)?;
            columns.add_column("on_update".to_owned(), Type::Integer, Value::Null)?;

            interpreter.open_internal_table("@foreign_keys".to_owned(), columns)
        })?;
        Ok(Self { handler })
    }

    pub fn add_key(
        &self,
        key: ForeignKey,
        interpreter: &Interpreter,
        table_name: &str,
    ) -> Result<()> {
        let ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        } = key;

        let columns = columns.join("|");
        let referred_columns = referred_columns.join("|");
        let on_delete = IntegerStorage::from(on_delete.unwrap_or_default());
        let on_update = IntegerStorage::from(on_update.unwrap_or_default());
        let values = vec![
            dbg!(name).into(),
            table_name.into(),
            columns.into(),
            foreign_table.into(),
            referred_columns.into(),
            on_delete.into(),
            on_update.into(),
        ];
        self.handler.insert_values(values, interpreter)?;
        Ok(())
    }

    pub fn process_drop_table(&self, table: &str, interpreter: &Interpreter) -> Result<()> {
        for row in self.handler.iter() {
            let row = row?;
            let parent_table = self.handler.get_value("table", &row)?.assume_string()?;
            if parent_table == table {
                self.handler.delete_row(&row, interpreter)?;
            } else if self
                .handler
                .get_value("referred_table", &row)?
                .assume_string()?
                == table
            {
                let key_name = self.handler.get_value("name", &row)?.assume_string()?;
                return Err(ExecutionError::ForeignKeyDependencyDelete {
                    parent_table,
                    key_name,
                }
                .into());
            }
        }
        Ok(())
    }

    pub fn table_foreign_keys<'a>(
        &'a self,
        table: &'a str,
        interpreter: &'a Interpreter,
    ) -> impl Iterator<Item = Result<ForeignKeyChecker>> + 'a {
        self.handler.iter().filter_map(move |row| {
            (|| {
                let row = row?;
                let table_name = self.handler.get_value("table", &row)?;
                if table_name.assume_string()? == table {
                    let foreign_key_name = self.handler.get_value("name", &row)?.assume_string()?;
                    let this_columns = self
                        .handler
                        .get_value("columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|c| self.handler.column_index(c))
                        .collect::<Result<Vec<_>>>()?;
                    let foreign_table = self
                        .handler
                        .get_value("referred_table", &row)?
                        .assume_string()?;
                    let foreign_handler =
                        interpreter.open_table(TableName::new(foreign_table, None))?;
                    let foreign_columns = self
                        .handler
                        .get_value("referred_columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|s| s.to_owned())
                        .collect();
                    Ok(Some(ForeignKeyChecker::new(
                        foreign_key_name,
                        this_columns,
                        foreign_handler,
                        foreign_columns,
                        ForeignKeyAction::NoAction,
                        ForeignKeyAction::NoAction,
                    )))
                } else {
                    Ok(None)
                }
            })()
            .transpose()
        })
    }

    pub fn parent_foreign_keys<'a>(
        &'a self,
        table: &'a str,
        interpreter: &'a Interpreter,
    ) -> impl Iterator<Item = Result<ForeignKeyChecker>> + 'a {
        self.handler.iter().filter_map(move |row| {
            (|| {
                let row = row?;
                let child_table = self.handler.get_value("referred_table", &row)?;
                if child_table.assume_string()? == table {
                    let foreign_key_name = self.handler.get_value("name", &row)?.assume_string()?;
                    let parent_table = self.handler.get_value("table", &row)?.assume_string()?;
                    let parent_columns = self
                        .handler
                        .get_value("referred_columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|c| self.handler.column_index(c))
                        .collect::<Result<Vec<_>>>()?;
                    let child_handler =
                        interpreter.open_table(TableName::new(parent_table, None))?;
                    let child_columns = self
                        .handler
                        .get_value("columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|s| s.to_owned())
                        .collect();
                    let on_update = self
                        .handler
                        .get_value("on_update", &row)?
                        .assume_integer()?
                        .try_into()?;
                    let on_delete = self
                        .handler
                        .get_value("on_delete", &row)?
                        .assume_integer()?
                        .try_into()?;
                    Ok(Some(ForeignKeyChecker::new(
                        foreign_key_name,
                        parent_columns,
                        child_handler,
                        child_columns,
                        on_update,
                        on_delete,
                    )))
                } else {
                    Ok(None)
                }
            })()
            .transpose()
        })
    }
}

pub enum Action<'a> {
    Delete,
    Update(&'a [Value]),
}

pub struct ForeignKeyChecker {
    name: String,
    this_columns: Vec<usize>,
    foreign_handler: TableHandler,
    foreign_columns: Vec<String>,
    on_update: ForeignKeyAction,
    on_delete: ForeignKeyAction,
}

impl ForeignKeyChecker {
    pub fn new(
        name: String,
        this_columns: Vec<usize>,
        foreign_handler: TableHandler,
        foreign_columns: Vec<String>,
        on_update: ForeignKeyAction,
        on_delete: ForeignKeyAction,
    ) -> Self {
        assert_eq!(this_columns.len(), foreign_columns.len());
        Self {
            name,
            this_columns,
            foreign_handler,
            foreign_columns,
            on_update,
            on_delete,
        }
    }

    pub fn check_row_contains(self, this_row: &[Value]) -> Result<()> {
        'row: for foreign_row in self.foreign_handler.iter() {
            let foreign_row = foreign_row?;
            for (&this_column, foreign_column) in
                self.this_columns.iter().zip(self.foreign_columns.iter())
            {
                let foreign_value = self
                    .foreign_handler
                    .get_value(foreign_column.as_str(), &foreign_row)?;
                if this_row[this_column] != foreign_value {
                    continue 'row;
                }
            }
            return Ok(());
        }
        Err(ExecutionError::ForeignKeyConstraintFailed(self.name).into())
    }

    pub fn check_parent_rows(
        self,
        this_row: &TableRow,
        handler: &TableHandler,
        action: Action,
        interpreter: &Interpreter,
    ) -> Result<()> {
        let foreign_key_action = match &action {
            Action::Delete => self.on_delete,
            Action::Update(_) => self.on_update,
        };
        'row: for foreign_row in self.foreign_handler.iter() {
            let foreign_row = foreign_row?;
            for (this_column, foreign_column) in self
                .this_columns
                .iter()
                .copied()
                .zip(self.foreign_columns.iter())
            {
                let foreign_value = self
                    .foreign_handler
                    .get_value(foreign_column.as_str(), &foreign_row)?;
                if handler.get_value(this_column, this_row)? != foreign_value {
                    continue 'row;
                }
            }
            match foreign_key_action {
                ForeignKeyAction::NoAction => {
                    return Err(ExecutionError::ForeignKeyConstraintFailed(self.name).into())
                }
                ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                    let mut new_row = Vec::with_capacity(self.foreign_handler.num_columns());
                    let mut changed_columns = self.foreign_columns.iter().peekable();
                    for column in self.foreign_handler.column_names() {
                        match changed_columns.peek() {
                            Some(change) if change.as_str() == column => {
                                let _ = changed_columns.next();
                                let value = match foreign_key_action {
                                    ForeignKeyAction::SetNull => Value::Null,
                                    ForeignKeyAction::SetDefault => {
                                        self.foreign_handler.get_default(column)?
                                    }
                                    _ => unreachable!(),
                                };
                                new_row.push(value)
                            }
                            _ => {
                                new_row.push(self.foreign_handler.get_value(column, &foreign_row)?)
                            }
                        }
                    }
                    self.foreign_handler
                        .update_row(foreign_row, interpreter, new_row)?;
                }
                ForeignKeyAction::Cascade => match action {
                    Action::Delete => self.foreign_handler.delete_row(&foreign_row, interpreter)?,
                    Action::Update(updated_row) => {
                        let mut new_row = Vec::with_capacity(self.foreign_handler.num_columns());
                        let mut changed_columns = self
                            .this_columns
                            .iter()
                            .zip(self.foreign_columns.iter())
                            .peekable();
                        for column in self.foreign_handler.column_names() {
                            let value = match changed_columns.peek() {
                                Some((&index, change)) if change.as_str() == column => {
                                    updated_row[index].clone()
                                }
                                _ => self.foreign_handler.get_value(column, &foreign_row)?,
                            };
                            new_row.push(value);
                        }
                    }
                },
            }
        }
        Ok(())
    }
}
