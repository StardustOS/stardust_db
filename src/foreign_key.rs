use std::{borrow::Borrow, convert::TryInto};

use co_sort::{co_sort, Permutation};

use crate::{
    ast::{ForeignKey, ForeignKeyAction},
    data_types::{IntegerStorage, Value},
    error::{ExecutionError, Result},
    interpreter::Interpreter,
    storage::Columns,
    table_handler::{TableHandler, TableRow, TableRowUpdater},
};

#[derive(Debug)]
pub struct ForeignKeys<'a, C: Borrow<Columns>, N: AsRef<str>> {
    handler: &'a TableHandler<C, N>,
}

impl<'a, C: Borrow<Columns>, N: AsRef<str>> ForeignKeys<'a, C, N> {
    pub fn new(handler: &'a TableHandler<C, N>) -> Self {
        Self { handler }
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
            name.into(),
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

    pub fn child_foreign_keys<'b>(
        &'b self,
        table: &'b str,
        interpreter: &'b Interpreter,
    ) -> impl Iterator<Item = Result<ChildKeyChecker>> + 'b {
        self.handler.iter().filter_map(move |row| {
            (|| {
                let row = row?;
                let table_name = self.handler.get_value("table", &row)?;
                if table_name.assume_string()? == table {
                    let foreign_key_name = self.handler.get_value("name", &row)?.assume_string()?;
                    let mut this_columns = self
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
                    let foreign_handler = interpreter.open_table(foreign_table, None)?;
                    let mut foreign_columns = self
                        .handler
                        .get_value("referred_columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|column_name| foreign_handler.column_index(column_name))
                        .collect::<Result<Vec<_>>>()?;

                    co_sort!(foreign_columns, this_columns);

                    Ok(Some(ChildKeyChecker::new(
                        foreign_key_name,
                        this_columns,
                        foreign_handler,
                        foreign_columns,
                    )))
                } else {
                    Ok(None)
                }
            })()
            .transpose()
        })
    }

    pub fn parent_foreign_keys<'b>(
        &'b self,
        table: &'b str,
        interpreter: &'b Interpreter,
    ) -> impl Iterator<Item = Result<ParentKeyChecker>> + 'b {
        self.handler.iter().filter_map(move |row| {
            (|| {
                let row = row?;
                let child_table = self.handler.get_value("referred_table", &row)?;
                if child_table.assume_string()? == table {
                    let foreign_key_name = self.handler.get_value("name", &row)?.assume_string()?;
                    let parent_table = self.handler.get_value("table", &row)?.assume_string()?;
                    let mut parent_columns: Vec<usize> = self
                        .handler
                        .get_value("columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|c| self.handler.column_index(c))
                        .collect::<Result<_>>()?;
                    let parent_handler = interpreter.open_table(parent_table, None)?;
                    let mut child_columns: Vec<usize> = self
                        .handler
                        .get_value("referred_columns", &row)?
                        .assume_string()?
                        .split('|')
                        .map(|column_name| parent_handler.column_index(column_name))
                        .collect::<Result<_>>()?;

                    co_sort!(parent_columns, child_columns);
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
                    Ok(Some(ParentKeyChecker::new(
                        foreign_key_name,
                        child_columns,
                        parent_handler,
                        parent_columns,
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

pub struct ChildKeyChecker {
    name: String,
    this_columns: Vec<usize>,
    foreign_handler: TableHandler<Columns, String>,
    foreign_columns: Vec<usize>,
}

impl ChildKeyChecker {
    pub fn new(
        name: String,
        this_columns: Vec<usize>,
        foreign_handler: TableHandler<Columns, String>,
        foreign_columns: Vec<usize>,
    ) -> Self {
        Self {
            name,
            this_columns,
            foreign_handler,
            foreign_columns,
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
                    .get_value(*foreign_column, &foreign_row)?;
                if !this_row[this_column].equals_or_null(&foreign_value) {
                    continue 'row;
                }
            }
            return Ok(());
        }
        Err(ExecutionError::ForeignKeyConstraintFailed(self.name).into())
    }
}

pub struct ParentKeyChecker {
    name: String,
    child_columns: Vec<usize>,
    parent_handler: TableHandler<Columns, String>,
    parent_columns: Vec<usize>,
    on_update: ForeignKeyAction,
    on_delete: ForeignKeyAction,
}

impl ParentKeyChecker {
    pub fn new(
        name: String,
        child_columns: Vec<usize>,
        parent_handler: TableHandler<Columns, String>,
        parent_columns: Vec<usize>,
        on_update: ForeignKeyAction,
        on_delete: ForeignKeyAction,
    ) -> Self {
        assert_eq!(child_columns.len(), parent_columns.len());
        Self {
            name,
            child_columns,
            parent_handler,
            parent_columns,
            on_update,
            on_delete,
        }
    }

    pub fn check_parent_rows<H: Borrow<Columns>, N: AsRef<str>>(
        self,
        this_row: &TableRow,
        handler: &TableHandler<H, N>,
        action: Action,
        interpreter: &Interpreter,
    ) -> Result<()> {
        let foreign_key_action = match &action {
            Action::Delete => self.on_delete,
            Action::Update(_) => self.on_update,
        };
        'row: for parent_row in self.parent_handler.iter() {
            let parent_row = parent_row?;
            for (child, parent) in self.child_columns.iter().zip(self.parent_columns.iter()) {
                let foreign_value = self.parent_handler.get_value(*parent, &parent_row)?;
                if !handler
                    .get_value(*child, this_row)?
                    .equals_or_null(&foreign_value)
                {
                    continue 'row;
                }
            }
            match foreign_key_action {
                ForeignKeyAction::NoAction => {
                    return Err(ExecutionError::ForeignKeyConstraintFailed(self.name).into())
                }
                ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                    let mut new_row = TableRowUpdater::new(&parent_row, &self.parent_handler);
                    for parent in self.parent_columns.iter().copied() {
                        match foreign_key_action {
                            ForeignKeyAction::SetNull => new_row.add_update(parent, Value::Null)?,
                            ForeignKeyAction::SetDefault => {
                                let default = self.parent_handler.get_default(parent);
                                new_row.add_update(parent, default)?;
                            }
                            _ => unreachable!(),
                        }
                    }
                    let new_row = new_row.finalise()?;
                    self.parent_handler
                        .update_row(parent_row, interpreter, new_row)?;
                }
                ForeignKeyAction::Cascade => match action {
                    Action::Delete => self.parent_handler.delete_row(&parent_row, interpreter)?,
                    Action::Update(updated_row) => {
                        let mut new_row = TableRowUpdater::new(&parent_row, &self.parent_handler);
                        for (&parent, &child) in
                            self.parent_columns.iter().zip(self.child_columns.iter())
                        {
                            new_row.add_update(parent, updated_row[child].clone())?;
                        }
                        let new_row = new_row.finalise()?;
                        self.parent_handler
                            .update_row(parent_row, interpreter, new_row)?;
                    }
                },
            }
        }
        Ok(())
    }
}
