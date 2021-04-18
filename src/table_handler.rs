use std::{convert::TryInto, mem::size_of};

use auto_enums::auto_enum;
use sled::{IVec, Tree};

use crate::{
    data_types::{Type, Value},
    foreign_key::Action,
    interpreter::{evaluate_expression, Interpreter},
    storage::{ColumnKey, ColumnName},
    table_definition::TableDefinition,
};
use crate::{
    error::{Error, ExecutionError, Result},
    resolved_expression::{Expression, ResolvedColumn},
    GetData, TableColumns,
};

#[derive(Debug)]
pub struct TableHandler {
    tree: Tree,
    table_definition: TableDefinition,
    table_name: String,
    alias: Option<String>,
}

impl TableHandler {
    pub fn new(
        tree: Tree,
        table_definition: TableDefinition,
        table_name: String,
        alias: Option<String>,
    ) -> Self {
        Self {
            tree,
            table_definition,
            table_name,
            alias,
        }
    }

    pub fn get_data_type(&self, column_name: &str) -> Option<Type> {
        self.table_definition.get_data_type(column_name)
    }

    pub fn get_value<K: ColumnKey>(&self, column_name: K, row: &TableRow) -> Result<Value> {
        self.table_definition.get_data(column_name, &row.right)
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.table_definition.contains_column(column_name)
    }

    pub fn column_index(&self, column_name: &str) -> Result<usize> {
        self.table_definition.column_index(column_name)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.table_definition.column_names()
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        self.table_definition.column_name(index)
    }

    pub fn iter(&self) -> TableIter {
        TableIter::new(self.tree.clone())
    }

    pub fn delete_row(&self, row: &TableRow, interpreter: &Interpreter) -> Result<()> {
        for key in interpreter
            .foreign_keys()?
            .parent_foreign_keys(self.unaliased_table_name(), interpreter)
        {
            let key = key?;
            key.check_parent_rows(&row, self, Action::Delete, interpreter)?;
        }
        self.tree.remove(&row.left)?;
        Ok(())
    }

    pub fn update_row(
        &self,
        row: TableRow,
        interpreter: &Interpreter,
        new_row: Vec<Value>,
    ) -> Result<()> {
        for key in interpreter
            .foreign_keys()?
            .parent_foreign_keys(self.unaliased_table_name(), interpreter)
        {
            let key = key?;
            key.check_parent_rows(&row, self, Action::Update(&new_row), interpreter)?;
        }
        self.tree.insert(row.left, row.right)?;
        Ok(())
    }

    pub fn insert_values(&self, values: Vec<Value>, interpreter: &Interpreter) -> Result<()> {
        self.check_row(&values, interpreter)?;
        let key = self.generate_next_index()?;
        let value = self
            .table_definition
            .columns()
            .generate_row(values.into_iter())?;
        self.tree.insert(key, value)?;
        Ok(())
    }

    #[auto_enum(Iterator)]
    fn not_nulls(&self) -> impl Iterator<Item = (usize, Option<&str>)> {
        let not_nulls = self.table_definition.not_nulls().map(|i| (i, None));
        if let Some((primary_key, name)) = self.table_definition.primary_key() {
            not_nulls.chain(primary_key.iter().map(move |i| (*i, Some(name))))
        } else {
            not_nulls
        }
    }

    #[auto_enum(Iterator)]
    fn uniques(&self) -> impl Iterator<Item = (&[usize], &str)> {
        let uniques = self.table_definition.uniques();
        if let Some((primary_key, name)) = self.table_definition.primary_key() {
            uniques.chain(std::iter::once((primary_key, name)))
        } else {
            uniques
        }
    }

    fn check_row(&self, row: &[Value], interpreter: &Interpreter) -> Result<()> {
        for (check, name) in self.table_definition.checks() {
            if !evaluate_expression(check, &(self, row))?.is_true() {
                return Err(ExecutionError::CheckConstraintFailed(name.to_owned()).into());
            }
        }
        for (i, name) in self.not_nulls() {
            if row[i].is_null() {
                return Err(ExecutionError::NullConstraintFailed(
                    if let Some(name) = name {
                        name
                    } else {
                        self.table_definition.columns().column_name(i)?
                    }
                    .to_owned(),
                )
                .into());
            }
        }
        for tree_row in self.tree.iter() {
            let (_, row_bytes) = tree_row?;
            for (unique_set, name) in self.uniques() {
                let mut identical = true;
                for &index in unique_set {
                    let other_value = self
                        .table_definition
                        .columns()
                        .get_data(index, &row_bytes)?;
                    if !row[index].compare(&other_value).is_equal() {
                        identical = false;
                        break;
                    }
                }
                if identical {
                    return Err(ExecutionError::UniqueConstraintFailed(name.to_owned()).into());
                }
            }
        }
        for foreign_key in interpreter
            .foreign_keys()?
            .table_foreign_keys(self.unaliased_table_name(), interpreter)
        {
            foreign_key?.check_row_contains(row)?;
        }
        Ok(())
    }

    pub fn num_columns(&self) -> usize {
        self.table_definition.num_columns()
    }

    pub fn get_default(&self, column_name: &str) -> Result<Value> {
        self.table_definition.get_default(column_name)
    }

    pub fn unaliased_table_name(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn aliased_table_name(&self) -> &str {
        self.alias
            .as_deref()
            .unwrap_or_else(|| self.table_name.as_str())
    }

    fn generate_next_index(&self) -> Result<[u8; 8]> {
        if let Some((last_key, _)) = self.tree.last()? {
            let bytes = last_key
                .get(..size_of::<u64>())
                .ok_or_else(|| Error::Internal("Key is wrong number of bytes".to_owned()))?
                .try_into()
                .unwrap();
            let value = u64::from_be_bytes(bytes) + 1;
            Ok(value.to_be_bytes())
        } else {
            Ok(0u64.to_be_bytes())
        }
    }
}

pub struct TableIter {
    tree: Tree,
    iter: sled::Iter,
}

impl TableIter {
    pub fn new(tree: Tree) -> Self {
        Self {
            iter: tree.iter(),
            tree,
        }
    }

    pub fn get_next(&mut self) -> Result<Option<TableRow>> {
        Ok(self
            .iter
            .next()
            .transpose()?
            .map(|(left, right)| TableRow::new(left, right)))
    }

    pub fn old_filter(
        &mut self,
        predicate: &Expression,
        handler: &TableHandler,
    ) -> Result<Option<TableRow>> {
        while let Some(next) = self.get_next()? {
            if evaluate_expression(predicate, &(handler, &next))?.is_true() {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    pub fn filter_where<'a>(
        &'a mut self,
        predicate: &'a Expression,
        handler: &'a TableHandler,
    ) -> impl Iterator<Item = Result<TableRow>> + 'a {
        self.filter(move |r| {
            if let Ok(row) = r {
                if let Ok(evaluated) = evaluate_expression(predicate, &(handler, row)) {
                    return evaluated.is_true();
                }
            }
            true
        })
    }

    pub fn reset(&mut self) {
        self.iter = self.tree.iter();
    }

    pub fn reset_next(&mut self) -> Result<Option<TableRow>> {
        self.reset();
        self.get_next()
    }
}

impl Iterator for TableIter {
    type Item = Result<TableRow>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.iter
                .next()?
                .map(|(left, right)| TableRow::new(left, right))
                .map_err(Into::into),
        )
    }
}

#[derive(Default, Clone)]
pub struct TableRow {
    left: IVec,
    right: IVec,
}

impl TableRow {
    fn new(left: IVec, right: IVec) -> Self {
        Self { left, right }
    }

    pub fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }
}

impl TableColumns for TableHandler {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        let this_name = self.aliased_table_name();
        let (table_name, column_name) = name.destructure();
        if let Some(table_name) = table_name {
            if this_name == table_name && self.table_definition.contains_column(&column_name) {
                return Ok(ResolvedColumn::new(table_name, column_name));
            }
        } else if self.table_definition.contains_column(&column_name) {
            return Ok(ResolvedColumn::new(this_name.to_owned(), column_name));
        }
        Err(Error::Execution(ExecutionError::NoColumn(column_name)))
    }
}

impl<'a> GetData for (&'a TableHandler, &'a TableRow) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (handler, row) = self;
        if row.left.is_empty() && row.right.is_empty() {
            return Ok(Value::Null);
        }

        if column_name.table_name() == handler.aliased_table_name() {
            handler
                .table_definition
                .get_data(column_name.column_name(), &row.right)
        } else {
            Err(Error::Internal(format!(
                "Table name resolved incorrectly for Single Table Row: {}",
                column_name.table_name()
            )))
        }
    }
}

impl GetData for (&TableHandler, &[Value]) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (handler, row) = self;
        let index = handler
            .table_definition
            .columns()
            .get_index(column_name.column_name())
            .ok_or_else(|| {
                Error::Internal(format!("Incorrectly resolved column {}", column_name))
            })?;
        Ok(row[index].clone())
    }
}
