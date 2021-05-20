use std::{borrow::Borrow, collections::HashSet, convert::TryInto, mem::size_of, ops::Deref};

use auto_enums::auto_enum;
use itertools::Itertools;
use sled::{Batch, IVec, Tree};

use crate::{
    ast::ColumnName,
    data_types::Value,
    foreign_key::Action,
    interpreter::{evaluate_expression, Interpreter},
    storage::{ColumnKey, Columns},
    table_definition::TableDefinition,
};
use crate::{
    error::{Error, ExecutionError, Result},
    resolved_expression::{Expression, ResolvedColumn},
    GetData, TableColumns,
};

#[derive(Debug)]
pub struct TableHandler<C: Borrow<Columns>, N: AsRef<str>> {
    tree: Tree,
    table_definition: TableDefinition<C>,
    table_name: N,
    alias: Option<N>,
}

impl<C: Borrow<Columns>, N: AsRef<str>> TableHandler<C, N> {
    pub fn new(
        tree: Tree,
        table_definition: TableDefinition<C>,
        table_name: N,
        alias: Option<N>,
    ) -> Self {
        Self {
            tree,
            table_definition,
            table_name,
            alias,
        }
    }

    pub fn get_value<K: ColumnKey>(&self, column_name: K, row: &TableRow) -> Result<Value> {
        self.table_definition.get_data(column_name, &row.right)
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

    fn update_get_left_right(
        &self,
        row: TableRow,
        interpreter: &Interpreter,
        new_row: Vec<Value>,
    ) -> Result<(IVec, Vec<u8>)> {
        self.check_row(&new_row, interpreter, Some(&row))?;
        for key in interpreter
            .foreign_keys()?
            .parent_foreign_keys(self.unaliased_table_name(), interpreter)
        {
            let key = key?;
            key.check_parent_rows(&row, self, Action::Update(&new_row), interpreter)?;
        }
        let right = self
            .table_definition
            .columns()
            .generate_row(new_row.into_iter())?;
        Ok((row.left, right))
    }

    pub fn update_row(
        &self,
        row: TableRow,
        interpreter: &Interpreter,
        new_row: Vec<Value>,
    ) -> Result<()> {
        let (left, right) = self.update_get_left_right(row, interpreter, new_row)?;
        self.tree.insert(left, right)?;
        Ok(())
    }

    pub fn update_row_batch(
        &self,
        row: TableRow,
        interpreter: &Interpreter,
        new_row: Vec<Value>,
        batch: &mut Batch
    ) -> Result<()> {
        let (left, right) = self.update_get_left_right(row, interpreter, new_row)?;
        batch.insert(left, right);
        Ok(())
    }

    pub fn insert_values(&self, values: Vec<Value>, interpreter: &Interpreter) -> Result<()> {
        self.check_row(&values, interpreter, None)?;
        let key = self.generate_next_index()?;
        let value = self
            .table_definition
            .columns()
            .generate_row(values.into_iter())?;
        self.tree.insert(key.to_be_bytes(), value)?;
        Ok(())
    }

    pub fn insert_values_batch(&self, values: Vec<Value>, interpreter: &Interpreter, batch: &mut Batch, key: &mut u64) -> Result<()> {
        self.check_row(&values, interpreter, None)?;
        let value = self
            .table_definition
            .columns()
            .generate_row(values.into_iter())?;
        batch.insert(&key.to_be_bytes(), value);
        *key += 1;
        Ok(())
    }

    pub fn apply_batch(&self, batch: Batch) -> Result<()> {
        self.tree.apply_batch(batch)?;
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

    pub fn contains_unique(&self, column_set: Vec<usize>) -> bool {
        for set in column_set.into_iter().powerset() {
            if self.uniques().any(|(s, _)| *s == set) {
                return true;
            }
        }
        false
    }

    fn check_row(
        &self,
        row: &[Value],
        interpreter: &Interpreter,
        exclude: Option<&TableRow>,
    ) -> Result<()> {
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
        for tree_row in self.iter() {
            let tree_row = tree_row?;
            if let Some(exclude) = exclude {
                if &tree_row == exclude {
                    continue;
                }
            }
            for (unique_set, name) in self.uniques() {
                let mut identical = true;
                for &index in unique_set {
                    let other_value = self.get_value(index, &tree_row)?;
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
            .child_foreign_keys(self.unaliased_table_name(), interpreter)
        {
            foreign_key?.check_row_contains(row)?;
        }
        Ok(())
    }

    pub fn unaliased_table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    pub fn aliased_table_name(&self) -> &str {
        self.alias
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or_else(|| self.table_name.as_ref())
    }

    pub fn generate_next_index(&self) -> Result<u64> {
        if let Some((last_key, _)) = self.tree.last()? {
            let bytes = last_key
                .get(..size_of::<u64>())
                .ok_or_else(|| Error::Internal("Key is wrong number of bytes".to_owned()))?
                .try_into()
                .unwrap();
            let value = u64::from_be_bytes(bytes) + 1;
            Ok(value)
        } else {
            Ok(0u64)
        }
    }
}

impl<C: Borrow<Columns>, N: AsRef<str>> Deref for TableHandler<C, N> {
    type Target = TableDefinition<C>;

    fn deref(&self) -> &Self::Target {
        &self.table_definition
    }
}

pub struct TableIter {
    iter: sled::Iter,
}

impl TableIter {
    pub fn new(tree: Tree) -> Self {
        Self {
            iter: tree.iter(),
        }
    }

    pub fn get_next(&mut self) -> Result<Option<TableRow>> {
        Ok(self
            .iter
            .next()
            .transpose()?
            .map(|(left, right)| TableRow::new(left, right)))
    }

    pub fn filter_where<'a, C: Borrow<Columns>, N: AsRef<str>>(
        &'a mut self,
        predicate: &'a Expression,
        handler: &'a TableHandler<C, N>,
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

impl PartialEq for &TableRow {
    fn eq(&self, other: &Self) -> bool {
        self.left == other.left
    }
}

pub struct TableRowUpdater<'a, C: Borrow<Columns>, N: AsRef<str>> {
    row: &'a TableRow,
    handler: &'a TableHandler<C, N>,
    new_row: Vec<Value>,
}

impl<'a, C: Borrow<Columns>, N: AsRef<str>> TableRowUpdater<'a, C, N> {
    pub fn new(row: &'a TableRow, handler: &'a TableHandler<C, N>) -> Self {
        let new_row = Vec::with_capacity(handler.num_columns());
        Self {
            row,
            handler,
            new_row,
        }
    }

    pub fn add_update(&mut self, index: usize, new_value: Value) -> Result<()> {
        for i in self.new_row.len()..index {
            self.new_row.push(self.handler.get_value(i, &self.row)?);
        }
        self.new_row.push(new_value);
        Ok(())
    }

    pub fn finalise(self) -> Result<Vec<Value>> {
        let mut new_row = self.new_row;
        for i in new_row.len()..self.handler.num_columns() {
            new_row.push(self.handler.get_value(i, &self.row)?);
        }
        Ok(new_row)
    }
}

pub struct RowBuilder<'a, C: Borrow<Columns>, N: AsRef<str>> {
    handler: &'a TableHandler<C, N>,
    new_row: Vec<Value>,
    inserted: HashSet<usize>,
}

impl<'a, C: Borrow<Columns>, N: AsRef<str>> RowBuilder<'a, C, N> {
    pub fn new(handler: &'a TableHandler<C, N>) -> Self {
        Self {
            handler,
            new_row: vec![Value::Null; handler.num_columns()],
            inserted: HashSet::new(),
        }
    }

    pub fn insert(&mut self, column: &str, value: Value) -> Result<()> {
        let index = self.handler.column_index(column)?;
        self.inserted.insert(index);
        self.new_row[index] = value;
        Ok(())
    }

    pub fn finalise(self) -> Vec<Value> {
        let mut new_row = self.new_row;
        for (i, value) in new_row.iter_mut().enumerate() {
            if !self.inserted.contains(&i) {
                *value = self.handler.get_default(i);
            }
        }
        new_row
    }
}

impl<C: Borrow<Columns>, N: AsRef<str>> TableColumns for TableHandler<C, N> {
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

impl<'a, C: Borrow<Columns>, N: AsRef<str>> GetData for (&'a TableHandler<C, N>, &'a TableRow) {
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

impl<C: Borrow<Columns>, N: AsRef<str>> GetData for (&TableHandler<C, N>, &[Value]) {
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
