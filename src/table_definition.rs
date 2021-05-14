use serde::{Deserialize, Serialize};

use crate::{
    data_types::{Type, Value},
    error::{ExecutionError, Result},
    resolved_expression::{Expression, ResolvedColumn},
    storage::{ColumnKey, ColumnName, Columns},
    TableColumns,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition<C: AsRef<Columns>> {
    columns: C,
    not_nulls: Vec<usize>,
    uniques: Vec<(Vec<usize>, String)>,
    primary_key: Option<(Vec<usize>, String)>,
    checks: Vec<(Expression, String)>,
}

impl<C: AsRef<Columns>> TableDefinition<C> {
    pub fn new_empty(columns: C) -> Self {
        Self {
            columns,
            not_nulls: Vec::new(),
            uniques: Vec::new(),
            primary_key: None,
            checks: Vec::new(),
        }
    }

    pub fn new(
        columns: C,
        not_nulls: Vec<usize>,
        uniques: Vec<(Vec<usize>, String)>,
        primary_key: Option<(Vec<usize>, String)>,
    ) -> Self {
        Self {
            columns,
            not_nulls,
            uniques,
            primary_key,
            checks: Vec::new(),
        }
    }

    pub fn add_check(&mut self, check: Expression, name: String) {
        self.checks.push((check, name))
    }

    pub fn num_columns(&self) -> usize {
        self.columns.as_ref().len()
    }

    pub fn columns(&self) -> &Columns {
        self.columns.as_ref()
    }

    pub fn column_index(&self, column_name: &str) -> Result<usize> {
        self.columns
            .as_ref()
            .get_index(column_name)
            .ok_or_else(|| ExecutionError::NoColumn(column_name.to_owned()).into())
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        self.columns.as_ref().column_name(index)
    }

    pub fn get_default<K: ColumnKey>(&self, column_name: K) -> Result<Value> {
        self.columns.as_ref().get_default(column_name)
    }

    pub fn get_data<K: ColumnKey>(&self, name: K, row: &[u8]) -> Result<Value> {
        self.columns.as_ref().get_data(name, row)
    }

    pub fn get_data_type(&self, column_name: &str) -> Option<Type> {
        self.columns.as_ref().get_data_type(column_name)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.as_ref().column_names()
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.as_ref().contains_column(column)
    }

    pub fn uniques(&self) -> impl Iterator<Item = (&[usize], &str)> {
        self.uniques.iter().map(|(u, n)| (u.as_slice(), n.as_str()))
    }

    pub fn not_nulls(&self) -> impl Iterator<Item = usize> + '_ {
        self.not_nulls.iter().copied()
    }

    pub fn checks(&self) -> impl Iterator<Item = (&Expression, &str)> {
        self.checks.iter().map(|(c, n)| (c, n.as_str()))
    }

    pub fn primary_key(&self) -> Option<(&[usize], &str)> {
        self.primary_key
            .as_ref()
            .map(|(keys, name)| (keys.as_slice(), name.as_str()))
    }
}

impl<C: AsRef<Columns>> TableColumns for (&TableDefinition<C>, &str) {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        let (definition, this_name) = self;
        let (table, column) = name.destructure();
        if let Some(table) = table {
            if table == *this_name && definition.contains_column(&column) {
                Ok(ResolvedColumn::new(table, column))
            } else {
                Err(ExecutionError::NoColumn(format!("{}.{}", table, column)).into())
            }
        } else if definition.contains_column(&column) {
            Ok(ResolvedColumn::new(this_name.to_string(), column))
        } else {
            Err(ExecutionError::NoColumn(format!("{}.{}", this_name.to_string(), column)).into())
        }
    }
}
