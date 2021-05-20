use std::{borrow::Borrow, collections::HashMap, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::{
    ast::ColumnName,
    data_types::Value,
    error::{ExecutionError, Result},
    resolved_expression::{Expression, ResolvedColumn},
    storage::Columns,
    TableColumns,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition<C: Borrow<Columns>> {
    columns: C,
    not_nulls: Vec<usize>,
    uniques: Vec<(Vec<usize>, String)>,
    primary_key: Option<(Vec<usize>, String)>,
    checks: Vec<(Expression, String)>,
    defaults: HashMap<usize, Value>,
}

impl<C: Borrow<Columns>> TableDefinition<C> {
    pub fn new_empty(columns: C) -> Self {
        Self {
            columns,
            not_nulls: Vec::new(),
            uniques: Vec::new(),
            primary_key: None,
            checks: Vec::new(),
            defaults: HashMap::new(),
        }
    }

    pub fn new(
        columns: C,
        not_nulls: Vec<usize>,
        uniques: Vec<(Vec<usize>, String)>,
        primary_key: Option<(Vec<usize>, String)>,
        checks: Vec<(Expression, String)>,
        defaults: HashMap<usize, Value>,
    ) -> Self {
        Self {
            columns,
            not_nulls,
            uniques,
            primary_key,
            checks,
            defaults,
        }
    }

    pub fn columns(&self) -> &Columns {
        self.columns.borrow()
    }

    pub fn column_index(&self, column_name: &str) -> Result<usize> {
        self.columns
            .borrow()
            .get_index(column_name)
            .ok_or_else(|| ExecutionError::NoColumn(column_name.to_owned()).into())
    }

    pub fn get_default(&self, column: usize) -> Value {
        self.defaults.get(&column).cloned().unwrap_or_default()
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

impl<C: Borrow<Columns>> Deref for TableDefinition<C> {
    type Target = Columns;

    fn deref(&self) -> &Self::Target {
        self.columns()
    }
}

impl<C: Borrow<Columns>> TableColumns for (&TableDefinition<C>, &str) {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        let (definition, this_name) = self;
        let columns = definition.columns();
        (columns, *this_name).resolve_name(name)
    }
}
