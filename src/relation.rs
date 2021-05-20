use std::fmt::{Display, Formatter};

#[cfg(test)]
use std::collections::HashSet;

use itertools::Itertools;

use crate::{
    data_types::Value,
    error::{ExecutionError, Result},
};

#[derive(Debug, Clone, Default)]
pub struct Relation {
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl Relation {
    pub(crate) fn new(column_names: Vec<String>) -> Self {
        Self {
            column_names,
            rows: Vec::new(),
        }
    }

    pub(crate) fn add_row(&mut self, row: Vec<Value>) -> Result<()> {
        if self.column_names.len() == row.len() {
            self.rows.push(row);
            Ok(())
        } else {
            Err(ExecutionError::WrongNumColumns {
                expected: self.column_names.len(),
                actual: self.rows.len(),
            }
            .into())
        }
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.column_names.iter().map(|n| n.as_ref())
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.column_names.iter().any(|c| column == c)
    }

    pub fn rows(&self) -> impl Iterator<Item = &[Value]> {
        self.rows.iter().map(|r| r.as_slice())
    }

    pub fn take_rows(self) -> Vec<Vec<Value>> {
        self.rows
    }

    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.column_names.is_empty()
    }

    pub fn get_value(&self, column: usize, row: usize) -> &Value {
        &self.rows[row][column]
    }

    pub fn get_value_named(&self, column: &str, row: usize) -> Option<&Value> {
        let column_index = self
            .column_names
            .iter()
            .position(|c| column == c.as_str())?;
        Some(&self.rows[row][column_index])
    }

    #[cfg(test)]
    pub(crate) fn assert_equals(&self, rows: HashSet<Vec<Value>>, column_names: Vec<&str>) {
        assert_eq!(self.rows.len(), rows.len());

        assert_eq!(self.column_names, column_names);
        dbg!(&self.rows);

        for row in &self.rows {
            assert!(rows.contains(row));
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        self.rows
            .iter()
            .map(move |row| Row::new(self.column_names.as_slice(), row.as_slice()))
    }
}

pub struct Row<'a> {
    columns: &'a [String],
    row: &'a [Value],
}

impl<'a> Row<'a> {
    fn new(columns: &'a [String], row: &'a [Value]) -> Self {
        Self { columns, row }
    }

    pub fn get_value_index(&self, index: usize) -> Option<&Value> {
        self.row.get(index)
    }

    pub fn get_value_named(&self, column_name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|name| name.as_str() == column_name)
            .map(|index| &self.row[index])
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.column_names.is_empty() {
            writeln!(f, "{}", self.column_names.iter().join("|"))?;
            for row in &self.rows {
                writeln!(f, "{}", row.iter().join("|"))?;
            }
        }
        Ok(())
    }
}
