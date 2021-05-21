use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

#[cfg(test)]
use std::collections::HashSet;

use itertools::Itertools;

use crate::{
    ast::{OrderBy, OrderByDirection},
    data_types::{Comparison, Value},
    error::{ExecutionError, Result},
};

/// Stores a list of rows returned by a query.
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

    pub(crate) fn sort(&mut self, order_by: Vec<OrderBy>) -> Result<()> {
        if order_by.is_empty() {
            return Ok(());
        }

        let order_by = order_by
            .into_iter()
            .map(|o| {
                let OrderBy {
                    expression,
                    direction,
                    nulls_first,
                } = o;
                let expression = expression.to_string();
                let index = self
                    .column_names
                    .iter()
                    .position(|s| s.as_str() == expression)
                    .ok_or(ExecutionError::NoColumn(expression))?;
                Ok((index, direction, nulls_first))
            })
            .collect::<Result<Vec<_>>>()?;

        self.rows.sort_unstable_by(|a, b| {
            for (index, direction, nulls_first) in &order_by {
                let a = &a[*index];
                let b = &b[*index];
                let mut order = match (a, b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => {
                        if *nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (_, Value::Null) => {
                        if *nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    _ => match a.compare(b) {
                        Comparison::LessThan => Ordering::Less,
                        Comparison::Equal => Ordering::Equal,
                        Comparison::GreaterThan => Ordering::Greater,
                        Comparison::Unknown => unreachable!(),
                    },
                };
                if matches!(direction, OrderByDirection::Descending) {
                    order = order.reverse();
                }
                if !matches!(order, Ordering::Equal) {
                    return order;
                }
            }
            Ordering::Equal
        });
        Ok(())
    }

    /// Returns an `Iterator` of column names.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.column_names.iter().map(|n| n.as_ref())
    }

    /// Checks if the column name is contained in the `Relation`.
    pub fn contains_column(&self, column: &str) -> bool {
        self.column_names.iter().any(|c| column == c)
    }

    /// Returns an `Iterator` of rows from the `Relation`.
    pub fn rows(&self) -> impl Iterator<Item = &[Value]> {
        self.rows.iter().map(|r| r.as_slice())
    }

    /// Returns a `Vec` of rows.
    pub fn take_rows(self) -> Vec<Vec<Value>> {
        self.rows
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    /// Returns the number of rows.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Checks if the `Relation` is an empty result.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.column_names.is_empty()
    }

    /// Gets a value from the specified row and column indexes.
    pub fn get_value(&self, column: usize, row: usize) -> Option<&Value> {
        self.rows.get(row).and_then(|row| row.get(column))
    }

    /// Gets a value by column name from the specified row.
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

    #[cfg(test)]
    pub(crate) fn assert_equals_ordered(&self, rows: Vec<Vec<Value>>, column_names: Vec<&str>) {
        assert_eq!(self.rows, rows);
        assert_eq!(self.column_names, column_names)
    }

    /// Returns an `Iterator` of `Row`s
    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        self.rows
            .iter()
            .map(move |row| Row::new(self.column_names.as_slice(), row.as_slice()))
    }
}

/// A row from a `Relation`
pub struct Row<'a> {
    columns: &'a [String],
    row: &'a [Value],
}

impl<'a> Row<'a> {
    fn new(columns: &'a [String], row: &'a [Value]) -> Self {
        Self { columns, row }
    }

    /// Get a value by index.
    pub fn get_value_index(&self, index: usize) -> Option<&Value> {
        self.row.get(index)
    }

    /// Get a value by name.
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
