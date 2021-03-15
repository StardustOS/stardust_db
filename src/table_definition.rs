use std::{collections::HashSet, convert::TryInto};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use sled::Tree;

use crate::{
    data_types::{Type, Value},
    error::{Error, ExecutionError, Result},
    storage::Columns,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Position {
    Left,
    Right,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    mappings: IndexMap<String, (Position, usize, Value)>,
    columns: TreeEntry,
    not_nulls: HashSet<usize>,
    uniques: HashSet<usize>,
}

impl TableDefinition {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mappings: IndexMap::with_capacity(capacity),
            columns: TreeEntry::Right(Columns::with_capacity(capacity)),
            not_nulls: HashSet::new(),
            uniques: HashSet::new(),
        }
    }

    pub fn add_column(
        mut self,
        name: String,
        default: Option<Value>,
        not_null: bool,
        unique: bool,
        primary_key: bool,
        data_type: Type,
    ) -> Result<Self> {
        if self.mappings.contains_key(&name) {
            return Err(ExecutionError::ColumnExists(name).into());
        }
        let value_index = self.mappings.len();
        let (columns, position, index) = if primary_key {
            match self.columns {
                TreeEntry::Right(right) => {
                    let left = Columns::from_column(data_type);
                    (TreeEntry::Both(left, right), Position::Left, 0)
                }
                TreeEntry::Both(mut left, right) => {
                    let index = left.add_column(data_type);
                    (TreeEntry::Both(left, right), Position::Left, index)
                }
            }
        } else {
            match self.columns {
                TreeEntry::Right(mut right) => {
                    let index = right.add_column(data_type);
                    (TreeEntry::Right(right), Position::Right, index)
                }
                TreeEntry::Both(left, mut right) => {
                    let index = right.add_column(data_type);
                    (TreeEntry::Both(left, right), Position::Right, index)
                }
            }
        };
        self.columns = columns;
        self.mappings
            .insert(name, (position, index, default.unwrap_or_default()));
        if not_null {
            self.not_nulls.insert(value_index);
        }
        if unique {
            self.uniques.insert(value_index);
        }

        Ok(self)
    }

    pub fn num_parts(&self) -> usize {
        self.columns.num_parts()
    }

    pub fn columns(&self) -> &TreeEntry {
        &self.columns
    }

    pub fn get_default(&self, column_name: &str) -> Result<Value> {
        self.mappings
            .get(column_name)
            .map(|(_, _, value)| value.clone())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "Error getting default: no column named {}",
                    column_name
                ))
            })
    }

    pub fn get_data<'a>(&self, name: &str, left: &'a [u8], right: &'a [u8]) -> Result<Value> {
        let (position, index, _) = *self
            .mappings
            .get(name)
            .ok_or_else(|| ExecutionError::NoColumn(name.to_owned()))?;
        self.columns.get_data(index, position, left, right)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.mappings.keys().map(|c| c.as_ref())
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        let (name, _) = self
            .mappings
            .get_index(index)
            .ok_or_else(|| Error::Internal(format!("No column with index {}", index)))?;
        Ok(name.as_str())
    }

    fn get_value_index(&self, index: usize) -> Result<(Position, usize)> {
        self.mappings
            .get_index(index)
            .map(|(_, (position, index, _))| (*position, *index))
            .ok_or_else(|| Error::Internal(format!("No mapping for index {}", index)))
    }

    fn check_row(&self, tree: &Tree, row: &[Value]) -> Result<()> {
        for &i in &self.not_nulls {
            if row[i].is_null() {
                return Err(
                    ExecutionError::NullConstraintFailed(self.column_name(i)?.to_owned()).into(),
                );
            }
        }
        for tree_row in tree.iter() {
            let (left, right) = tree_row?;
            for &i in &self.uniques {
                let (position, index) = self.get_value_index(i)?;
                let value =
                    self.columns
                        .get_data(index, position, left.as_ref(), right.as_ref())?;
                if row[i]
                    .compare(&value)
                    .get_truth(&crate::ast::ComparisonOp::Eq)
                    .is_true()
                {
                    return Err(ExecutionError::UniqueConstraintFailed(
                        self.column_name(i)?.to_owned(),
                        value,
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    pub fn insert_values(&self, tree: &Tree, values: Vec<Value>) -> Result<()> {
        self.check_row(tree, &values)?;
        match &self.columns {
            TreeEntry::Right(c) => {
                let key = generate_next_index(tree)?;
                let value = c.generate_row(values.into_iter())?;
                tree.insert(key, value)?;
            }
            TreeEntry::Both(l, r) => {
                let mut iter = values.into_iter();
                let key = l.generate_row(iter.by_ref().take(l.len()))?;
                let value = r.generate_row(iter)?;
                tree.insert(key, value)?;
            }
        }
        Ok(())
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.mappings.contains_key(column)
    }

    pub fn num_columns(&self) -> usize {
        self.mappings.len()
    }
}

fn generate_next_index(tree: &Tree) -> Result<[u8; 8]> {
    if let Some((last_key, _)) = tree.last()? {
        let bytes = last_key
            .as_ref()
            .try_into()
            .map_err(|_| Error::Internal("Key is wrong number of bytes".to_owned()))?;
        let value = u64::from_be_bytes(bytes) + 1;
        Ok(value.to_be_bytes())
    } else {
        Ok(0u64.to_be_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TreeEntry {
    Right(Columns),
    Both(Columns, Columns),
}

impl TreeEntry {
    fn num_parts(&self) -> usize {
        match self {
            TreeEntry::Right(_) => 1,
            TreeEntry::Both(_, _) => 2,
        }
    }

    fn get_data<'a>(
        &self,
        index: usize,
        position: Position,
        left: &'a [u8],
        right: &'a [u8],
    ) -> Result<Value> {
        match (self, position) {
            (Self::Right(c), Position::Right) => c.get_data(index, right),
            (Self::Both(l, _), Position::Left) => l.get_data(index, left),
            (Self::Both(_, r), Position::Right) => r.get_data(index, right),
            _ => Err(Error::Internal(format!(
                "Tried to get left data for right columns with index {}",
                index
            ))),
        }
    }
}
