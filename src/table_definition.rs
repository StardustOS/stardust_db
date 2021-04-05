use std::{convert::TryInto, mem::size_of};

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
    columns: Columns,
    not_nulls: Vec<usize>,
    uniques: Vec<(Vec<usize>, String)>,
    primary_key_name: String,
}

impl TableDefinition {
    pub fn with_capacity(
        capacity: usize,
        uniques: Vec<(Vec<usize>, String)>,
        primary_key_name: String,
    ) -> Self {
        Self {
            columns: Columns::with_capacity(capacity),
            not_nulls: Vec::new(),
            uniques,
            primary_key_name,
        }
    }

    pub fn add_column(
        &mut self,
        name: String,
        default: Option<Value>,
        not_null: bool,
        data_type: Type,
    ) -> Result<()> {
        if self.columns.contains_column(&name) {
            return Err(ExecutionError::ColumnExists(name).into());
        }
        let index = self
            .columns
            .add_column(name, data_type, default.unwrap_or_default())?;
        if not_null {
            self.not_nulls.push(index);
        }
        Ok(())
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn columns(&self) -> &Columns {
        &self.columns
    }

    pub fn get_default(&self, column_name: &str) -> Result<Value> {
        self.columns.get_default(column_name)
    }

    pub fn get_data<'a>(&self, name: &str, row: &'a [u8]) -> Result<Value> {
        self.columns.get_data(name, row)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.column_names()
    }

    fn check_row(&self, tree: &Tree, row: &[Value]) -> Result<()> {
        for &i in &self.not_nulls {
            if row[i].is_null() {
                return Err(ExecutionError::NullConstraintFailed(
                    self.columns.column_name(i)?.to_owned(),
                )
                .into());
            }
        }
        for tree_row in tree.iter() {
            let (_, row_bytes) = tree_row?;
            for (unique_set, name) in &self.uniques {
                let mut identical = true;
                for index in unique_set.iter().copied() {
                    let other_value = self.columns.get_data(index, &row_bytes)?;
                    if !row[index].compare(&other_value).is_equal() {
                        identical = false;
                        break;
                    }
                }
                if identical {
                    return Err(ExecutionError::UniqueConstraintFailed(name.clone()).into());
                }
            }
        }
        Ok(())
    }

    pub fn insert_values(&self, tree: &Tree, values: Vec<Value>) -> Result<()> {
        self.check_row(tree, &values)?;
        let key = generate_next_index(tree)?;
        let mut value = Vec::new();
        self.columns.generate_row(values.into_iter(), &mut value)?;
        tree.insert(key, value)?;
        Ok(())
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.contains_column(column)
    }
}

fn generate_next_index(tree: &Tree) -> Result<[u8; 8]> {
    if let Some((last_key, _)) = tree.last()? {
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
/*
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
*/
