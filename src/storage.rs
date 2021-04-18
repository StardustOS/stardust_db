use std::{convert::TryInto, mem::size_of};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{Type, Value},
    error::{Error, ExecutionError, Result},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnName {
    table_name: Option<String>,
    column_name: String,
}

impl ColumnName {
    pub fn new(table_name: Option<String>, column_name: String) -> Self {
        Self {
            table_name,
            column_name,
        }
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_ref().map(|n| n.as_ref())
    }

    pub fn column_name(&self) -> &str {
        self.column_name.as_ref()
    }

    pub fn destructure(self) -> (Option<String>, String) {
        (self.table_name, self.column_name)
    }
}

impl std::fmt::Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        /*if let Some(table_name) = &self.table_name {
            write!(f, "{}.{}", table_name, self.column_name)
        } else {
            write!(f, "{}", self.column_name)
        }*/
        write!(f, "{}", self.column_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnEntry {
    t: Type,
    position: usize,
    null_index: usize,
    default: Value,
}

impl ColumnEntry {
    pub fn new(t: Type, position: usize, null_index: usize, default: Value) -> Self {
        Self {
            t,
            position,
            null_index,
            default,
        }
    }

    pub fn get_type(&self) -> Type {
        self.t
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn null_index(&self) -> usize {
        self.null_index
    }

    pub fn bitmask_index(&self) -> (usize, usize) {
        (self.null_index / 8, self.null_index % 8)
    }

    pub fn default_value(&self) -> Value {
        self.default.clone()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Columns {
    columns: IndexMap<String, ColumnEntry>,
    sized_len: usize,
    sized_count: usize,
    unsized_count: usize,
}

impl Columns {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_column(name: String, t: Type, default: Value) -> Self {
        let mut columns = Self::with_capacity(1);
        columns
            .add_column(name, t, default)
            .expect("No columns so this can't fail");
        columns
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: IndexMap::with_capacity(capacity),
            ..Default::default()
        }
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    fn bitmask_size(&self) -> usize {
        (self.sized_count + 7) / 8
    }

    fn bitmask_start(&self) -> usize {
        self.sized_len + self.unsized_count * size_of::<u16>()
    }

    fn last_unsized_position(&self) -> usize {
        assert!(self.unsized_count > 0);
        self.sized_len + (self.unsized_count - 1) * size_of::<u16>()
    }

    pub fn get_index(&self, column: &str) -> Option<usize> {
        self.columns.get_index_of(column)
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|k| k.as_str())
    }

    pub fn get_data_type(&self, column: &str) -> Option<Type> {
        self.columns.get(column).map(|c| c.get_type())
    }

    pub fn get_default(&self, column: &str) -> Result<Value> {
        self.columns
            .get(column)
            .map(|e| e.default_value())
            .ok_or_else(|| ExecutionError::NoColumn(column.to_owned()).into())
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        self.columns
            .get_index(index)
            .map(|(k, _)| k.as_str())
            .ok_or_else(|| Error::Internal(format!("No column with index {}", index)))
    }

    pub fn add_column(&mut self, name: String, t: Type, default: Value) -> Result<usize> {
        if self.contains_column(&name) {
            return Err(ExecutionError::ColumnExists(name).into());
        }
        let index = self.columns.len();
        if let Some(s) = t.size() {
            let entry = ColumnEntry::new(t, self.sized_len, self.sized_count, default);
            self.columns.insert(name, entry);
            self.sized_len += s;
            self.sized_count += 1;
        } else {
            let entry = ColumnEntry::new(t, self.unsized_count * size_of::<u16>(), 0, default);
            self.columns.insert(name, entry);
            self.unsized_count += 1;
        }
        Ok(index)
    }

    pub fn generate_row<I>(&self, data: I) -> Result<Vec<u8>>
    where
        I: ExactSizeIterator<Item = Value>,
    {
        if self.columns.len() != data.len() {
            return Err(Error::Execution(ExecutionError::WrongNumColumns {
                expected: self.columns.len(),
                actual: data.len(),
            }));
        }
        let bitmask_start = self.bitmask_start();
        let mut row = vec![0; bitmask_start + self.bitmask_size()];

        for (entry, value) in self.columns.values().zip(data) {
            let contents = entry.get_type().resolve_value(value);
            let pos = entry.position();
            if let Some(size) = entry.get_type().size() {
                if let Some(contents) = contents {
                    let (index, bit) = entry.bitmask_index();
                    row[bitmask_start + index] |= 1 << bit;
                    let (bytes, _) = contents.encode();
                    row[pos..pos + size].copy_from_slice(bytes.as_ref());
                } // Otherwise value and bitmask is 0
            } else if let Some(contents) = contents {
                let (bytes, _) = contents.encode();
                append_unsized(self.sized_len + pos, bytes.as_ref(), &mut row);
            } // Otherwise dictionary entry is 0
        }
        Ok(row)
    }

    pub fn get_data<K>(&self, key: K, row: &[u8]) -> Result<Value>
    where
        K: ColumnKey,
    {
        let entry = key.get_entry(&self.columns)?;
        if let Some(s) = entry.get_type().size() {
            let position = entry.position();
            let bitmask_start = self.bitmask_start();
            let (index, bit) = entry.bitmask_index();
            if row[bitmask_start + index] & 1 << bit == 0 {
                Ok(Value::Null)
            } else {
                let bytes = &row[position..position + s];
                entry.get_type().decode(bytes).map(Value::TypedValue)
            }
        } else {
            let position = self.sized_len + entry.position();
            if row[position..position + size_of::<u16>()] == [0u8; size_of::<u16>()] {
                Ok(Value::Null)
            } else {
                let bytes = get_unsized_data(position, self.last_unsized_position(), row);
                entry.get_type().decode(bytes).map(Value::TypedValue)
            }
        }
    }
}

pub trait ColumnKey {
    fn get_entry(self, map: &IndexMap<String, ColumnEntry>) -> Result<&ColumnEntry>;
}

impl ColumnKey for usize {
    fn get_entry(self, map: &IndexMap<String, ColumnEntry>) -> Result<&ColumnEntry> {
        map.get_index(self)
            .map(|(_k, v)| v)
            .ok_or_else(|| Error::Internal(format!("Could not get entry for index {}", self)))
    }
}

impl ColumnKey for &str {
    fn get_entry(self, map: &IndexMap<String, ColumnEntry>) -> Result<&ColumnEntry> {
        map.get(self)
            .ok_or_else(|| ExecutionError::NoColumn(self.to_owned()).into())
    }
}

fn append_unsized(dictionary_position: usize, bytes: &[u8], row: &mut Vec<u8>) {
    let data_position = (row.len() as u16).to_be_bytes();
    row[dictionary_position..dictionary_position + size_of::<u16>()]
        .copy_from_slice(&data_position);
    row.extend_from_slice(bytes.as_ref());
}

fn get_unsized_data(dictionary_position: usize, end_position: usize, row: &[u8]) -> &[u8] {
    let mut next_start = dictionary_position;
    let end = loop {
        next_start += size_of::<u16>();
        if next_start > end_position {
            break row.len();
        }
        let end = u16::from_be_bytes(
            row[next_start..next_start + size_of::<u16>()]
                .try_into()
                .unwrap(), //Shouldn't be possible,
        ) as usize;
        if end > 0 {
            break end;
        }
    };
    let next_start = dictionary_position + size_of::<u16>();
    let data_position =
        u16::from_be_bytes(row[dictionary_position..next_start].try_into().unwrap()) as usize;
    &row[data_position..end]
}
