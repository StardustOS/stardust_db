use std::{convert::TryInto, mem::size_of};

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
        if let Some(table_name) = &self.table_name {
            write!(f, "{}.{}", table_name, self.column_name)
        } else {
            write!(f, "{}", self.column_name)
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ColumnEntry {
    t: Type,
    position: usize,
    null_index: usize,
}

impl ColumnEntry {
    pub fn new(t: Type, position: usize, null_index: usize) -> Self {
        Self {
            t,
            position,
            null_index,
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Columns {
    columns: Vec<ColumnEntry>,
    sized_len: usize,
    sized_count: usize,
    unsized_count: usize,
}

impl Columns {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_column(t: Type) -> Self {
        let mut columns = Self::with_capacity(1);
        columns.add_column(t);
        columns
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: Vec::with_capacity(capacity),
            ..Default::default()
        }
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
        assert!(self.sized_count > 0);
        (self.unsized_count - 1) * size_of::<u16>()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn add_column(&mut self, t: Type) -> usize {
        let index = self.columns.len();
        if let Some(s) = t.size() {
            let entry = ColumnEntry::new(t, self.sized_len, self.sized_count);
            self.columns.push(entry);
            self.sized_len += s;
            self.sized_count += 1;
        } else {
            let entry = ColumnEntry::new(t, self.unsized_count * size_of::<u16>(), 0);
            self.columns.push(entry);
            self.unsized_count += 1;
        }
        index
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
        let bitmask_size = self.bitmask_size();
        let mut row = vec![0; self.bitmask_start() + bitmask_size];
        let bitmask_start = self.bitmask_start();

        for (&entry, value) in self.columns.iter().zip(data) {
            let contents = entry.get_type().resolve_value(value);
            let pos = entry.position();
            if let Some(size) = entry.get_type().size() {
                if let Some(contents) = contents {
                    let (index, bit) = entry.bitmask_index();
                    row[bitmask_start + index] |= 1 << bit;
                    let (bytes, _) = contents.encode();
                    row[pos..pos + size].copy_from_slice(bytes.as_ref());
                } // Otherwise value and bitmask is 0
            } else {
                if let Some(contents) = contents {
                    let (bytes, _) = contents.encode();
                    append_unsized(self.sized_len + pos, bytes.as_ref(), &mut row);
                } // Otherwise dictionary entry is 0
            }
        }
        println!("{:?}", row);
        Ok(row)
    }

    pub fn get_data<'a>(&self, index: usize, row: &'a [u8]) -> Result<Value> {
        let entry = self
            .columns
            .get(index)
            .ok_or_else(|| Error::Internal(format!("No column for index {}", index)))?;
        if let Some(s) = entry.get_type().size() {
            let position = entry.position();
            let bitmask_start = self.bitmask_start();
            let (index, bit) = entry.bitmask_index();
            if row[bitmask_start + index] & 1 << bit == 0 {
                Ok(Value::Null)
            } else {
                let bytes = &row[position..position + s];
                entry
                    .get_type()
                    .decode(bytes)
                    .map(|contents| Value::TypedValue(contents))
            }
        } else {
            let position = self.sized_len + entry.position();
            if &row[position..position + size_of::<u16>()] == &[0u8; size_of::<u16>()] {
                Ok(Value::Null)
            } else {
                let bytes = get_unsized_data(position, self.last_unsized_position(), row);
                entry
                    .get_type()
                    .decode(bytes)
                    .map(|contents| Value::TypedValue(contents))
            }
        }
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
