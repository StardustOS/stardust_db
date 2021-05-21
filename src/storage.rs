use std::{convert::TryInto, mem::size_of};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    ast::ColumnName,
    data_types::{Type, Value},
    error::{Error, ExecutionError, Result},
    resolved_expression::ResolvedColumn,
    TableColumns,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn bitmask_index(&self) -> (usize, usize) {
        (self.null_index / 8, self.null_index % 8)
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

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: IndexMap::with_capacity(capacity),
            ..Default::default()
        }
    }

    pub fn contains_column(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    fn bitmask_size(&self) -> usize {
        (self.sized_count + 7) / 8
    }

    fn bitmask_start(&self) -> usize {
        self.sized_len
    }

    fn directory_start(&self) -> usize {
        self.sized_len + self.bitmask_size()
    }

    fn fixed_len(&self) -> usize {
        self.sized_len + self.bitmask_size() + self.unsized_count * size_of::<u16>()
    }

    fn last_unsized_position(&self) -> usize {
        assert!(self.unsized_count > 0);
        self.directory_start() + (self.unsized_count - 1) * size_of::<u16>()
    }

    pub fn get_index(&self, column: &str) -> Option<usize> {
        self.columns.get_index_of(column)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|k| k.as_str())
    }

    pub fn get_data_type(&self, column: &str) -> Option<Type> {
        self.columns.get(column).map(|c| c.get_type())
    }

    pub fn column_name(&self, index: usize) -> Result<&str> {
        self.columns
            .get_index(index)
            .map(|(k, _)| k.as_str())
            .ok_or_else(|| Error::Internal(format!("No column with index {}", index)))
    }

    pub fn add_column(&mut self, name: String, t: Type) -> Result<usize> {
        if self.contains_column(&name) {
            return Err(ExecutionError::ColumnExists(name).into());
        }
        let index = self.columns.len();
        if let Some(s) = t.size() {
            let entry = ColumnEntry::new(t, self.sized_len, self.sized_count);
            self.columns.insert(name, entry);
            self.sized_len += s;
            self.sized_count += 1;
        } else {
            let entry = ColumnEntry::new(t, self.unsized_count * size_of::<u16>(), 0);
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
        let mut row = vec![0; self.fixed_len()];

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
                append_unsized(self.directory_start() + pos, bytes.as_ref(), &mut row);
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
            let position = self.directory_start() + entry.position();
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

impl TableColumns for (&Columns, &str) {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        let (columns, this_name) = self;
        let (table, column) = name.destructure();
        if let Some(table) = table {
            if table == *this_name && columns.contains_column(&column) {
                Ok(ResolvedColumn::new(table, column))
            } else {
                Err(ExecutionError::NoColumn(format!("{}.{}", table, column)).into())
            }
        } else if columns.contains_column(&column) {
            Ok(ResolvedColumn::new(this_name.to_string(), column))
        } else {
            Err(ExecutionError::NoColumn(format!("{}.{}", this_name.to_string(), column)).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_types::{Type, Value};

    use super::Columns;

    #[test]
    fn single_sized() {
        let mut columns = Columns::new();
        columns.add_column("id".to_string(), Type::Integer).unwrap();
        assert_eq!(
            columns.generate_row(vec![25.into()].into_iter()).unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 25, 1]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 25, 1])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns.get_data(0, &[0, 0, 0, 0, 0, 0, 0, 25, 1]).unwrap(),
            25.into()
        );
        assert_eq!(
            columns.generate_row(vec![Value::Null].into_iter()).unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns.get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn single_unsized() {
        let mut columns = Columns::new();
        columns
            .add_column("name".to_string(), Type::String)
            .unwrap();
        assert_eq!(
            columns
                .generate_row(vec!["User".into()].into_iter())
                .unwrap(),
            vec![0, 2, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 2, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns.get_data(0, &[0, 2, 85, 115, 101, 114]).unwrap(),
            "User".into()
        );
        assert_eq!(
            columns.generate_row(vec![Value::Null].into_iter()).unwrap(),
            vec![0, 0]
        );
        assert_eq!(columns.get_data("name", &[0, 0]).unwrap(), Value::Null);
        assert_eq!(columns.get_data(0, &[0, 0]).unwrap(), Value::Null);
    }

    #[test]
    fn sized_then_unsized() {
        let mut columns = Columns::new();
        columns.add_column("id".to_string(), Type::Integer).unwrap();
        columns
            .add_column("name".to_string(), Type::String)
            .unwrap();
        assert_eq!(
            columns
                .generate_row(vec![25.into(), "User".into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data(
                    "id",
                    &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(
                    "name",
                    &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );

        assert_eq!(
            columns
                .generate_row(vec![25.into(), Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            Value::Null
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, "User".into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(
                    "name",
                    &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn unsized_then_sized() {
        let mut columns = Columns::new();
        columns
            .add_column("name".to_string(), Type::String)
            .unwrap();
        columns.add_column("id".to_string(), Type::Integer).unwrap();
        assert_eq!(
            columns
                .generate_row(vec!["User".into(), 25.into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data(
                    "id",
                    &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(
                    "name",
                    &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, 25.into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 25, 1, 0, 0])
                .unwrap(),
            Value::Null
        );

        assert_eq!(
            columns
                .generate_row(vec!["User".into(), Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(
                    "name",
                    &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114]
                )
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn multiple_sized() {
        let mut columns = Columns::new();
        columns.add_column("id".to_string(), Type::Integer).unwrap();
        columns
            .add_column("age".to_string(), Type::Integer)
            .unwrap();
        assert_eq!(
            columns
                .generate_row(vec![1.into(), 25.into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 3]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 3])
                .unwrap(),
            1.into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 3])
                .unwrap(),
            1.into()
        );
        assert_eq!(
            columns
                .get_data("age", &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 3])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 3])
                .unwrap(),
            25.into()
        );

        assert_eq!(
            columns
                .generate_row(vec![1.into(), Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .unwrap(),
            1.into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .unwrap(),
            1.into()
        );
        assert_eq!(
            columns
                .get_data("age", &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .unwrap(),
            Value::Null
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, 25.into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 2]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 2])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 2])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data("age", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 2])
                .unwrap(),
            25.into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 2])
                .unwrap(),
            25.into()
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            columns
                .get_data("id", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data("age", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn multiple_unsized() {
        let mut columns = Columns::new();
        columns
            .add_column("name".to_string(), Type::String)
            .unwrap();
        columns
            .add_column("hobby".to_string(), Type::String)
            .unwrap();
        assert_eq!(
            columns
                .generate_row(vec!["User".into(), "Music".into()].into_iter())
                .unwrap(),
            vec![0, 4, 0, 8, 85, 115, 101, 114, 77, 117, 115, 105, 99]
        );
        assert_eq!(
            columns
                .get_data(
                    "name",
                    &[0, 4, 0, 8, 85, 115, 101, 114, 77, 117, 115, 105, 99]
                )
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 4, 0, 8, 85, 115, 101, 114, 77, 117, 115, 105, 99])
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(
                    "hobby",
                    &[0, 4, 0, 8, 85, 115, 101, 114, 77, 117, 115, 105, 99]
                )
                .unwrap(),
            "Music".into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 4, 0, 8, 85, 115, 101, 114, 77, 117, 115, 105, 99])
                .unwrap(),
            "Music".into()
        );

        assert_eq!(
            columns
                .generate_row(vec!["User".into(), Value::Null].into_iter())
                .unwrap(),
            vec![0, 4, 0, 0, 85, 115, 101, 114]
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 4, 0, 0, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 4, 0, 0, 85, 115, 101, 114])
                .unwrap(),
            "User".into()
        );
        assert_eq!(
            columns
                .get_data("hobby", &[0, 4, 0, 0, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 4, 0, 0, 85, 115, 101, 114])
                .unwrap(),
            Value::Null
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, "Music".into()].into_iter())
                .unwrap(),
            vec![0, 0, 0, 4, 77, 117, 115, 105, 99]
        );
        assert_eq!(
            columns
                .get_data("name", &[0, 0, 0, 4, 77, 117, 115, 105, 99])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data(0, &[0, 0, 0, 4, 77, 117, 115, 105, 99])
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            columns
                .get_data("hobby", &[0, 0, 0, 4, 77, 117, 115, 105, 99])
                .unwrap(),
            "Music".into()
        );
        assert_eq!(
            columns
                .get_data(1, &[0, 0, 0, 4, 77, 117, 115, 105, 99])
                .unwrap(),
            "Music".into()
        );

        assert_eq!(
            columns
                .generate_row(vec![Value::Null, Value::Null].into_iter())
                .unwrap(),
            vec![0, 0, 0, 0]
        );
        assert_eq!(
            columns.get_data("name", &[0, 0, 0, 0]).unwrap(),
            Value::Null
        );
        assert_eq!(columns.get_data(0, &[0, 0, 0, 0]).unwrap(), Value::Null);
        assert_eq!(
            columns.get_data("hobby", &[0, 0, 0, 0]).unwrap(),
            Value::Null
        );
        assert_eq!(columns.get_data(1, &[0, 0, 0, 0]).unwrap(), Value::Null);
    }
}
