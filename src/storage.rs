use std::convert::TryInto;
use std::mem::size_of;

use indexmap::map::IndexMap;
use serde::{Deserialize, Serialize};

use crate::data_types::{Type, Value};
use crate::error::{Error, ExecutionError, Result};

#[derive(Debug, Clone, Default, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnName {
    table_name: Option<String>,
    column_name: String
}

impl ColumnName {
    pub fn new(table_name: Option<String>, column_name: String) -> Self {
        Self { table_name, column_name }
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Columns {
    columns: IndexMap<ColumnName, (Type, usize)>,
    sized_len: usize,
    unsized_count: usize,
}

impl Columns {
    pub fn new() -> Self {
        Default::default()
    }

    fn find_entry(&self, name: &ColumnName) -> Result<&(Type, usize)> {
        if name.table_name.is_some() {
            self.columns.get(name).ok_or_else(|| Error::Execution(ExecutionError::NoColumn(name.to_string())))
        } else {
            if let Some(result) = self.columns.get(name) {
                Ok(result)
            } else {
                let mut result = None;
                for (existing_name, entry) in &self.columns {
                    if existing_name.column_name == name.column_name {
                        if result.is_some() {
                            return Err(Error::Execution(ExecutionError::AmbiguousName(name.to_string())));
                        } else {
                            result = Some(entry)
                        }
                    }
                }
                result.ok_or_else(|| Error::Execution(ExecutionError::NoColumn(name.to_string())))
            }
        }
    }

    pub fn display_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|name| name.column_name.as_ref())
    }

    pub fn column_names(&self) -> impl Iterator<Item = &ColumnName> {
        self.columns.keys()
    }

    pub fn names_and_types(&self) -> impl Iterator<Item = (&ColumnName, Type)> {
        self.columns
            .iter()
            .map(|(name, (t, _))| (name, *t))
    }

    pub fn get_type(&self, name: &ColumnName) -> Result<Type> {
        self.find_entry(name).map(|(t, _)| *t)
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn add_column(&mut self, name: ColumnName, t: Type) -> Result<()> {
        if self.columns.contains_key(&name) {
            return Err(Error::Execution(ExecutionError::ColumnExists(name.to_string())));
        }
        if let Some(s) = t.size() {
            self.columns.insert(name, (t, self.sized_len));
            self.sized_len += s + 1;
        } else {
            self.columns
                .insert(name, (t, self.unsized_count * 2 * size_of::<u32>()));
            self.unsized_count += 1;
        }
        Ok(())
    }

    /*pub fn extend_from_existing(&mut self, existing: Columns) -> Result<()> {
        for (name, (t, _)) in existing.columns {
            self.add_column(name, t)?
        }
        Ok(())
    }

    pub fn extend_from_existing_filter<I>(&mut self, existing: &Columns, names: I) -> Result<()>
    where
        I: IntoIterator<Item = String>,
    {
        for name in names {
            if let Some((t, _)) = existing.columns.get(&name) {
                self.add_column(name, *t)?
            }
        }
        Ok(())
    }*/

    pub fn generate_row(&self, data: Vec<Value>) -> Result<Vec<u8>> {
        if self.columns.len() != data.len() {
            return Err(Error::Execution(ExecutionError::WrongNumColumns {
                expected: self.columns.len(),
                actual: data.len(),
            }));
        }
        let mut row = vec![0; self.sized_len + self.unsized_count * 2 * size_of::<u32>()];

        for (&(t, pos), value) in self.columns.values().zip(data) {
            let contents = t.resolve_value(value)?;
            if let Some(size) = t.size() {
                if let Some(contents) = contents {
                    row[pos] = 1;
                    let (bytes, _) = contents.encode();
                    row[pos + 1..pos + 1 + size].copy_from_slice(bytes.as_ref());
                } // Otherwise value is 0
            } else {
                if let Some(contents) = contents {
                    let (bytes, _) = contents.encode();
                    append_unsized(self.sized_len + pos, bytes.as_ref(), &mut row);
                } // Otherwise dictionary entry is 0
            }
        }

        Ok(row)
    }

    pub fn get_data<'a>(&self, name: &ColumnName, row: &'a [u8]) -> Result<Value> {
        let &(t, position) = self.find_entry(name)?;
        if let Some(s) = t.size() {
            if row[position] == 0 {
                Ok(Value::Null)
            } else {
                let bytes = &row[position + 1..position + 1 + s];
                t.decode(bytes).map(|contents| Value::TypedValue(contents))
            }
        } else {
            if &row[position..position + 2 * size_of::<u32>()] == &[0u8; 2 * size_of::<u32>()] {
                Ok(Value::Null)
            } else {
                let bytes = get_unsized_data(self.sized_len + position, row);
                t.decode(bytes).map(|contents| Value::TypedValue(contents))
            }
        }
    }
}

pub struct JoinColumns<'a> {
    columns: &'a [&'a Columns],
    result: Columns
}

impl<'a> JoinColumns<'a> {
    pub fn new(columns: &'a [&'a Columns]) -> Result<Self> {
        let mut result = Columns::new();
        for columns in columns {
            for (name, &(t, _)) in &columns.columns {
                result.add_column(name.clone(), t)?;
            }
        }

        Ok(Self { columns, result })
    }
}


fn append_unsized(position: usize, bytes: &[u8], row: &mut Vec<u8>) {
    let data_position = (row.len() as u32).to_be_bytes();
    row[position..position + size_of::<u32>()].copy_from_slice(&data_position);
    let size_point = position + size_of::<u32>();
    row[size_point..size_point + size_of::<u32>()]
        .copy_from_slice(&(bytes.len() as u32).to_be_bytes());
    row.extend_from_slice(bytes.as_ref());
}

fn get_unsized_data(position: usize, row: &[u8]) -> &[u8] {
    let size_start = position + size_of::<u32>();
    let data_position = u32::from_be_bytes(row[position..size_start].try_into().unwrap()) as usize;
    let size = u32::from_be_bytes(
        row[size_start..size_start + size_of::<u32>()]
            .try_into()
            .unwrap(), //Shouldn't be possible,
    );
    &row[data_position..data_position + size as usize]
}
