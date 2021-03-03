use crate::ast::Column;
use crate::error::{Error, ExecutionError, Result};
use indexmap::map::IndexMap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::convert::TryInto;
use std::fmt::Formatter;
use std::mem::size_of;
use std::num::ParseIntError;

pub type IntegerStorage = i64;


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    Integer,
    String,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Integer => write!(f, "Integer"),
            Type::String => write!(f, "String"),
        }
    }
}

impl Type {
    pub fn size(&self) -> Option<usize> {
        match self {
            Type::Integer => Some(size_of::<IntegerStorage>()),
            Type::String => None,
        }
    }

    pub fn decode(&self, data: &[u8]) -> Result<TypeContents> {
        Ok(match self {
            Type::Integer => {
                TypeContents::Integer(IntegerStorage::from_be_bytes(data.try_into().map_err(
                    |_| Error::Internal("Incorrect number of bytes of Integer Decode".to_string()),
                )?))
            }
            Type::String => TypeContents::String(String::from_utf8(data.into()).map_err(|_| {
                Error::Internal(format!("Could not decode bytes to string: {:?}", data))
            })?),
        })
    }

    pub fn get_contents_from_string(&self, data: String) -> Result<TypeContents> {
        Ok(match self {
            Type::Integer => TypeContents::Integer(data.parse().map_err(|e: ParseIntError| {
                ExecutionError::ParseError(data, *self, e.to_string())
            })?),
            Type::String => TypeContents::String(data),
        })
    }
}

#[derive(Debug, Clone)]
pub enum TypeContents {
    Integer(IntegerStorage),
    String(String),
}

impl std::fmt::Display for TypeContents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeContents::Integer(i) => write!(f, "{}", i),
            TypeContents::String(s) => write!(f, "{}", s),
        }
    }
}

impl TypeContents {
    pub fn encode(&self) -> (Cow<[u8]>, Type) {
        match self {
            TypeContents::Integer(i) => (Cow::Owned(i.to_be_bytes().into()), Type::Integer),
            TypeContents::String(s) => (Cow::Borrowed(s.as_bytes()), Type::String),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Columns {
    columns: IndexMap<String, (Type, usize)>,
    sized_len: usize,
    unsized_count: usize,
}

impl Columns {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|name| name.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn add_column(&mut self, name: String, t: Type) -> Result<()> {
        if self.columns.contains_key(&name) {
            return Err(Error::Execution(ExecutionError::ColumnExists(name)));
        }
        if let Some(s) = t.size() {
            self.columns.insert(name, (t, self.sized_len));
            self.sized_len += s;
        } else {
            self.columns
                .insert(name, (t, self.unsized_count * 2 * size_of::<u32>()));
            self.unsized_count += 1;
        }
        Ok(())
    }

    pub fn extend_from_existing(&mut self, existing: Columns) -> Result<()> {
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
    }

    pub fn generate_row(&self, data: Vec<String>) -> Result<Vec<u8>> {
        if self.columns.len() != data.len() {
            return Err(Error::Execution(ExecutionError::WrongNumColumns {
                expected: self.columns.len(),
                actual: data.len(),
            }));
        }
        let mut row = vec![0; self.sized_len + self.unsized_count * 2 * size_of::<u32>()];

        for (&(t, pos), value) in self.columns.values().zip(data) {
            let contents = t.get_contents_from_string(value)?;
            let (bytes, _) = contents.encode();
            if let Some(size) = t.size() {
                row[pos..pos + size].copy_from_slice(bytes.as_ref());
            } else {
                append_unsized(self.sized_len + pos, bytes.as_ref(), &mut row);
            }
        }

        Ok(row)
    }

    pub fn get_data<'a>(&self, name: &str, row: &'a [u8]) -> Option<(Type, &'a [u8])> {
        let &(t, position) = self.columns.get(name)?;
        if let Some(s) = t.size() {
            Some((t, &row[position..position + s]))
        } else {
            get_unsized_data(self.sized_len + position, row).map(|b| (t, b))
        }
    }

    pub fn get_typed_data(&self, name: &str, row: &[u8]) -> Option<TypeContents> {
        let (t, bytes) = self.get_data(name, row)?;
        t.decode(bytes).ok()
    }

    pub fn filter_row(&self, row: &[u8], filter: &Columns) -> Result<Vec<u8>> {
        let mut result = vec![0; filter.sized_len + filter.unsized_count * 2 * size_of::<u32>()];
        for (name, &(t, new_pos)) in filter.columns.iter() {
            let (original_t, bytes) = self
                .get_data(name, row)
                .ok_or_else(|| ExecutionError::NoData(name.to_string()))?;
            if t != original_t {
                return Err(Error::Execution(ExecutionError::TypeError {
                    column: name.to_string(),
                    expected_type: t,
                    actual_type: original_t,
                }));
            }

            if let Some(s) = t.size() {
                result[new_pos..new_pos + s].copy_from_slice(bytes)
            } else {
                append_unsized(filter.sized_len + new_pos, bytes, &mut result)
            }
        }
        Ok(result)
    }
}

pub struct JoinColumns<'a> {
    columns: &'a [&'a Columns],
    result: Columns,
}

impl<'a> JoinColumns<'a> {
    pub fn new(columns: &'a [&'a Columns]) -> Result<Self> {
        let mut result = Columns::new();
        for (name, (t, _)) in columns.into_iter().flat_map(|c| c.columns.iter()) {
            result.add_column(name.to_string(), *t)?;
        }
        Ok(Self { columns, result })
    }

    pub fn join_rows(&self, rows: &[&[u8]]) -> Result<Vec<u8>> {
        if rows.len() != self.columns.len() {
            return Err(Error::Internal(
                "Incorrect number of rows in JoinColumns".to_string(),
            ));
        }
        let mut result =
            vec![0; self.result.sized_len + self.result.unsized_count * 2 * size_of::<u32>()];
        let mut sized_position = 0;
        let mut unsized_directory_position = self.result.sized_len;
        for (column, row) in self.columns.iter().zip(rows.iter()) {
            for (t, existing_pos) in column.columns.values() {
                if let Some(size) = t.size() {
                    result[sized_position..sized_position + size]
                        .copy_from_slice(&row[*existing_pos..*existing_pos + size]);
                    sized_position += size;
                } else {
                    let position = column.sized_len + existing_pos;
                    let bytes = get_unsized_data(position, row)
                        .ok_or_else(|| Error::Internal("row not long enough".to_string()))?;
                    append_unsized(unsized_directory_position, bytes, &mut result);
                    unsized_directory_position += 2 * size_of::<u32>()
                }
            }
        }
        Ok(result)
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

fn get_unsized_data(position: usize, row: &[u8]) -> Option<&[u8]> {
    let size_start = position + size_of::<u32>();
    let data_position =
        u32::from_be_bytes(row.get(position..size_start)?.try_into().unwrap()) as usize;
    let size = u32::from_be_bytes(
        row.get(size_start..size_start + size_of::<u32>())?
            .try_into()
            .unwrap(), //Shouldn't be possible,
    );
    row.get(data_position..data_position + size as usize)
}
