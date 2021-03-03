use crate::error::{Error, ExecutionError, Result};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow, convert::TryInto, fmt::Formatter, mem::size_of, num::ParseIntError, ops::Not,
};

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

    pub fn resolve_value(&self, data: Value) -> Result<Option<TypeContents>> {
        Ok(match data {
            Value::Null => None,
            Value::TypedValue(t) => Some(t.cast(self)),
            Value::Literal(s) => Some(self.get_contents_from_string(s)?),
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

    pub fn cast(self, t: &Type) -> Self {
        match (self, t) {
            (TypeContents::String(s), Type::String) => TypeContents::String(s),
            (TypeContents::Integer(i), Type::Integer) => TypeContents::Integer(i),
            (TypeContents::String(s), Type::Integer) => {
                TypeContents::Integer(s.parse().unwrap_or_default())
            }
            (TypeContents::Integer(i), Type::String) => TypeContents::String(i.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    TypedValue(TypeContents),
    Literal(String),
}

impl Value {
    pub fn cast(self, t: &Type) -> Result<Self> {
        Ok(match self {
            Self::Null => Self::Null,
            Self::TypedValue(contents) => Self::TypedValue(contents.cast(t)),
            Self::Literal(literal) => Self::TypedValue(t.get_contents_from_string(literal)?),
        })
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => Ok(()),
            Value::TypedValue(t) => write!(f, "{}", t),
            Value::Literal(s) => write!(f, "{}", s),
        }
    }
}

impl From<TypeContents> for Value {
    fn from(contents: TypeContents) -> Self {
        Self::TypedValue(contents)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

impl Not for TruthValue {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

impl TruthValue {
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, Self::True) => Self::True,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            _ => Self::False,
        }
    }

    pub fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, Self::False) => Self::False,
            (Self::True, _) | (_, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    pub fn is_true(self) -> bool {
        match self {
            Self::True => true,
            _ => false,
        }
    }
}
