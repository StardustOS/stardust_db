use crate::{
    ast::ComparisonOp,
    error::{Error, Result},
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, cmp::Ordering, convert::TryInto, fmt::Formatter, mem::size_of, ops::Not};

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

    pub fn get_contents_from_string(&self, data: String) -> TypeContents {
        match self {
            /*Type::Integer => TypeContents::Integer(data.parse().map_err(|e: ParseIntError| {
                ExecutionError::ParseError(data, *self, e.to_string())
            })?),*/
            Type::Integer => TypeContents::Integer(data.parse().unwrap_or_default()),
            Type::String => TypeContents::String(data),
        }
    }

    pub fn resolve_value(&self, data: Value) -> Option<TypeContents> {
        match data {
            Value::Null => None,
            Value::TypedValue(t) => Some(t.cast(self)),
            Value::TruthValue(t) => match t {
                TruthValue::True => Some(TypeContents::Integer(1).cast(self)),
                TruthValue::False => Some(TypeContents::Integer(0).cast(self)),
                TruthValue::Unknown => None,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        match t {
            Type::Integer => TypeContents::Integer((&self).into()),
            Type::String => TypeContents::String(self.into()),
        }
    }

    pub fn get_truth(&self) -> TruthValue {
        IntegerStorage::from(self).into()
    }

    pub fn get_type(&self) -> Type {
        match self {
            Self::Integer(_) => Type::Integer,
            Self::String(_) => Type::String,
        }
    }
}

impl From<&TypeContents> for IntegerStorage {
    fn from(contents: &TypeContents) -> Self {
        match contents {
            TypeContents::Integer(i) => *i,
            TypeContents::String(s) => s.parse().unwrap_or_default(),
        }
    }
}

impl From<TypeContents> for String {
    fn from(contents: TypeContents) -> Self {
        match contents {
            TypeContents::String(s) => s,
            TypeContents::Integer(i) => i.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    TypedValue(TypeContents),
    TruthValue(TruthValue),
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

pub enum Comparison {
    Unknown,
    LessThan,
    Equal,
    GreaterThan,
}

impl Comparison {
    pub fn get_truth(&self, op: &ComparisonOp) -> TruthValue {
        if matches!(self, Self::Unknown) {
            return TruthValue::Unknown;
        }
        match op {
            ComparisonOp::Eq => matches!(self, Self::Equal).into(),
            ComparisonOp::Gt => matches!(self, Self::GreaterThan).into(),
            ComparisonOp::Lt => matches!(self, Self::LessThan).into(),
            ComparisonOp::GtEq => matches!(self, Self::Equal | Self::GreaterThan).into(),
            ComparisonOp::LtEq => matches!(self, Self::Equal | Self::LessThan).into(),
        }
    }
}

impl From<Ordering> for Comparison {
    fn from(o: Ordering) -> Self {
        match o {
            Ordering::Equal => Comparison::Equal,
            Ordering::Greater => Comparison::GreaterThan,
            Ordering::Less => Comparison::LessThan,
        }
    }
}

impl Value {
    pub fn cast(self, t: &Type) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::TypedValue(contents) => Self::TypedValue(contents.cast(t)),
            Self::TruthValue(truth) => match truth {
                TruthValue::True => Self::TypedValue(TypeContents::Integer(1).cast(t)),
                TruthValue::False => Self::TypedValue(TypeContents::Integer(0).cast(t)),
                TruthValue::Unknown => Self::Null,
            },
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /*pub fn get_type(&self) -> Option<Type> {
        match self {
            Self::Null => None,
            Self::Literal(_) => None,
            Self::TypedValue(contents) => Some(contents.get_type()),
            Self::TruthValue(_) => Some(Type::Integer)
        }
    }*/

    pub fn get_truth(&self) -> TruthValue {
        match self {
            Value::Null => TruthValue::Unknown,
            Value::TypedValue(contents) => contents.get_truth(),
            Value::TruthValue(t) => *t,
        }
    }

    pub fn compare(&self, other: &Value) -> Comparison {
        match (self, &other) {
            (Self::Null, _) | (_, Self::Null) => Comparison::Unknown,
            (
                Self::TypedValue(TypeContents::String(a)),
                Self::TypedValue(TypeContents::String(b)),
            ) => a.cmp(&b).into(),
            (
                Self::TypedValue(TypeContents::Integer(a)),
                Self::TypedValue(TypeContents::Integer(b)),
            ) => a.cmp(&b).into(),
            (
                Self::TypedValue(TypeContents::String(s)),
                Self::TypedValue(TypeContents::Integer(i)),
            ) => {
                if let Ok(i2) = s.parse::<IntegerStorage>() {
                    i2.cmp(i).into()
                } else {
                    s.cmp(&i.to_string()).into()
                }
            }
            (
                Self::TypedValue(TypeContents::Integer(i)),
                Self::TypedValue(TypeContents::String(s)),
            ) => {
                if let Ok(i2) = s.parse::<IntegerStorage>() {
                    i.cmp(&i2).into()
                } else {
                    i.to_string().cmp(s).into()
                }
            }
            (Self::TruthValue(t), _) => Self::TruthValue(*t).cast(&Type::Integer).compare(other),
            (_, Self::TruthValue(t)) => self.compare(&Self::TruthValue(*t).cast(&Type::Integer)),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => Ok(()),
            Value::TypedValue(t) => write!(f, "{}", t),
            Value::TruthValue(t) => write!(
                f,
                "{}",
                match t {
                    TruthValue::True => "1",
                    TruthValue::False => "0",
                    TruthValue::Unknown => "",
                }
            ),
        }
    }
}

impl From<TypeContents> for Value {
    fn from(contents: TypeContents) -> Self {
        Self::TypedValue(contents)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl From<IntegerStorage> for TruthValue {
    fn from(i: IntegerStorage) -> Self {
        match i {
            0 => TruthValue::False,
            _ => TruthValue::True,
        }
    }
}

impl From<&TypeContents> for TruthValue {
    fn from(contents: &TypeContents) -> Self {
        IntegerStorage::from(contents).into()
    }
}

impl From<&Value> for TruthValue {
    fn from(value: &Value) -> Self {
        value.get_truth()
    }
}

impl From<bool> for TruthValue {
    fn from(b: bool) -> Self {
        match b {
            true => TruthValue::True,
            false => TruthValue::False,
        }
    }
}

impl From<TruthValue> for Value {
    fn from(t: TruthValue) -> Self {
        Value::TruthValue(t)
    }
}
