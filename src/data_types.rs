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
            Type::String => TypeContents::String(String::from_utf8(data.into()).map_err(|e| {
                Error::Internal(format!(
                    "Could not decode bytes to string: {:?}, e: {}",
                    data, e
                ))
            })?),
        })
    }

    pub fn get_contents_from_string(&self, data: String) -> TypeContents {
        match self {
            Type::Integer => TypeContents::Integer(string_to_int(&data)),
            Type::String => TypeContents::String(data),
        }
    }

    pub fn resolve_value(&self, data: Value) -> Option<TypeContents> {
        match data {
            Value::Null => None,
            Value::TypedValue(t) => Some(t.cast(self)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
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

    pub fn is_true(&self) -> bool {
        IntegerStorage::from(self) > 0
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
            TypeContents::String(s) => string_to_int(s),
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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Value {
    Null,
    TypedValue(TypeContents),
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Comparison {
    Unknown,
    LessThan,
    Equal,
    GreaterThan,
}

impl Comparison {
    pub fn get_value(&self, op: &ComparisonOp) -> Value {
        if matches!(self, Self::Unknown) {
            return Value::Null;
        }
        match op {
            ComparisonOp::Eq => matches!(self, Self::Equal).into(),
            ComparisonOp::NotEq => matches!(self, Self::GreaterThan | Self::LessThan).into(),
            ComparisonOp::Gt => matches!(self, Self::GreaterThan).into(),
            ComparisonOp::Lt => matches!(self, Self::LessThan).into(),
            ComparisonOp::GtEq => matches!(self, Self::Equal | Self::GreaterThan).into(),
            ComparisonOp::LtEq => matches!(self, Self::Equal | Self::LessThan).into(),
        }
    }

    pub fn is_equal(&self) -> bool {
        matches!(self, Comparison::Equal)
    }

    pub fn is_equal_or_null(&self) -> bool {
        matches!(self, Comparison::Equal | Comparison::Unknown)
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
        }
    }

    pub fn cast_string(&self) -> Option<Cow<str>> {
        match self {
            Self::Null => None,
            Self::TypedValue(TypeContents::String(s)) => Some(Cow::Borrowed(s.as_str())),
            Self::TypedValue(TypeContents::Integer(i)) => Some(Cow::Owned(i.to_string())),
        }
    }

    pub fn cast_int(&self) -> Option<IntegerStorage> {
        match self {
            Self::Null => None,
            Self::TypedValue(TypeContents::String(s)) => Some(string_to_int(s)),
            Self::TypedValue(TypeContents::Integer(i)) => Some(*i),
        }
    }

    pub fn assume_string(self) -> Result<String> {
        match self {
            Self::TypedValue(TypeContents::String(s)) => Ok(s),
            v => Err(Error::Internal(format!("Assssumed string, got {}", v))),
        }
    }

    pub fn assume_integer(self) -> Result<IntegerStorage> {
        match self {
            Self::TypedValue(TypeContents::Integer(i)) => Ok(i),
            v => Err(Error::Internal(format!("Assssumed integer, got {}", v))),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    fn get_truth(&self) -> TruthValue {
        match self {
            Value::Null => TruthValue::Unknown,
            Value::TypedValue(contents) => contents.is_true().into(),
        }
    }

    pub fn is_true(&self) -> bool {
        match self {
            Value::Null => false,
            Value::TypedValue(t) => t.is_true(),
        }
    }

    pub fn and(&self, other: &Self) -> Self {
        self.get_truth().and(other.get_truth()).into()
    }

    pub fn or(&self, other: &Self) -> Self {
        self.get_truth().or(other.get_truth()).into()
    }

    pub fn equals_or_null(&self, other: &Value) -> bool {
        self.compare(other).is_equal_or_null()
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
            ) => string_to_int(s).cmp(i).into(),
            (
                Self::TypedValue(TypeContents::Integer(i)),
                Self::TypedValue(TypeContents::String(s)),
            ) => i.cmp(&string_to_int(s)).into(),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::TypedValue(t) => write!(f, "{}", t),
        }
    }
}

impl From<TypeContents> for Value {
    fn from(contents: TypeContents) -> Self {
        Self::TypedValue(contents)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::TypedValue(TypeContents::String(s))
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::TypedValue(TypeContents::String(s.to_owned()))
    }
}

impl From<IntegerStorage> for Value {
    fn from(i: IntegerStorage) -> Self {
        Self::TypedValue(TypeContents::Integer(i))
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        match b {
            true => Self::TypedValue(TypeContents::Integer(1)),
            false => Self::TypedValue(TypeContents::Integer(0)),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum TruthValue {
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
        match t {
            TruthValue::True => 1.into(),
            TruthValue::False => 0.into(),
            TruthValue::Unknown => Self::Null,
        }
    }
}

fn string_to_int(string: &str) -> IntegerStorage {
    let mut result: IntegerStorage = 0;
    let string = string.trim_start();
    let mut characters = string.chars().peekable();
    let sign = match characters.peek() {
        Some('-') => {
            let _ = characters.next();
            true
        }
        Some(_) => false,
        None => return 0,
    };
    for character in characters {
        match character {
            '0'..='9' => {
                result = match result.checked_mul(10).and_then(|result| {
                    result.checked_add(character as IntegerStorage - '0' as IntegerStorage)
                }) {
                    Some(result) => result,
                    None => {
                        return if sign {
                            IntegerStorage::MIN
                        } else {
                            IntegerStorage::MAX
                        }
                    }
                }
            }
            _ => break,
        }
    }
    if sign {
        -result
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::data_types::{string_to_int, Comparison, Value};

    #[test]
    fn test_string_to_int() {
        assert_eq!(string_to_int("0"), 0);
        assert_eq!(string_to_int("1"), 1);
        assert_eq!(string_to_int("10"), 10);
        assert_eq!(string_to_int("01"), 1);
        assert_eq!(string_to_int("010"), 10);
        assert_eq!(string_to_int("-0"), 0);
        assert_eq!(string_to_int("-1"), -1);
        assert_eq!(string_to_int("-10"), -10);
        assert_eq!(string_to_int("-01"), -1);
        assert_eq!(string_to_int("-010"), -10);

        assert_eq!(string_to_int("hello"), 0);
        assert_eq!(string_to_int("0hello"), 0);
        assert_eq!(string_to_int("1hello"), 1);
        assert_eq!(string_to_int("10hello"), 10);
        assert_eq!(string_to_int("10hello10"), 10);
        assert_eq!(string_to_int("0101hello"), 101);
        assert_eq!(string_to_int("-hello"), 0);
        assert_eq!(string_to_int("-0hello"), 0);
        assert_eq!(string_to_int("-1hello"), -1);
        assert_eq!(string_to_int("-10hello"), -10);
        assert_eq!(string_to_int("-10hello10"), -10);
        assert_eq!(string_to_int("-0101hello"), -101);
        assert_eq!(
            string_to_int("123123123123123123123123123"),
            9223372036854775807
        );
        assert_eq!(
            string_to_int("-123123123123123123123123123"),
            -9223372036854775808
        );
    }

    #[test]
    fn test_comparisons() {
        assert_eq!(Value::Null.compare(&Value::Null), Comparison::Unknown);
        assert_eq!(Value::Null.compare(&5.into()), Comparison::Unknown);
        assert_eq!(Value::Null.compare(&"Hello".into()), Comparison::Unknown);
        assert_eq!(Value::from(5).compare(&Value::Null), Comparison::Unknown);
        assert_eq!(
            Value::from("Hello").compare(&Value::Null),
            Comparison::Unknown
        );

        assert_eq!(Value::from(5).compare(&5.into()), Comparison::Equal);
        assert_eq!(Value::from(0).compare(&5.into()), Comparison::LessThan);
        assert_eq!(Value::from(5).compare(&0.into()), Comparison::GreaterThan);

        assert_eq!(
            Value::from("hello").compare(&"hello".into()),
            Comparison::Equal
        );
        assert_eq!(
            Value::from("HELLO").compare(&"hello".into()),
            Comparison::LessThan
        );
        assert_eq!(
            Value::from("hello").compare(&"HELLO".into()),
            Comparison::GreaterThan
        );

        assert_eq!(Value::from(5).compare(&"5".into()), Comparison::Equal);
        assert_eq!(
            Value::from("HELLO").compare(&5.into()),
            Comparison::LessThan
        );
        assert_eq!(
            Value::from(5).compare(&"HELLO".into()),
            Comparison::GreaterThan
        );
    }
}
