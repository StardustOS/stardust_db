use crate::data_types::Type;
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Sled error: {0}")]
    Storage(#[from] sled::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("table `{0}` already exists")]
    TableExists(String),
    #[error("column `{0}` already exists")]
    ColumnExists(String),
    #[error("table `{0}` doesn't exist")]
    NoTable(String),
    #[error("missing data for `{0}` when constructing row")]
    NoData(String),
    #[error("incorrect type when constructing column `{column}`. Expected `{expected_type}`, found `{actual_type}`")]
    TypeError {
        column: String,
        expected_type: Type,
        actual_type: Type,
    },
    #[error("could not parse `{0}` as `{1}`: {2}")]
    ParseError(String, Type, String),
    #[error("incorrect number of columns. Expected {expected}, found {actual}")]
    WrongNumColumns { expected: usize, actual: usize },
    #[error("no column named {0}")]
    NoColumn(String),
    #[error("ambiguous column name: {0}")]
    AmbiguousName(String),
    #[error("NOT NULL constraint on column `{0}` failed")]
    NullConstraintFailed(String),
    #[error("UNIQUE constraint on column failed{0}")]
    UniqueConstraintFailed(String),
    #[error("multiple primary keys for table {0}")]
    MultiplePrimaryKey(String),
    #[error("missing constraint for join")]
    NoConstraintOnJoin,
    #[error("no tables specified")]
    NoTables,
}

