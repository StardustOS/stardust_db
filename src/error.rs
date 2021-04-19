use crate::data_types::Type;
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Sled error: {0}")]
    Storage(#[from] sled::Error),
    #[error("{0}")]
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
    #[error("NOT NULL constraint `{0}` failed")]
    NullConstraintFailed(String),
    #[error("UNIQUE constraint `{0}` failed")]
    UniqueConstraintFailed(String),
    #[error("CHECK constraint `{0}` failed")]
    CheckConstraintFailed(String),
    #[error("FOREIGN KEY constraint `{0}` failed")]
    ForeignKeyConstraintFailed(String),
    #[error("multiple primary keys for table `{0}`")]
    MultiplePrimaryKey(String),
    #[error("missing constraint for join")]
    NoConstraintOnJoin,
    #[error("no tables specified")]
    NoTables,
    #[error("duplicate table name or alias `{0}`")]
    DuplicateTableName(String),
    #[error("incorrect number of columns referred to in foreign key. Expected {expected}, found {found}")]
    IncorrectNumForeignKeyReferredColumns { expected: usize, found: usize },
    #[error("incorrect type for column `{this_column_name}` found on referred column `{referred_column_name}`. Expected `{this_column_type}`, found `{referred_column_type}`")]
    IncorrectForeignKeyReferredColumnType {
        this_column_name: String,
        referred_column_name: String,
        this_column_type: Type,
        referred_column_type: Type,
    },
    #[error("parent table `{parent_table}` has foreign key dependency `{key_name}`")]
    ForeignKeyDependencyDelete {
        parent_table: String,
        key_name: String,
    },
    #[error("FOREIGN KEY constraint `{0}` does not refer to unique columns")]
    ForeignKeyNotUnique(String),
}
