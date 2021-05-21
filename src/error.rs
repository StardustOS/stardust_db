use crate::data_types::Type;
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

/// The top-level Error enum.
#[derive(Error, Debug)]
pub enum Error {
    /// An error originating in `sled`.
    #[error("Sled error: {0}")]
    Storage(#[from] sled::Error),
    /// An SQL parse error.
    #[error("{0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    /// An error caused by an invalid query.
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    /// An error in from the `bincode` encoder/decoder.
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    /// An error caused by a bug in the database system.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// An error caused by an invalid query.
#[derive(Error, Debug)]
pub enum ExecutionError {
    /// The table already exists.
    #[error("table `{0}` already exists")]
    TableExists(String),
    /// The column has already been defined.
    #[error("column `{0}` already exists")]
    ColumnExists(String),
    /// The table doesn't exist.
    #[error("table `{0}` doesn't exist")]
    NoTable(String),
    /// The value is the wrong type for the column.
    #[error("incorrect type when constructing column `{column}`. Expected `{expected_type}`, found `{actual_type}`")]
    TypeError {
        column: String,
        expected_type: Type,
        actual_type: Type,
    },
    /// The wrong number of column values were specified.
    #[error("incorrect number of columns. Expected {expected}, found {actual}")]
    WrongNumColumns { expected: usize, actual: usize },
    /// The column doesn't exist.
    #[error("no column named {0}")]
    NoColumn(String),
    /// The column exists in multiple referenced tables.
    #[error("ambiguous column name: {0}")]
    AmbiguousName(String),
    /// A NOT NULL constraint failed.
    #[error("NOT NULL constraint `{0}` failed")]
    NullConstraintFailed(String),
    /// A UNIQUE constraint failed.
    #[error("UNIQUE constraint `{0}` failed")]
    UniqueConstraintFailed(String),
    /// A CHECK constraint failed.
    #[error("CHECK constraint `{0}` failed")]
    CheckConstraintFailed(String),
    /// A FOREIGN KEY constraint failed.
    #[error("FOREIGN KEY constraint `{0}` failed")]
    ForeignKeyConstraintFailed(String),
    /// Multiple primary keys were defined.
    #[error("multiple primary keys for table `{0}`")]
    MultiplePrimaryKey(String),
    /// Multiple default values were defined.
    #[error("multiple default values for column `{0}`")]
    MultipleDefault(String),
    /// Multiple NOT NULL constriaints were specified for a column.
    #[error("multiple NOT NULL constraints for column `{0}`")]
    MultipleNotNull(String),
    /// Multiple UNIQUE constriaints were specified for a column.
    #[error("multiple UNIQUE constraints for column `{0}`")]
    MultipleUnique(String),
    /// The Join has no constraint,
    #[error("missing constraint for join")]
    NoConstraintOnJoin,
    /// No tables were specified.
    #[error("no tables specified")]
    NoTables,
    /// The table name was duplicated.
    #[error("duplicate table name or alias `{0}`")]
    DuplicateTableName(String),
    /// The wrong number of referred columns in a foreign key constraint were specified.
    #[error("incorrect number of columns referred to in foreign key. Expected {expected}, found {found}")]
    IncorrectNumForeignKeyReferredColumns { expected: usize, found: usize },
    /// The wrong type for a foreign key constraint was specified.
    #[error("incorrect type for column `{this_column_name}` found on referred column `{referred_column_name}`. Expected `{this_column_type}`, found `{referred_column_type}`")]
    IncorrectForeignKeyReferredColumnType {
        this_column_name: String,
        referred_column_name: String,
        this_column_type: Type,
        referred_column_type: Type,
    },
    /// The table could not be deleted as it is referred to in a foreign key constraint.
    #[error("parent table `{parent_table}` has foreign key dependency `{key_name}`")]
    ForeignKeyDependencyDelete {
        parent_table: String,
        key_name: String,
    },
    /// The foreign key referred columns are not unique.
    #[error("FOREIGN KEY constraint `{0}` does not refer to unique columns")]
    ForeignKeyNotUnique(String),
}
