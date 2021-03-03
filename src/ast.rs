use crate::{data_types::Type, storage::ColumnName};
use std::fmt::Formatter;

#[derive(Debug)]
pub enum SqlQuery {
    CreateTable(CreateTable),
    Insert(Insert),
    SelectQuery(SelectQuery),
    DropTable(DropTable),
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<Column>,
}

impl CreateTable {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: Type,
    pub default: Option<Expression>,
}

impl Column {
    pub fn new(name: String, data_type: Type, default: Option<Expression>) -> Self {
        Self {
            name,
            data_type,
            default,
        }
    }
}

#[derive(Debug)]
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: SelectQuery,
}

impl Insert {
    pub fn new(table: String, columns: Option<Vec<String>>, values: SelectQuery) -> Self {
        Self {
            table,
            columns,
            values,
        }
    }
}

#[derive(Debug)]
pub enum SelectQuery {
    Values(Values),
    Select(SelectContents),
}

#[derive(Debug)]
pub struct Values {
    pub rows: Vec<Vec<Expression>>,
}

impl Values {
    pub fn new(rows: Vec<Vec<Expression>>) -> Self {
        Self { rows }
    }
}

#[derive(Debug)]
pub enum Expression {
    Literal(String),
    Identifier(ColumnName),
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Literal(s) => write!(f, "{}", s),
            Expression::Identifier(i) => write!(f, "{}", i)
        }
    }
}

impl Expression {
    pub fn to_column_name(&self) -> ColumnName {
        match self {
            Expression::Identifier(c) => c.clone(),
            Expression::Literal(s) => ColumnName::new(None, s.to_string())
        }
    }
}

#[derive(Debug)]
pub struct SelectContents {
    pub projections: Vec<Projection>,
    pub from: TableJoins,
}

impl SelectContents {
    pub fn new(projections: Vec<Projection>, from: TableJoins) -> Self {
        Self { projections, from }
    }
}

#[derive(Debug)]
pub enum Projection {
    Wildcard,
    Unaliased(Expression),
    Aliased(Expression, String)
}

#[derive(Debug)]
pub struct TableJoins {
    pub tables: String,
}

impl TableJoins {
    pub fn new(tables: String) -> Self {
        Self { tables }
    }
}

#[derive(Debug)]
pub struct DropTable {
    pub names: Vec<String>,
}

impl DropTable {
    pub fn new(names: Vec<String>) -> Self {
        Self { names }
    }
}
