use crate::{
    data_types::{Type, Value},
    storage::ColumnName,
};
use std::fmt::{Debug, Formatter};

#[derive(Debug)]
pub enum SqlQuery {
    CreateTable(CreateTable),
    Insert(Insert),
    SelectQuery(SelectQuery),
    DropTable(DropTable),
    Delete(Delete),
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
    pub not_null: bool,
    pub unique: bool,
}

impl Column {
    pub fn new(
        name: String,
        data_type: Type,
        default: Option<Expression>,
        not_null: bool,
        unique: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            default,
            not_null,
            unique,
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
    Value(Value),
    Identifier(ColumnName),
    BinaryOp(Box<Expression>, BinaryOp, Box<Expression>),
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Value(v) => write!(f, "{}", v),
            Expression::Identifier(i) => write!(f, "{}", i),
            Expression::BinaryOp(l, op, r) => write!(f, "{} {} {}", l, op, r),
        }
    }
}

impl Expression {
    pub fn to_column_name(&self) -> ColumnName {
        match self {
            Expression::Identifier(c) => c.clone(),
            Expression::Value(v) => ColumnName::new(None, v.to_string()),
            Expression::BinaryOp(_, _, _) => ColumnName::new(None, self.to_string()),
        }
    }
}

#[derive(Debug)]
pub enum BinaryOp {
    And,
    Or,
    Comparison(ComparisonOp),
}

#[derive(Debug)]
pub enum ComparisonOp {
    Eq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Comparison(c) => std::fmt::Display::fmt(c, f),
        }
    }
}

impl std::fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ComparisonOp::Eq => "=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Lt => "<",
                ComparisonOp::GtEq => ">=",
                ComparisonOp::LtEq => "<=",
            }
        )
    }
}

#[derive(Debug)]
pub struct SelectContents {
    pub projections: Vec<Projection>,
    pub from: TableJoins,
    pub selection: Option<Expression>,
}

impl SelectContents {
    pub fn new(
        projections: Vec<Projection>,
        from: TableJoins,
        selection: Option<Expression>,
    ) -> Self {
        Self {
            projections,
            from,
            selection,
        }
    }
}

#[derive(Debug)]
pub enum Projection {
    Wildcard,
    QualifiedWildcard(String),
    Unaliased(Expression),
    Aliased(Expression, String),
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

#[derive(Debug)]
pub struct Delete {
    pub table_name: String,
    pub predicate: Option<Expression>,
}

impl Delete {
    pub fn new(table_name: String, predicate: Option<Expression>) -> Self {
        Self {
            table_name,
            predicate,
        }
    }
}
