use auto_enums::auto_enum;
use serde::{Deserialize, Serialize};

use crate::{
    data_types::{IntegerStorage, Type, Value},
    error::{Error, Result},
    storage::ColumnName,
};
use std::{
    convert::TryFrom,
    fmt::{Debug, Formatter},
};

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
    pub uniques: Vec<(Vec<usize>, String)>,
    pub primary_key: Option<(Vec<usize>, String)>,
    pub checks: Vec<(UnresolvedExpression, String)>,
    pub foreign_keys: Vec<ForeignKey>,
}

impl CreateTable {
    pub fn new(
        name: String,
        columns: Vec<Column>,
        uniques: Vec<(Vec<usize>, String)>,
        primary_key: Option<(Vec<usize>, String)>,
        checks: Vec<(UnresolvedExpression, String)>,
        foreign_keys: Vec<ForeignKey>,
    ) -> Self {
        Self {
            name,
            columns,
            uniques,
            primary_key,
            checks,
            foreign_keys,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
    pub on_delete: Option<ForeignKeyAction>,
    pub on_update: Option<ForeignKeyAction>,
}

impl ForeignKey {
    pub fn new(
        name: String,
        columns: Vec<String>,
        foreign_table: String,
        referred_columns: Vec<String>,
        on_delete: Option<ForeignKeyAction>,
        on_update: Option<ForeignKeyAction>,
    ) -> Self {
        Self {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ForeignKeyAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

impl From<ForeignKeyAction> for IntegerStorage {
    fn from(action: ForeignKeyAction) -> Self {
        match action {
            ForeignKeyAction::NoAction => 0,
            ForeignKeyAction::Cascade => 1,
            ForeignKeyAction::SetNull => 2,
            ForeignKeyAction::SetDefault => 3,
        }
    }
}

impl TryFrom<IntegerStorage> for ForeignKeyAction {
    type Error = Error;

    fn try_from(value: IntegerStorage) -> Result<Self> {
        Ok(match value {
            0 => ForeignKeyAction::NoAction,
            1 => ForeignKeyAction::Cascade,
            2 => ForeignKeyAction::SetNull,
            3 => ForeignKeyAction::SetDefault,
            v => {
                return Err(Error::Internal(format!(
                    "Incorrect number {} in TryFrom for ForeignKeyAction",
                    v
                )))
            }
        })
    }
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        Self::NoAction
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: Type,
    pub default: Option<UnresolvedExpression>,
    pub not_null: bool,
}

impl Column {
    pub fn new(
        name: String,
        data_type: Type,
        default: Option<UnresolvedExpression>,
        not_null: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            default,
            not_null,
        }
    }
}

#[derive(Debug)]
pub struct Insert {
    pub table: TableName,
    pub columns: Option<Vec<String>>,
    pub values: SelectQuery,
}

impl Insert {
    pub fn new(table: String, columns: Option<Vec<String>>, values: SelectQuery) -> Self {
        Self {
            table: TableName::new(table, None),
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
    pub rows: Vec<Vec<UnresolvedExpression>>,
}

impl Values {
    pub fn new(rows: Vec<Vec<UnresolvedExpression>>) -> Self {
        Self { rows }
    }
}

#[derive(Debug)]
pub enum UnresolvedExpression {
    Value(Value),
    Identifier(ColumnName),
    BinaryOp(
        Box<UnresolvedExpression>,
        BinaryOp,
        Box<UnresolvedExpression>,
    ),
}

impl std::fmt::Display for UnresolvedExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnresolvedExpression::Value(v) => write!(f, "{}", v),
            UnresolvedExpression::Identifier(i) => write!(f, "{}", i),
            UnresolvedExpression::BinaryOp(l, op, r) => write!(f, "{} {} {}", l, op, r),
        }
    }
}

impl UnresolvedExpression {
    pub fn to_column_name(&self) -> ColumnName {
        match self {
            UnresolvedExpression::Identifier(c) => c.clone(),
            UnresolvedExpression::Value(v) => ColumnName::new(None, v.to_string()),
            UnresolvedExpression::BinaryOp(_, _, _) => ColumnName::new(None, self.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinaryOp {
    And,
    Or,
    Comparison(ComparisonOp),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    NotEq,
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
                ComparisonOp::NotEq => "<>",
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
    pub from: Option<TableJoins>,
    pub selection: Option<UnresolvedExpression>,
}

impl SelectContents {
    pub fn new(
        projections: Vec<Projection>,
        from: Option<TableJoins>,
        selection: Option<UnresolvedExpression>,
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
    Unaliased(UnresolvedExpression),
    Aliased(UnresolvedExpression, String),
}

#[derive(Debug)]
pub enum TableJoins {
    Table(TableName),
    Join {
        left: Box<TableJoins>,
        right: Box<TableJoins>,
        operator: JoinOperator,
        constraint: JoinConstraint,
    },
}

impl TableJoins {
    pub fn contains_table(&self, table_name: &str) -> bool {
        match self {
            TableJoins::Table(name) => table_name == name.aliased_name(),
            TableJoins::Join { left, right, .. } => {
                left.contains_table(table_name) || right.contains_table(table_name)
            }
        }
    }

    #[auto_enum(Iterator)]
    pub fn table_names(&self) -> impl Iterator<Item = &str> + '_ {
        match self {
            TableJoins::Table(n) => std::iter::once(n.aliased_name()),
            TableJoins::Join { left, right, .. } => {
                Box::new(left.table_names().chain(right.table_names()))
                    as Box<dyn Iterator<Item = _>>
            }
        }
    }
}

#[derive(Debug)]
pub struct TableName {
    pub name: String,
    pub alias: Option<String>,
}

impl TableName {
    pub fn new(name: String, alias: Option<String>) -> Self {
        Self { name, alias }
    }

    pub fn aliased_name(&self) -> &str {
        self.alias.as_deref().unwrap_or_else(|| self.name.as_str())
    }
}

#[derive(Debug)]
pub enum JoinOperator {
    Inner,
    Left,
    Right,
}

#[derive(Debug)]
pub enum JoinConstraint {
    On(UnresolvedExpression),
    Natural,
    Using(Vec<String>),
    None,
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
    pub predicate: Option<UnresolvedExpression>,
}

impl Delete {
    pub fn new(table_name: String, predicate: Option<UnresolvedExpression>) -> Self {
        Self {
            table_name,
            predicate,
        }
    }
}
