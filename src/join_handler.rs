use std::{
    collections::{HashMap, HashSet},
};

use auto_enums::auto_enum;

use crate::{
    ast::{BinaryOp, ColumnName, ComparisonOp, JoinConstraint, JoinOperator, TableJoins},
    data_types::Value,
    error::{Error, ExecutionError, Result},
    interpreter::{evaluate_expression, resolve_expression, Interpreter},
    resolved_expression::{Expression, ResolvedColumn},
    storage::Columns,
    table_handler::{TableHandler, TableIter, TableRow},
    GetData, TableColumns,
};

pub enum JoinHandler {
    Join(Join),
    Empty,
}

impl JoinHandler {
    pub fn new(interpreter: &Interpreter, joins: Option<TableJoins>) -> Result<Self> {
        Ok(if let Some(joins) = joins {
            JoinHandler::Join(Join::new(interpreter, joins)?)
        } else {
            JoinHandler::Empty
        })
    }

    pub fn all_column_names(&self) -> Result<impl Iterator<Item = ResolvedColumn> + '_> {
        match self {
            Self::Join(join) => Ok(join.wildcard_column_names()),
            Self::Empty => Err(ExecutionError::NoTables.into()),
        }
    }

    pub fn column_names(
        &self,
        table_name: &str,
    ) -> Result<impl Iterator<Item = ResolvedColumn> + '_> {
        match self {
            JoinHandler::Join(join) => join.column_names(table_name),
            JoinHandler::Empty => Err(ExecutionError::NoTables.into()),
        }
    }

    pub fn iter(&self, filter: Option<Expression>) -> Result<JoinHandlerIter> {
        Ok(match self {
            JoinHandler::Join(join) => JoinHandlerIter::Iter(join.iter(filter)?),
            JoinHandler::Empty => JoinHandlerIter::None(false),
        })
    }
}

impl TableColumns for JoinHandler {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        match self {
            Self::Join(join) => join.resolve_name(name),
            Self::Empty => Err(ExecutionError::NoColumn(name.to_string()).into()),
        }
    }
}

pub enum JoinHandlerIter<'a> {
    Iter(JoinIter<'a>),
    None(bool),
}

impl<'a> JoinHandlerIter<'a> {
    pub fn get_next(&mut self) -> Result<Option<RowValue<'_>>> {
        match self {
            JoinHandlerIter::Iter(iter) => iter.get_next(),
            JoinHandlerIter::None(finished) => {
                if !*finished {
                    *finished = true;
                    Ok(Some(RowValue::Empty))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

pub enum Join {
    Table(TableHandler<Columns, String>),
    Join {
        left: Box<Join>,
        right: Box<Join>,
        constraint: Option<Expression>,
        join_operator: JoinOperator,
        exclude_columns: HashSet<ResolvedColumn>,
    },
}

impl TableColumns for Join {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        match self {
            Join::Table(table) => table.resolve_name(name),
            Join::Join { left, right, .. } => {
                let left_resolved = left.resolve_name(name.clone());
                let right_resolved = right.resolve_name(name);
                match (left_resolved, right_resolved) {
                    (Ok(l), Ok(_)) => {
                        Err(ExecutionError::AmbiguousName(l.take_column_name()).into())
                    }
                    (Ok(left), Err(_)) => Ok(left),
                    (Err(_), Ok(right)) => Ok(right),
                    (Err(e), Err(_)) => Err(e),
                }
            }
        }
    }
}

impl Join {
    pub fn new(interpreter: &Interpreter, joins: TableJoins) -> Result<Self> {
        Ok(match joins {
            TableJoins::Table(table_name) => {
                Self::Table(interpreter.open_table(table_name.name, table_name.alias)?)
            }
            TableJoins::Join {
                left,
                right,
                operator,
                constraint,
            } => {
                let left = Box::new(Join::new(interpreter, *left)?);
                let right = Box::new(Join::new(interpreter, *right)?);
                let (constraint, exclude_columns) = match constraint {
                    JoinConstraint::On(constraint) => (
                        Some(resolve_expression(
                            constraint,
                            &(left.as_ref(), right.as_ref()),
                        )?),
                        HashSet::new(),
                    ),
                    JoinConstraint::Natural => {
                        let mut left_columns: HashMap<_, _> = left
                            .wildcard_column_names()
                            .map(|c| {
                                let (t, c) = c.destructure();
                                (c, t)
                            })
                            .collect();
                        let mut exclude_columns = HashSet::new();
                        let mut constraint = Expression::Value(1.into());
                        for right_column in right.wildcard_column_names() {
                            if let Some((left_column, left_table)) =
                                left_columns.remove_entry(right_column.column_name())
                            {
                                exclude_columns.insert(right_column.clone());
                                let equals = Expression::BinaryOp(
                                    Box::new(Expression::Identifier(ResolvedColumn::new(
                                        left_table,
                                        left_column,
                                    ))),
                                    BinaryOp::Comparison(ComparisonOp::Eq),
                                    Box::new(Expression::Identifier(right_column)),
                                );
                                constraint = Expression::BinaryOp(
                                    Box::new(constraint),
                                    BinaryOp::And,
                                    Box::new(equals),
                                )
                            }
                        }
                        (Some(constraint), exclude_columns)
                    }
                    JoinConstraint::Using(columns) => {
                        let mut exclude_columns = HashSet::new();
                        let mut constraint = Expression::Value(1.into());
                        for column_name in columns {
                            if let (Some(left_table), Some(right_table)) = (
                                left.table_for_column(&column_name),
                                right.table_for_column(&column_name),
                            ) {
                                exclude_columns.insert(ResolvedColumn::new(
                                    right_table.to_owned(),
                                    column_name.clone(),
                                ));
                                let equals = Expression::BinaryOp(
                                    Box::new(Expression::Identifier(ResolvedColumn::new(
                                        left_table.to_owned(),
                                        column_name.clone(),
                                    ))),
                                    BinaryOp::Comparison(ComparisonOp::Eq),
                                    Box::new(Expression::Identifier(ResolvedColumn::new(
                                        right_table.to_owned(),
                                        column_name,
                                    ))),
                                );
                                constraint = Expression::BinaryOp(
                                    Box::new(constraint),
                                    BinaryOp::And,
                                    Box::new(equals),
                                )
                            } else {
                                return Err(ExecutionError::NoColumn(column_name).into());
                            }
                        }
                        (Some(constraint), exclude_columns)
                    }
                    JoinConstraint::None => (None, HashSet::new()),
                };
                Self::Join {
                    left,
                    right,
                    join_operator: operator,
                    constraint,
                    exclude_columns,
                }
            }
        })
    }

    fn table_for_column(&self, column_name: &str) -> Option<&str> {
        match self {
            Join::Table(t) => t
                .contains_column(column_name)
                .then(|| t.aliased_table_name()),
            Join::Join { left, right, .. } => left
                .table_for_column(column_name)
                .or_else(|| right.table_for_column(column_name)),
        }
    }

    #[auto_enum(Iterator)]
    pub fn wildcard_column_names(&self) -> impl Iterator<Item = ResolvedColumn> + '_ {
        match self {
            Join::Table(t) => t
                .column_names()
                .map(move |c| ResolvedColumn::new(t.aliased_table_name().to_owned(), c.to_owned())),
            Join::Join {
                left,
                right,
                exclude_columns,
                ..
            } => Box::new(
                left.wildcard_column_names().chain(
                    right
                        .wildcard_column_names()
                        .filter(move |c| !exclude_columns.contains(c)),
                ),
            ) as Box<dyn Iterator<Item = _>>,
        }
    }

    pub fn column_names<'a>(
        &'a self,
        table_name: &str,
    ) -> Result<Box<dyn Iterator<Item = ResolvedColumn> + 'a>> {
        match self {
            Join::Table(t) => {
                if t.aliased_table_name() == table_name {
                    Ok(Box::new(t.column_names().map(move |c| {
                        ResolvedColumn::new(t.aliased_table_name().to_owned(), c.to_owned())
                    })))
                } else {
                    Err(Error::Internal(format!(
                        "table name mismatch: this is {}, was given {}",
                        t.aliased_table_name(),
                        table_name
                    )))
                }
            }
            Join::Join { left, right, .. } => {
                if left.has_table(table_name) {
                    left.column_names(table_name)
                } else if right.has_table(table_name) {
                    right.column_names(table_name)
                } else {
                    Err(ExecutionError::NoTable(table_name.to_owned()).into())
                }
            }
        }
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        match self {
            Join::Table(table) => table.aliased_table_name() == table_name,
            Join::Join { left, right, .. } => {
                left.has_table(table_name) || right.has_table(table_name)
            }
        }
    }

    fn num_tables(&self) -> usize {
        match self {
            Join::Table(_) => 1,
            Join::Join { left, right, .. } => left.num_tables() + right.num_tables(),
        }
    }

    pub fn iter(&self, filter: Option<Expression>) -> Result<JoinIter<'_>> {
        let inner = self.iter_inner()?;
        let len = self.num_tables();
        Ok(JoinIter::new(inner, len, filter))
    }

    fn iter_inner(&self) -> Result<JoinIterInner<'_>> {
        Ok(match self {
            Join::Table(table) => JoinIterInner::Table(table.iter(), table),
            Join::Join {
                left,
                right,
                constraint,
                join_operator,
                ..
            } => {
                let left_len = left.num_tables();
                let left = Box::new(left.iter_inner()?);
                let right = Box::new(right.iter_inner()?);
                let constraint = constraint
                    .as_ref()
                    .ok_or(ExecutionError::NoConstraintOnJoin);
                let join_type = match join_operator {
                    JoinOperator::Inner => JoinType::Inner {
                        constraint: constraint.ok(),
                        initialise: true,
                    },
                    JoinOperator::Left => JoinType::Left {
                        constraint: constraint?,
                        advance_left: true,
                        right_has_yielded: false,
                    },
                    JoinOperator::Right => JoinType::Right {
                        constraint: constraint?,
                        advance_right: true,
                        left_has_yielded: false,
                    },
                };
                JoinIterInner::Join {
                    left,
                    right,
                    left_len,
                    join_type,
                }
            }
        })
    }
}

enum JoinIterInner<'a> {
    Table(TableIter, &'a TableHandler<Columns, String>),
    Join {
        left: Box<JoinIterInner<'a>>,
        right: Box<JoinIterInner<'a>>,
        left_len: usize,
        join_type: JoinType<'a>,
    },
}

enum JoinType<'a> {
    Inner {
        constraint: Option<&'a Expression>,
        initialise: bool,
    },
    Left {
        constraint: &'a Expression,
        advance_left: bool,
        right_has_yielded: bool,
    },
    Right {
        constraint: &'a Expression,
        advance_right: bool,
        left_has_yielded: bool,
    },
}

pub struct JoinIter<'a> {
    inner: JoinIterInner<'a>,
    buffer: Vec<TableRow>,
    filter: Option<Expression>,
    finished: bool,
}

impl<'a> JoinIter<'a> {
    fn new(inner: JoinIterInner<'a>, len: usize, filter: Option<Expression>) -> Self {
        Self {
            inner,
            buffer: vec![Default::default(); len],
            filter,
            finished: false,
        }
    }

    fn advance(&mut self) -> Result<bool> {
        if self.finished {
            return Ok(false);
        }
        if !self.inner.advance(self.buffer.as_mut_slice())? {
            self.finished = true;
            return Ok(false);
        }

        Ok(true)
    }

    pub fn get_next(&mut self) -> Result<Option<RowValue<'_>>> {
        while self.advance()? {
            if let Some(filter) = &self.filter {
                if evaluate_expression(filter, &(&self.inner, self.buffer.as_slice()))?.is_true() {
                    return Ok(Some(RowValue::new(self.buffer.as_slice())));
                }
            } else {
                return Ok(Some(RowValue::new(self.buffer.as_slice())));
            }
        }
        Ok(None)
    }
}

impl<'a> JoinIterInner<'a> {
    fn advance(&mut self, buffer: &mut [TableRow]) -> Result<bool> {
        match self {
            JoinIterInner::Table(iter, handler) => {
                assert!(buffer.len() == 1);
                Ok(if let Some(next) = iter.get_next()? {
                    buffer[0] = next;
                    true
                } else {
                    buffer[0] = TableRow::default();
                    *iter = handler.iter();
                    false
                })
            }
            JoinIterInner::Join {
                left,
                right,
                left_len,
                join_type,
            } => loop {
                let (left_buffer, right_buffer) = buffer.split_at_mut(*left_len);
                match join_type {
                    JoinType::Inner {
                        constraint,
                        initialise,
                    } => {
                        if *initialise && !left.advance(left_buffer)? {
                            return Ok(false);
                        }
                        if !right.advance(right_buffer)? {
                            if *initialise {
                                return Ok(false);
                            }
                            right.next_reset(right_buffer)?;
                            if !left.advance(left_buffer)? {
                                return Ok(false);
                            }
                        }
                        *initialise = false;
                        if let Some(constraint) = constraint {
                            if evaluate_expression(
                                constraint,
                                &(left.as_ref(), right.as_ref(), &*buffer),
                            )?
                            .is_true()
                            {
                                return Ok(true);
                            }
                        } else {
                            return Ok(true);
                        }
                    }
                    JoinType::Left {
                        constraint,
                        advance_left,
                        right_has_yielded,
                    } => {
                        if *advance_left {
                            *advance_left = false;
                            if !left.advance(left_buffer)? {
                                return Ok(false);
                            }
                        }
                        if *right_has_yielded {
                            if !right.advance(right_buffer)? {
                                right.next_reset(right_buffer)?;
                                *right_has_yielded = false;
                                if !left.advance(left_buffer)? {
                                    return Ok(false);
                                }
                            }
                            if evaluate_expression(
                                constraint,
                                &(left.as_ref(), right.as_ref(), &*buffer),
                            )?
                            .is_true()
                            {
                                *right_has_yielded = true;
                                return Ok(true);
                            }
                        } else if right.advance(right_buffer)? {
                            if evaluate_expression(
                                constraint,
                                &(left.as_ref(), right.as_ref(), &*buffer),
                            )?
                            .is_true()
                            {
                                *right_has_yielded = true;
                                return Ok(true);
                            }
                        } else {
                            *advance_left = true;
                            return Ok(true);
                        }
                    }
                    JoinType::Right {
                        constraint,
                        advance_right,
                        left_has_yielded,
                    } => {
                        if *advance_right {
                            *advance_right = false;
                            if !right.advance(right_buffer)? {
                                return Ok(false);
                            }
                        }
                        if *left_has_yielded {
                            if !left.advance(left_buffer)? {
                                left.next_reset(left_buffer)?;
                                *left_has_yielded = false;
                                if !right.advance(right_buffer)? {
                                    return Ok(false);
                                }
                            }
                            if evaluate_expression(
                                constraint,
                                &(left.as_ref(), right.as_ref(), &*buffer),
                            )?
                            .is_true()
                            {
                                *left_has_yielded = true;
                                return Ok(true);
                            }
                        } else if left.advance(left_buffer)? {
                            if evaluate_expression(
                                constraint,
                                &(left.as_ref(), right.as_ref(), &*buffer),
                            )?
                            .is_true()
                            {
                                *left_has_yielded = true;
                                return Ok(true);
                            }
                        } else {
                            *advance_right = true;
                            return Ok(true);
                        }
                    }
                }
            },
        }
    }

    fn reset(&mut self, buffer: &mut [TableRow]) {
        match self {
            JoinIterInner::Table(iter, handler) => {
                assert!(buffer.len() == 1);
                *iter = handler.iter();
                buffer[0] = TableRow::default();
            }
            JoinIterInner::Join {
                left,
                right,
                left_len,
                ..
            } => {
                let (left_buffer, right_buffer) = buffer.split_at_mut(*left_len);
                left.reset(left_buffer);
                right.reset(right_buffer);
            }
        }
    }

    fn next_reset(&mut self, buffer: &mut [TableRow]) -> Result<bool> {
        self.reset(buffer);
        self.advance(buffer)
    }

    fn has_table(&self, table_name: &str) -> bool {
        match self {
            JoinIterInner::Table(_, handler) => handler.aliased_table_name() == table_name,
            JoinIterInner::Join { left, right, .. } => {
                left.has_table(table_name) || right.has_table(table_name)
            }
        }
    }

    fn num_tables(&self) -> usize {
        match self {
            JoinIterInner::Table(_, _) => 1,
            JoinIterInner::Join {
                right, left_len, ..
            } => left_len + right.num_tables(),
        }
    }
}

pub enum RowValue<'a> {
    Data(&'a [TableRow]),
    Empty,
}

impl<'a> RowValue<'a> {
    fn new(row: &'a [TableRow]) -> Self {
        Self::Data(row)
    }
}

impl<'a> GetData for (&'a JoinIterInner<'a>, &'a [TableRow]) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (handler, row) = self;
        match handler {
            JoinIterInner::Table(_, handler) => {
                assert!(row.len() == 1);
                (*handler, &row[0]).get_data(column_name)
            }
            JoinIterInner::Join {
                left,
                right,
                left_len,
                ..
            } => {
                let (left_row, right_row) = row.split_at(*left_len);
                let table_name = column_name.table_name();
                if left.has_table(table_name) {
                    (left.as_ref(), left_row).get_data(column_name)
                } else if right.has_table(table_name) {
                    (right.as_ref(), right_row).get_data(column_name)
                } else {
                    Err(Error::Internal(format!(
                        "Neither side has table {}",
                        table_name
                    )))
                }
            }
        }
    }
}

impl<'a> GetData for (&'a Join, &'a [TableRow]) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (handler, row) = self;
        match handler {
            Join::Table(handler) => {
                assert!(row.len() == 1);
                (handler, &row[0]).get_data(column_name)
            }
            Join::Join { left, right, .. } => {
                let left_len = left.num_tables();
                let (left_row, right_row) = row.split_at(left_len);
                let table_name = column_name.table_name();
                if left.has_table(table_name) {
                    (left.as_ref(), left_row).get_data(column_name)
                } else if right.has_table(table_name) {
                    (right.as_ref(), right_row).get_data(column_name)
                } else {
                    Err(Error::Internal(format!(
                        "Neither side has table {}",
                        table_name
                    )))
                }
            }
        }
    }
}

impl<'a> GetData for (&'a JoinHandler, &'a RowValue<'a>) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (handler, row) = self;
        match (handler, row) {
            (JoinHandler::Join(j), RowValue::Data(d)) => (j, *d).get_data(column_name),
            _ => Err(Error::Internal("Getdata on invalid".to_owned())),
        }
    }
}

impl<'a> GetData for (&'a JoinIterInner<'a>, &'a JoinIterInner<'a>, &'a [TableRow]) {
    fn get_data(&self, column_name: &ResolvedColumn) -> Result<Value> {
        let (left, right, buffer) = self;
        let table_name = column_name.table_name();
        let left_len = left.num_tables();
        let (left_buffer, right_buffer) = buffer.split_at(left_len);
        if left.has_table(table_name) {
            (*left, left_buffer).get_data(column_name)
        } else if right.has_table(table_name) {
            (*right, right_buffer).get_data(column_name)
        } else {
            Err(Error::Internal(format!(
                "Neither side has table {}",
                table_name
            )))
        }
    }
}

impl TableColumns for (&Join, &Join) {
    fn resolve_name(&self, name: ColumnName) -> Result<ResolvedColumn> {
        let (left, right) = self;
        let left_resolved = left.resolve_name(name.clone());
        let right_resolved = right.resolve_name(name);
        match (left_resolved, right_resolved) {
            (Ok(l), Ok(_)) => Err(ExecutionError::AmbiguousName(l.destructure().1).into()),
            (Ok(left), Err(_)) => Ok(left),
            (Err(_), Ok(right)) => Ok(right),
            (Err(e), Err(_)) => Err(e),
        }
    }
}
