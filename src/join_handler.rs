use std::sync::Arc;

use crate::{
    ast::Expression,
    data_types::Value,
    error::{Error, ExecutionError, Result},
    interpreter::resolve_expression,
    storage::ColumnName,
    table_handler::{TableHandler, TableIter, TableRow},
    Row, TableColumns,
};
use indexmap::IndexMap;

#[derive(Default)]
pub struct JoinHandler {
    trees: IndexMap<Arc<str>, TableHandler>,
}

impl JoinHandler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            trees: IndexMap::with_capacity(capacity),
        }
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.trees
            .values()
            .flat_map(|handler| handler.column_names())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.trees.keys().map(|table| table.as_ref())
    }

    pub fn table_column_names(&self, table_name: &str) -> Result<impl Iterator<Item = &str>> {
        self.trees
            .get(table_name)
            .map(|handler| handler.column_names())
            .ok_or_else(|| Error::Internal(format!("No column names for table {}", table_name)))
    }

    pub fn add_tree(&mut self, handler: TableHandler) {
        self.trees.insert(handler.table_name(), handler);
    }

    pub fn contains_table(&self, table_name: &str) -> bool {
        self.trees.contains_key(table_name)
    }

    pub fn iter(&self) -> JoinIter {
        JoinIter::new(self.trees.values().map(|handler| handler.iter()).collect())
    }
}

pub struct JoinIter {
    iters: Vec<TableIter>,
    buffer: Option<Vec<TableRow>>,
    finished: bool,
}

impl JoinIter {
    pub fn new(iters: Vec<TableIter>) -> Self {
        Self {
            iters,
            buffer: None,
            finished: false,
        }
    }

    fn advance(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        if let Some(buffer) = &mut self.buffer {
            for (index, iter) in self.iters.iter_mut().enumerate().rev() {
                if let Some(row) = iter.next()? {
                    buffer[index] = row;
                    return Ok(());
                } else {
                    if index == 0 {
                        self.finished = true;
                        return Ok(());
                    }
                    if let Some(row) = iter.reset_next()? {
                        buffer[index] = row;
                    } else {
                        self.finished = true;
                        return Ok(());
                    }
                }
            }
        } else {
            let mut buffer = Vec::with_capacity(self.iters.len());
            for iter in self.iters.iter_mut() {
                if let Some(next) = iter.next()? {
                    buffer.push(next);
                } else {
                    self.finished = true;
                    return Ok(());
                }
            }
            self.buffer = Some(buffer)
        }

        Ok(())
    }

    fn get(&self) -> Result<Option<RowValue>> {
        if self.finished {
            return Ok(None);
        }
        return Ok(Some(RowValue::new(
            self.buffer
                .as_ref()
                .ok_or_else(|| Error::Internal("Buffer is none".to_owned()))?,
        )));
    }

    pub fn next(&mut self) -> Result<Option<RowValue>> {
        if self.iters.len() == 0 {
            self.finished = true;
            return Ok(Some(RowValue::empty()));
        }
        self.advance()?;
        self.get()
    }

    pub fn filter(
        &mut self,
        filter: &Expression,
        handler: &JoinHandler,
    ) -> Result<Option<RowValue>> {
        if self.iters.len() == 0 {
            self.finished = true;
            return Ok(Some(RowValue::empty()));
        }
        while let Some(row) = self.next()? {
            if resolve_expression(filter, &row, &handler)?
                .get_truth()
                .is_true()
            {
                return self.get();
            }
        }
        Ok(None)
    }
}

impl TableColumns for JoinHandler {
    fn resolve_name(&self, name: ColumnName) -> Result<ColumnName> {
        if let Some(table_name) = name.table_name() {
            let handler = self
                .trees
                .get(table_name)
                .ok_or_else(|| ExecutionError::NoColumn(name.to_string()))?;
            if handler.contains_column(name.column_name()) {
                Ok(name)
            } else {
                Err(Error::Execution(ExecutionError::NoColumn(name.to_string())))
            }
        } else {
            let mut table_name = None;
            for (tree_name, handler) in &self.trees {
                if handler.contains_column(&name.column_name()) {
                    if table_name.is_none() {
                        table_name = Some(tree_name.to_string());
                    } else {
                        return Err(Error::Execution(ExecutionError::AmbiguousName(
                            name.destructure().1,
                        )));
                    }
                }
            }
            table_name
                .map(|n| ColumnName::new(Some(n), name.clone().destructure().1))
                .ok_or_else(|| ExecutionError::NoColumn(name.destructure().1).into())
        }
    }
}

pub enum RowValue<'a> {
    Data(&'a Vec<TableRow>),
    Empty,
}

impl<'a> RowValue<'a> {
    fn new(row: &'a Vec<TableRow>) -> Self {
        Self::Data(row)
    }

    pub fn empty() -> Self {
        Self::Empty
    }
}

impl<'a> Row for RowValue<'a> {
    type Handler = JoinHandler;

    fn get_data(&self, handler: &Self::Handler, column_name: &ColumnName) -> Result<Value> {
        match self {
            Self::Data(row) => {
                let table_name = column_name
                    .table_name()
                    .ok_or_else(|| Error::Internal("Unresolved column name".to_owned()))?;
                if let Some((index, _, handler)) = handler.trees.get_full(table_name) {
                    row.get(index)
                        .ok_or_else(|| Error::Internal(format!("No entry at index {}", index)))?
                        .get_data(handler, column_name)
                } else {
                    Err(Error::Internal("No table with that name".to_owned()))
                }
            }
            Self::Empty => Err(Error::Internal(
                "Tried to get data from empty row".to_owned(),
            )),
        }
    }
}
