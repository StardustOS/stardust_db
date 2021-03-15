use std::sync::Arc;

use sled::{IVec, Tree};

use crate::{
    ast::Expression,
    error::{Error, ExecutionError, Result},
    interpreter::resolve_expression,
    Row, TableColumns,
};
use crate::{data_types::Value, storage::ColumnName, table_definition::TableDefinition};

pub struct TableHandler {
    tree: Tree,
    table_definition: TableDefinition,
    table_name: Arc<str>,
}

impl TableHandler {
    pub fn new(tree: Tree, table_definition: TableDefinition, table_name: String) -> Self {
        Self {
            tree,
            table_definition,
            table_name: table_name.into(),
        }
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.table_definition.contains_column(column_name)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.table_definition.column_names()
    }

    pub fn table_name(&self) -> Arc<str> {
        self.table_name.clone()
    }

    pub fn iter(&self) -> TableIter {
        TableIter::new(self.tree.clone())
    }
}

pub struct TableIter {
    tree: Tree,
    iter: sled::Iter,
}

impl TableIter {
    pub fn new(tree: Tree) -> Self {
        Self {
            iter: tree.iter(),
            tree,
        }
    }

    pub fn next(&mut self) -> Result<Option<TableRow>> {
        Ok(self
            .iter
            .next()
            .transpose()?
            .map(|(left, right)| TableRow::new(left, right)))
    }

    pub fn filter(
        &mut self,
        predicate: &Expression,
        handler: &TableHandler,
    ) -> Result<Option<TableRow>> {
        while let Some(next) = self.next()? {
            if resolve_expression(predicate, &next, handler)?
                .get_truth()
                .is_true()
            {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    pub fn reset_next(&mut self) -> Result<Option<TableRow>> {
        self.iter = self.tree.iter();
        self.next()
    }
}

pub struct TableRow {
    left: IVec,
    right: IVec,
}

impl TableRow {
    fn new(left: IVec, right: IVec) -> Self {
        Self { left, right }
    }

    pub fn delete_row(&self, handler: &TableHandler) -> Result<()> {
        handler.tree.remove(self.left.as_ref())?;
        Ok(())
    }
}

impl Row for TableRow {
    type Handler = TableHandler;

    fn get_data(&self, handler: &TableHandler, column_name: &ColumnName) -> Result<Value> {
        if let Some(table_name) = column_name.table_name() {
            if table_name == handler.table_name.as_ref() {
                handler.table_definition.get_data(
                    column_name.column_name(),
                    &self.left,
                    &self.right,
                )
            } else {
                Err(Error::Internal(format!(
                    "Table name resolved incorrectly for Single Table Row: {}",
                    table_name
                )))
            }
        } else {
            Err(Error::Internal(format!(
                "Unresolved column name in Single Table Row: {}",
                column_name
            )))
        }
    }
}

impl TableColumns for TableHandler {
    fn resolve_name(&self, name: ColumnName) -> Result<ColumnName> {
        if let Some(table) = name.table_name() {
            if self.table_name.as_ref() == table
                && self.table_definition.contains_column(name.column_name())
            {
                return Ok(name);
            }
        } else if self.table_definition.contains_column(name.column_name()) {
            return Ok(ColumnName::new(
                Some(self.table_name.to_string()),
                name.destructure().1,
            ));
        }
        Err(Error::Execution(ExecutionError::NoColumn(name.to_string())))
    }
}
