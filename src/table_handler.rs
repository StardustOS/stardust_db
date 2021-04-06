use sled::{IVec, Tree};

use crate::{
    ast::Expression,
    error::{Error, ExecutionError, Result},
    interpreter::resolve_expression,
    GetData, TableColumns,
};
use crate::{data_types::Value, storage::ColumnName, table_definition::TableDefinition};

pub struct TableHandler {
    tree: Tree,
    table_definition: TableDefinition,
    table_name: String,
    alias: Option<String>,
}

impl TableHandler {
    pub fn new(
        tree: Tree,
        table_definition: TableDefinition,
        table_name: String,
        alias: Option<String>,
    ) -> Self {
        Self {
            tree,
            table_definition,
            table_name,
            alias,
        }
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.table_definition.contains_column(column_name)
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.table_definition.column_names()
    }

    pub fn iter(&self) -> TableIter {
        TableIter::new(self.tree.clone())
    }

    pub fn insert_values(&self, values: Vec<Value>) -> Result<()> {
        self.table_definition.insert_values(&self.tree, values)
    }

    pub fn num_columns(&self) -> usize {
        self.table_definition.num_columns()
    }

    pub fn get_default(&self, column_name: &str) -> Result<Value> {
        self.table_definition.get_default(column_name)
    }

    pub fn aliased_table_name(&self) -> &str {
        self.alias
            .as_deref()
            .unwrap_or_else(|| self.table_name.as_str())
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

    pub fn get_next(&mut self) -> Result<Option<TableRow>> {
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
        while let Some(next) = self.get_next()? {
            if resolve_expression(predicate, &(handler, &next))?.is_true() {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    pub fn reset(&mut self) {
        self.iter = self.tree.iter();
    }

    pub fn reset_next(&mut self) -> Result<Option<TableRow>> {
        self.reset();
        self.get_next()
    }
}

#[derive(Default, Clone)]
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

    pub fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }
}

impl TableColumns for TableHandler {
    fn resolve_name(&self, name: ColumnName) -> Result<ColumnName> {
        let this_name = self.aliased_table_name();
        if let Some(table) = name.table_name() {
            if this_name == table && self.table_definition.contains_column(name.column_name()) {
                return Ok(name);
            }
        } else if self.table_definition.contains_column(name.column_name()) {
            return Ok(ColumnName::new(
                Some(this_name.to_string()),
                name.destructure().1,
            ));
        }
        Err(Error::Execution(ExecutionError::NoColumn(name.to_string())))
    }
}

impl<'a> GetData for (&'a TableHandler, &'a TableRow) {
    fn get_data(&self, column_name: &ColumnName) -> Result<Value> {
        let (handler, row) = self;
        if row.left.is_empty() && row.right.is_empty() {
            return Ok(Value::Null);
        }

        if let Some(table_name) = column_name.table_name() {
            if table_name == handler.aliased_table_name() {
                handler
                    .table_definition
                    .get_data(column_name.column_name(), &row.right)
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
