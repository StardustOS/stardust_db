use crate::ast::{CreateTable, DropTable, Expression, Insert, Projection, SelectContents, SelectQuery, SqlQuery, Values, Column};
use crate::error::{Error, ExecutionError, Result};
use crate::storage::Columns;
use itertools::Itertools;
use sled::{open, Db};
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

pub struct Interpreter {
    db: Db,
}

impl Interpreter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Interpreter> {
        Ok(Interpreter { db: open(path)? })
    }

    pub fn execute(&mut self, query: SqlQuery) -> Result<Relation> {
        match query {
            SqlQuery::CreateTable(create_table) => self.execute_create_table(create_table),
            SqlQuery::Insert(insert) => self.execute_insert(insert),
            SqlQuery::SelectQuery(select) => self.execute_select(select),
            SqlQuery::DropTable(drop_table) => self.execute_drop_table(drop_table),
        }
    }

    fn execute_create_table(&mut self, create_table: CreateTable) -> Result<Relation> {
        let CreateTable { name, columns } = create_table;
        let directory = self.db.open_tree("@tables")?;
        if directory.contains_key(name.as_bytes())? {
            return Err(Error::Execution(ExecutionError::TableExists(name)));
        }
        let mut columns_definition = Columns::new();
        for Column { name, data_type, default } in columns {
            columns_definition.add_column(name, data_type)?;
        }
        let encoded: Vec<u8> = bincode::serialize(&columns_definition)?;
        directory.insert(name.clone().into_bytes(), encoded)?;
        directory.flush()?;

        let new_table = self.db.open_tree(name.clone().into_bytes())?;
        new_table.flush()?;
        Ok(Default::default())
    }

    fn execute_insert(&mut self, insert: Insert) -> Result<Relation> {
        let Insert {
            table,
            columns,
            values,
        } = insert;
        let directory = self.db.open_tree("@tables")?;
        let columns_bytes = directory
            .get(table.as_bytes())?
            .ok_or_else(|| Error::Execution(ExecutionError::NoTable(table.clone())))?;
        let columns_definition: Columns = bincode::deserialize(columns_bytes.as_ref())?;
        let table = self.db.open_tree(table.as_bytes())?;
        match values {
            SelectQuery::Values(values) => {
                let Values { rows } = values;
                for row in rows {
                    let row_values = row
                        .into_iter()
                        .map(|e| match e {
                            Expression::Literal(s) => s,
                            _ => unimplemented!("{:?}", e),
                        })
                        .collect();
                    let row_bytes = columns_definition.generate_row(row_values).unwrap();
                    let row = table
                        .last()?
                        .map(|(key, _)| u32::from_be_bytes(key.as_ref().try_into().unwrap()) + 1)
                        .unwrap_or_default();
                    table.insert(row.to_be_bytes(), row_bytes)?;
                }
            }
            _ => unimplemented!("{:?}", values),
        }

        Ok(Default::default())
    }

    fn execute_select(&mut self, select: SelectQuery) -> Result<Relation> {
        Ok(match select {
            SelectQuery::Select(select) => {
                let SelectContents { projections, from } = select;
                let table = from.table;
                let directory = self.db.open_tree("@tables")?;
                let columns_bytes = directory
                    .get(table.as_bytes())?
                    .ok_or_else(|| Error::Execution(ExecutionError::NoTable(table.clone())))?;
                let columns_definition: Columns = bincode::deserialize(columns_bytes.as_ref())?;
                let table = self.db.open_tree(table.as_bytes())?;

                let mut result_column = Columns::new();
                for projection in projections {
                    match projection {
                        Projection::Wildcard => {
                            result_column.extend_from_existing(columns_definition.clone())?;
                        }
                        Projection::ColumnName(s) => {
                            result_column
                                .extend_from_existing_filter(&columns_definition, Some(s))?;
                        }
                        Projection::Unaliased(u) => unimplemented!("{:?}", u),
                    }
                }

                let mut result_set = Relation::new(result_column.clone());
                for entry in &table {
                    let (_key, value) = entry?;
                    let row = columns_definition.filter_row(value.as_ref(), &result_column)?;
                    result_set.add_row(row);
                }

                result_set
            }
            _ => unimplemented!("{:?}", select),
        })
    }

    fn execute_drop_table(&mut self, drop_table: DropTable) -> Result<Relation> {
        for name in drop_table.names {
            let directory = self.db.open_tree("@tables")?;
            directory.remove(name.as_bytes())?;
            self.db.drop_tree(name.as_bytes())?;
        }
        Ok(Default::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Relation {
    columns: Columns,
    rows: Vec<Vec<u8>>,
}

impl Relation {
    pub fn new(columns: Columns) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<u8>) {
        self.rows.push(row)
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.columns.is_empty() {
            writeln!(f, "{}", self.columns.names().join("|"))?;
            for row in &self.rows {
                writeln!(
                    f,
                    "{}",
                    self.columns
                        .names()
                        .map(|name| self.columns.get_typed_data(name, row.as_slice()).unwrap())
                        .join("|")
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProjectionResult {
    columns: Columns,
    rows: Vec<Vec<u8>>,
}

impl ProjectionResult {
    pub fn new(columns: Columns) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<u8>) {
        self.rows.push(row)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    columns: Columns,
    defaults: HashMap<String, String>
}

impl TableDefinition {
    pub fn new(columns: Columns, defaults: HashMap<String, String>) -> Self {
        Self { columns, defaults }
    }
}