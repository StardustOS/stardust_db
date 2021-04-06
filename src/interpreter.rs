use crate::{
    ast::{
        BinaryOp, Column, CreateTable, Delete, DropTable, Expression, Insert, Projection,
        SelectContents, SelectQuery, SqlQuery, TableName, Values,
    },
    data_types::Value,
    error::{Error, ExecutionError, Result},
    join_handler::JoinHandler,
    table_definition::TableDefinition,
    table_handler::TableHandler,
    EmptyRow, GetData, TableColumns,
};
use itertools::Itertools;
use sled::{open, Db};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    path::Path,
};

pub struct Interpreter {
    db: Db,
}

impl Interpreter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Interpreter> {
        Ok(Interpreter { db: open(path)? })
    }

    pub fn execute(&self, query: SqlQuery) -> Result<Relation> {
        match query {
            SqlQuery::CreateTable(create_table) => self.execute_create_table(create_table),
            SqlQuery::Insert(insert) => self.execute_insert(insert),
            SqlQuery::SelectQuery(select) => self.execute_select(select),
            SqlQuery::DropTable(drop_table) => self.execute_drop_table(drop_table),
            SqlQuery::Delete(delete) => self.execute_delete(delete),
        }
    }

    pub fn open_table(&self, table_name: TableName) -> Result<TableHandler> {
        let TableName { name, alias } = table_name;
        let directory = self.db.open_tree("@tables")?;
        let columns_bytes = directory
            .get(name.as_bytes())?
            .ok_or_else(|| Error::Execution(ExecutionError::NoTable(name.clone())))?;
        let table_definition: TableDefinition = bincode::deserialize(columns_bytes.as_ref())?;
        let tree = self.db.open_tree(name.as_bytes())?;
        Ok(TableHandler::new(tree, table_definition, name, alias))
    }

    fn execute_create_table(&self, create_table: CreateTable) -> Result<Relation> {
        let CreateTable {
            name,
            columns,
            uniques,
            primary_key_name,
        } = create_table;
        let table_name = name;
        let directory = self.db.open_tree("@tables")?;
        if directory.contains_key(table_name.as_bytes())? {
            return Err(Error::Execution(ExecutionError::TableExists(table_name)));
        }
        let mut table_definition =
            TableDefinition::with_capacity(columns.len(), uniques, primary_key_name);
        for Column {
            name,
            data_type,
            default,
            not_null,
        } in columns.into_iter()
        {
            let default = default
                .map(|d| resolve_expression(&d, &EmptyRow))
                .transpose()?;
            table_definition.add_column(name, default, not_null, data_type)?;
        }
        let encoded: Vec<u8> = bincode::serialize(&table_definition)?;
        directory.insert(table_name.clone().into_bytes(), encoded)?;
        directory.flush()?;

        let new_table = self.db.open_tree(table_name.into_bytes())?;
        new_table.flush()?;
        Ok(Default::default())
    }

    fn execute_insert(&self, insert: Insert) -> Result<Relation> {
        let Insert {
            table,
            columns,
            values,
        } = insert;
        let specified_columns = columns;
        let table = self.open_table(table)?;
        let values = self.execute_select(values)?;
        if let Some(specified_columns) = specified_columns {
            if values.num_columns() != specified_columns.len() {
                return Err(ExecutionError::WrongNumColumns {
                    expected: specified_columns.len(),
                    actual: values.num_columns(),
                }
                .into());
            }
            for row in values.take_rows() {
                let mut peekable = row.into_iter().zip(specified_columns.iter()).peekable();
                let mut row_values = Vec::with_capacity(table.num_columns());
                for column_name in table.column_names() {
                    match peekable.peek() {
                        Some((_, name)) if column_name == name.as_str() => {
                            let (value, _) = peekable.next().unwrap();
                            row_values.push(value);
                        }
                        _ => row_values.push(table.get_default(column_name)?),
                    }
                }
                table.insert_values(row_values)?;
            }
        } else {
            if values.num_columns() != table.num_columns() {
                return Err(ExecutionError::WrongNumColumns {
                    expected: table.num_columns(),
                    actual: values.num_columns(),
                }
                .into());
            }
            for row in values.take_rows() {
                table.insert_values(row)?
            }
        }
        Ok(Default::default())
    }

    fn execute_select(&self, select: SelectQuery) -> Result<Relation> {
        match select {
            SelectQuery::Select(select) => {
                let SelectContents {
                    projections,
                    from,
                    selection,
                } = select;

                let table_handler = JoinHandler::new(self, from)?;
                let mut result_column_names = Vec::with_capacity(projections.len());
                let mut projection_expressions = Vec::with_capacity(projections.len());

                let selection = selection
                    .map(|e| resolve_column_names(e, &table_handler))
                    .transpose()?;
                for projection in projections {
                    match projection {
                        Projection::Wildcard => {
                            for column_name in table_handler.all_column_names()? {
                                result_column_names.push(column_name.column_name().to_owned());
                                projection_expressions.push(Expression::Identifier(column_name));
                            }
                        }
                        Projection::QualifiedWildcard(table_name) => {
                            for column_name in table_handler.column_names(&table_name)? {
                                result_column_names.push(column_name.column_name().to_owned());
                                projection_expressions.push(Expression::Identifier(column_name));
                            }
                        }
                        Projection::Unaliased(e) => {
                            result_column_names.push(e.to_string());
                            projection_expressions.push(resolve_column_names(e, &table_handler)?);
                        }
                        Projection::Aliased(e, alias) => {
                            result_column_names.push(alias);
                            projection_expressions.push(resolve_column_names(e, &table_handler)?);
                        }
                    }
                }

                self.generate_results(
                    result_column_names,
                    projection_expressions,
                    table_handler,
                    selection,
                )
            }
            SelectQuery::Values(values) => {
                let Values { rows } = values;
                if rows.is_empty() {
                    return Ok(Relation::default());
                }
                let mut columns = Vec::with_capacity(rows[0].len());
                for expression in &rows[0] {
                    let name = expression.to_string();
                    columns.push(name);
                }
                let mut result = Relation::new(columns);
                for row in rows {
                    let values = row
                        .into_iter()
                        .map(|e| resolve_expression(&e, &EmptyRow))
                        .collect::<Result<_>>()?;
                    result.add_row(values)?
                }

                Ok(result)
            }
        }
    }

    fn execute_delete(&self, delete: Delete) -> Result<Relation> {
        let Delete {
            table_name,
            predicate,
        } = delete;
        let table = self.open_table(TableName::new(table_name, None))?;
        let mut iter = table.iter();
        let predicate = predicate
            .map(|p| resolve_column_names(p, &table))
            .transpose()?;
        while let Some(row) = if let Some(predicate) = &predicate {
            iter.filter(predicate, &table)?
        } else {
            iter.get_next()?
        } {
            row.delete_row(&table)?
        }
        Ok(Relation::default())
    }

    fn generate_results(
        &self,
        result_column_names: Vec<String>,
        projections: Vec<Expression>,
        table: JoinHandler,
        filter: Option<Expression>,
    ) -> Result<Relation> {
        let mut result_set = Relation::new(result_column_names);
        let mut iter = table.iter(filter)?;
        while let Some(row) = iter.get_next()? {
            let row_values = projections
                .iter()
                .map(|e| resolve_expression(e, &(&table, &row)))
                .collect::<Result<Vec<_>>>()?;
            result_set.add_row(row_values)?;
        }
        Ok(result_set)
    }

    fn execute_drop_table(&self, drop_table: DropTable) -> Result<Relation> {
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
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl Relation {
    pub fn new(column_names: Vec<String>) -> Self {
        Self {
            column_names,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Vec<Value>) -> Result<()> {
        if self.column_names.len() == row.len() {
            self.rows.push(row);
            Ok(())
        } else {
            Err(Error::Execution(ExecutionError::WrongNumColumns {
                expected: self.column_names.len(),
                actual: self.rows.len(),
            }))
        }
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.column_names.iter().map(|n| n.as_ref())
    }

    pub fn rows(&self) -> impl Iterator<Item = &[Value]> {
        self.rows.iter().map(|r| r.as_slice())
    }

    pub fn take_rows(self) -> Vec<Vec<Value>> {
        self.rows
    }

    pub fn num_columns(&self) -> usize {
        self.column_names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.column_names.is_empty()
    }

    pub fn assert_equals(&self, rows: HashSet<Vec<Value>>, column_names: Vec<&str>) {
        assert_eq!(self.rows.len(), rows.len());

        assert_eq!(self.column_names, column_names);

        for row in &self.rows {
            assert!(rows.contains(row));
        }
    }

    pub fn ordered_equals(&self, rows: Vec<Vec<Value>>, column_names: Vec<&str>) -> bool {
        self.rows == rows && self.column_names == column_names
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.column_names.is_empty() {
            writeln!(f, "{}", self.column_names.iter().join("|"))?;
            for row in &self.rows {
                writeln!(f, "{}", row.iter().join("|"))?;
            }
        }
        Ok(())
    }
}

pub fn resolve_column_names(
    expression: Expression,
    table: &impl TableColumns,
) -> Result<Expression> {
    match expression {
        Expression::Value(v) => Ok(Expression::Value(v)),
        Expression::Identifier(i) => Ok(Expression::Identifier(table.resolve_name(i)?)),
        Expression::BinaryOp(l, op, r) => Ok(Expression::BinaryOp(
            Box::new(resolve_column_names(*l, table)?),
            op,
            Box::new(resolve_column_names(*r, table)?),
        )),
    }
}

pub fn resolve_expression<H>(expression: &Expression, row: &H) -> Result<Value>
where
    H: GetData,
{
    match expression {
        Expression::Identifier(column_name) => row.get_data(column_name),
        Expression::Value(v) => Ok(v.clone()),
        Expression::BinaryOp(l, op, r) => {
            let left = resolve_expression(l, row)?;
            let right = resolve_expression(r, row)?;
            match op {
                BinaryOp::And => Ok(left.and(&right)),
                BinaryOp::Or => Ok(left.or(&right)),
                BinaryOp::Comparison(c) => {
                    let comparison_result = left.compare(&right);
                    Ok(comparison_result.get_value(c))
                }
            }
        }
    }
}
