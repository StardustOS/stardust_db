use crate::{
    ast::{
        BinaryOp, Column, CreateTable, Delete, DropTable, Expression, Insert, Projection,
        SelectContents, SelectQuery, SqlQuery, Values,
    },
    data_types::Value,
    error::{Error, ExecutionError, Result},
    join_handler::{JoinHandler, RowValue},
    storage::ColumnName,
    table_definition::TableDefinition,
    table_handler::TableHandler,
    Row, TableColumns,
};
use itertools::Itertools;
use sled::{open, Db};
use std::{
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

    pub fn execute(&mut self, query: SqlQuery) -> Result<Relation> {
        match query {
            SqlQuery::CreateTable(create_table) => self.execute_create_table(create_table),
            SqlQuery::Insert(insert) => self.execute_insert(insert),
            SqlQuery::SelectQuery(select) => self.execute_select(select),
            SqlQuery::DropTable(drop_table) => self.execute_drop_table(drop_table),
            SqlQuery::Delete(delete) => self.execute_delete(delete),
        }
    }

    fn execute_create_table(&mut self, create_table: CreateTable) -> Result<Relation> {
        let CreateTable { name, columns } = create_table;
        let table_name = name;
        let directory = self.db.open_tree("@tables")?;
        if directory.contains_key(table_name.as_bytes())? {
            return Err(Error::Execution(ExecutionError::TableExists(table_name)));
        }
        let mut table_definition = TableDefinition::with_capacity(columns.len());
        for Column {
            name,
            data_type,
            default,
            not_null,
            unique,
        } in columns.into_iter()
        {
            let default = default
                .map(|d| resolve_expression(&d, &RowValue::empty(), &JoinHandler::default()))
                .transpose()?;
            table_definition =
                table_definition.add_column(name, default, not_null, unique, false, data_type)?;
        }
        let encoded: Vec<u8> = bincode::serialize(&table_definition)?;
        directory.insert(table_name.clone().into_bytes(), encoded)?;
        directory.flush()?;

        let new_table = self.db.open_tree(table_name.into_bytes())?;
        new_table.flush()?;
        Ok(Default::default())
    }

    fn execute_insert(&mut self, insert: Insert) -> Result<Relation> {
        let Insert {
            table,
            columns,
            values,
        } = insert;
        let specified_columns = columns;
        let directory = self.db.open_tree("@tables")?;
        let columns_bytes = directory
            .get(table.as_bytes())?
            .ok_or_else(|| Error::Execution(ExecutionError::NoTable(table.clone())))?;
        let table_definition: TableDefinition = bincode::deserialize(columns_bytes.as_ref())?;
        let table = self.db.open_tree(table.as_bytes())?;
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
                let mut row_values = Vec::with_capacity(table_definition.num_columns());
                for column_name in table_definition.column_names() {
                    match peekable.peek() {
                        Some((_, name)) if column_name == name.as_str() => {
                            let (value, _) = peekable.next().unwrap();
                            row_values.push(value);
                        }
                        _ => row_values.push(table_definition.get_default(column_name)?),
                    }
                }
                table_definition.insert_values(&table, row_values)?;
            }
        } else {
            if values.num_columns() != table_definition.num_columns() {
                return Err(ExecutionError::WrongNumColumns {
                    expected: table_definition.num_columns(),
                    actual: values.num_columns(),
                }
                .into());
            }
            for row in values.take_rows() {
                table_definition.insert_values(&table, row)?
            }
        }
        Ok(Default::default())
    }

    fn execute_select(&mut self, select: SelectQuery) -> Result<Relation> {
        match select {
            SelectQuery::Select(select) => {
                let SelectContents {
                    projections,
                    from,
                    selection,
                } = select;
                let table_name = from.tables;
                let directory = self.db.open_tree("@tables")?;
                let table_defintion_bytes = directory
                    .get(table_name.as_bytes())?
                    .ok_or_else(|| ExecutionError::NoTable(table_name.clone()))?;
                let table_definition: TableDefinition =
                    bincode::deserialize(table_defintion_bytes.as_ref())?;
                let table_tree = self.db.open_tree(table_name.as_bytes())?;
                let single_table_handler =
                    TableHandler::new(table_tree, table_definition, table_name);
                let mut table_handler = JoinHandler::with_capacity(1);
                table_handler.add_tree(single_table_handler);

                let mut result_column_names = Vec::with_capacity(projections.len());
                let mut projection_expressions = Vec::with_capacity(projections.len());

                let selection = selection
                    .map(|e| resolve_column_names(e, &table_handler))
                    .transpose()?;
                for projection in projections {
                    match projection {
                        Projection::Wildcard => {
                            for table_name in table_handler.table_names() {
                                for column_name in table_handler.column_names() {
                                    result_column_names.push(column_name.to_owned());
                                    projection_expressions.push(Expression::Identifier(
                                        ColumnName::new(
                                            Some(table_name.to_owned()),
                                            column_name.to_owned(),
                                        ),
                                    ));
                                }
                            }
                        }
                        Projection::QualifiedWildcard(table_name) => {
                            if !table_handler.contains_table(&table_name) {
                                return Err(ExecutionError::NoTable(table_name).into());
                            }
                            for name in table_handler.column_names() {
                                result_column_names.push(name.to_owned());
                                projection_expressions.push(Expression::Identifier(
                                    ColumnName::new(Some(table_name.clone()), name.to_owned()),
                                ));
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
                        .map(|e| {
                            resolve_expression(&e, &RowValue::empty(), &JoinHandler::default())
                        })
                        .collect::<Result<_>>()?;
                    result.add_row(values)?
                }

                Ok(result)
            }
        }
    }

    fn execute_delete(&mut self, delete: Delete) -> Result<Relation> {
        let Delete {
            table_name,
            predicate,
        } = delete;
        let directory = self.db.open_tree("@tables")?;
        let table_defintion_bytes = directory
            .get(table_name.as_bytes())?
            .ok_or_else(|| ExecutionError::NoTable(table_name.clone()))?;
        let table_definition: TableDefinition =
            bincode::deserialize(table_defintion_bytes.as_ref())?;
        if let Some(predicate) = predicate {
            let table_tree = self.db.open_tree(table_name.as_bytes())?;
            let handler = TableHandler::new(table_tree, table_definition, table_name);
            let predicate = resolve_column_names(predicate, &handler)?;
            let mut iter = handler.iter();
            while let Some(row) = iter.filter(&predicate, &handler)? {
                row.delete_row(&handler)?;
            }
        } else {
            self.db.drop_tree(table_name.as_bytes())?;
        }
        Ok(Relation::default())
    }

    fn generate_results(
        &mut self,
        result_column_names: Vec<String>,
        projections: Vec<Expression>,
        table: JoinHandler,
        filter: Option<Expression>,
    ) -> Result<Relation> {
        let mut result_set = Relation::new(result_column_names);
        let mut iter = table.iter();
        while let Some(row) = if let Some(filter) = &filter {
            iter.filter(filter, &table)?
        } else {
            iter.next()?
        } {
            let row_values = projections
                .iter()
                .map(|e| resolve_expression(e, &row, &table))
                .collect::<Result<Vec<_>>>()?;
            result_set.add_row(row_values)?;
        }
        Ok(result_set)
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

fn resolve_column_names(expression: Expression, table: &impl TableColumns) -> Result<Expression> {
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

pub fn resolve_expression<R>(
    expression: &Expression,
    row: &R,
    handler: &R::Handler,
) -> Result<Value>
where
    R: Row,
{
    match expression {
        Expression::Identifier(i) => row.get_data(handler, i),
        Expression::Value(v) => Ok(v.clone()),
        Expression::BinaryOp(l, op, r) => {
            let left = resolve_expression(l, row, handler)?;
            let right = resolve_expression(r, row, handler)?;
            match op {
                BinaryOp::And => Ok(Value::TruthValue(left.get_truth().and(right.get_truth()))),
                BinaryOp::Or => Ok(Value::TruthValue(left.get_truth().or(right.get_truth()))),
                BinaryOp::Comparison(c) => {
                    let comparison_result = left.compare(&right);
                    Ok(comparison_result.get_truth(c).into())
                }
            }
        }
    }
}
