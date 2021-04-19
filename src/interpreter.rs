use crate::{
    ast::{
        BinaryOp, Column, CreateTable, Delete, DropTable, Insert, Projection, SelectContents,
        SelectQuery, SqlQuery, TableName, UnresolvedExpression, Update, Values,
    },
    data_types::{Type, Value},
    error::{Error, ExecutionError, Result},
    foreign_key::ForeignKeys,
    join_handler::JoinHandler,
    resolved_expression::Expression,
    storage::Columns,
    table_definition::TableDefinition,
    table_handler::{TableHandler, TableRowUpdater},
    Empty, GetData, TableColumns,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use sled::{open, Db};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    path::Path,
};

pub struct Interpreter {
    db: Db,
    foreign_keys: OnceCell<TableHandler>,
}

impl Interpreter {
    pub fn was_recovered(&self) -> bool {
        self.db.was_recovered()
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Interpreter> {
        Ok(Interpreter {
            db: open(path)?,
            foreign_keys: OnceCell::new(),
        })
    }

    pub fn execute(&self, query: SqlQuery) -> Result<Relation> {
        let result = match query {
            SqlQuery::CreateTable(create_table) => self.execute_create_table(create_table),
            SqlQuery::Insert(insert) => self.execute_insert(insert),
            SqlQuery::SelectQuery(select) => self.execute_select(select),
            SqlQuery::DropTable(drop_table) => self.execute_drop_table(drop_table),
            SqlQuery::Delete(delete) => self.execute_delete(delete),
            SqlQuery::Update(update) => self.execute_update(update),
        }?;
        self.db.flush()?;
        Ok(result)
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

    pub fn open_internal_table(
        &self,
        table_name: String,
        columns: Columns,
    ) -> Result<TableHandler> {
        let tree = self.db.open_tree(&table_name)?;
        let table_definition = TableDefinition::new_empty(columns);
        Ok(TableHandler::new(tree, table_definition, table_name, None))
    }

    pub fn foreign_keys(&self) -> Result<ForeignKeys> {
        let handler = self.foreign_keys.get_or_try_init(|| {
            let mut columns = Columns::new();
            columns.add_column("name".to_owned(), Type::String, Value::Null)?;
            columns.add_column("table".to_owned(), Type::String, Value::Null)?;
            columns.add_column("columns".to_owned(), Type::String, Value::Null)?;
            columns.add_column("referred_table".to_owned(), Type::String, Value::Null)?;
            columns.add_column("referred_columns".to_owned(), Type::String, Value::Null)?;
            columns.add_column("on_delete".to_owned(), Type::Integer, Value::Null)?;
            columns.add_column("on_update".to_owned(), Type::Integer, Value::Null)?;

            self.open_internal_table("@foreign_keys".to_owned(), columns)
        })?;
        Ok(ForeignKeys::new(handler))
    }

    pub fn tables(&self) -> Result<Vec<TableHandler>> {
        self.db
            .tree_names()
            .into_iter()
            .filter_map(|n| {
                let name = String::from_utf8(n.as_ref().into()).unwrap();
                name.starts_with('@').then(|| name)
            })
            .map(|name| self.open_table(TableName::new(name, None)))
            .collect::<Result<Vec<_>>>()
    }

    fn execute_create_table(&self, create_table: CreateTable) -> Result<Relation> {
        let CreateTable {
            name,
            columns,
            uniques,
            primary_key,
            checks,
            foreign_keys,
        } = create_table;
        let table_name = name;
        let directory = self.db.open_tree("@tables")?;
        if directory.contains_key(table_name.as_bytes())? {
            return Err(Error::Execution(ExecutionError::TableExists(table_name)));
        }
        let mut table_definition =
            TableDefinition::with_capacity(columns.len(), uniques, primary_key);

        for key in foreign_keys {
            let foreign_table = self.open_table(TableName::new(key.foreign_table.clone(), None))?;
            let referred_columns_indexes = key
                .referred_columns
                .iter()
                .map(|s| foreign_table.column_index(s.as_str()))
                .collect::<Result<_>>()?;
            if !foreign_table.contains_unique(referred_columns_indexes) {
                return Err(ExecutionError::ForeignKeyNotUnique(key.name).into());
            }
            for (this_column, referred_column) in
                key.columns.iter().zip(key.referred_columns.iter())
            {
                let referred_type = foreign_table
                    .get_data_type(referred_column)
                    .ok_or_else(|| ExecutionError::NoColumn(referred_column.clone()))?;
                let this_column = columns
                    .iter()
                    .find(|c| c.name == *this_column)
                    .ok_or_else(|| ExecutionError::NoColumn(this_column.clone()))?;
                if referred_type != this_column.data_type {
                    return Err(ExecutionError::IncorrectForeignKeyReferredColumnType {
                        this_column_name: this_column.name.clone(),
                        referred_column_name: referred_column.clone(),
                        this_column_type: this_column.data_type,
                        referred_column_type: referred_type,
                    }
                    .into());
                }
            }
            self.foreign_keys()?.add_key(key, self, &table_name)?;
        }
        for Column {
            name,
            data_type,
            default,
            not_null,
        } in columns.into_iter()
        {
            let default = default
                .map(|d| evaluate_expression(&resolve_expression(d, &Empty)?, &Empty))
                .transpose()?;
            table_definition.add_column(name, default, not_null, data_type)?;
        }
        for (check, name) in checks {
            let check = resolve_expression(check, &(&table_definition, table_name.as_str()))?;
            table_definition.add_check(check, name);
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
                table.insert_values(row_values, self)?;
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
                table.insert_values(row, self)?
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
                    .map(|e| resolve_expression(e, &table_handler))
                    .transpose()?;
                for projection in projections {
                    match projection {
                        Projection::Wildcard => {
                            for column_name in table_handler.all_column_names()? {
                                result_column_names.push(column_name.to_string());
                                projection_expressions.push(Expression::Identifier(column_name));
                            }
                        }
                        Projection::QualifiedWildcard(table_name) => {
                            for column_name in table_handler.column_names(&table_name)? {
                                result_column_names.push(column_name.to_string());
                                projection_expressions.push(Expression::Identifier(column_name));
                            }
                        }
                        Projection::Unaliased(e) => {
                            result_column_names.push(e.to_string());
                            projection_expressions.push(resolve_expression(e, &table_handler)?);
                        }
                        Projection::Aliased(e, alias) => {
                            result_column_names.push(alias);
                            projection_expressions.push(resolve_expression(e, &table_handler)?);
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
                        .map(|e| evaluate_expression(&resolve_expression(e, &Empty)?, &Empty))
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
            .map(|p| resolve_expression(p, &table))
            .transpose()?
            .unwrap_or_else(|| Expression::Value(1.into()));
        for row in iter.filter_where(&predicate, &table) {
            let row = row?;
            table.delete_row(&row, self)?;
        }
        Ok(Relation::default())
    }

    fn execute_update(&self, update: Update) -> Result<Relation> {
        let Update {
            table_name,
            assignments,
            filter,
        } = update;
        let table = self.open_table(TableName::new(table_name, None))?;
        let mut iter = table.iter();
        let mut assignments = assignments
            .into_iter()
            .map(|(c, e)| Ok((table.column_index(&c)?, resolve_expression(e, &table)?)))
            .collect::<Result<Vec<_>>>()?;
        assignments.sort_unstable_by_key(|(i, _)| *i);
        let filter = filter
            .map(|p| resolve_expression(p, &table))
            .transpose()?
            .unwrap_or_else(|| Expression::Value(1.into()));
        for row in iter.filter_where(&filter, &table) {
            let row = row?;
            let mut new_row = TableRowUpdater::new(&row, &table);
            for (column, new_value_expression) in &assignments {
                let new_value = evaluate_expression(&new_value_expression, &(&table, &row))?;
                new_row.add_update(*column, new_value)?;
            }
            let new_row = new_row.finalise()?;
            table.update_row(row, &self, new_row)?;
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
                .map(|e| evaluate_expression(e, &(&table, &row)))
                .collect::<Result<Vec<_>>>()?;
            result_set.add_row(row_values)?;
        }
        Ok(result_set)
    }

    fn execute_drop_table(&self, drop_table: DropTable) -> Result<Relation> {
        for name in drop_table.names {
            self.foreign_keys()?.process_drop_table(&name, self)?;
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
        dbg!(&self.rows);

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

pub fn resolve_expression(
    expression: UnresolvedExpression,
    table: &impl TableColumns,
) -> Result<Expression> {
    match expression {
        UnresolvedExpression::Value(v) => Ok(Expression::Value(v)),
        UnresolvedExpression::Identifier(i) => Ok(Expression::Identifier(table.resolve_name(i)?)),
        UnresolvedExpression::BinaryOp(l, op, r) => Ok(Expression::BinaryOp(
            Box::new(resolve_expression(*l, table)?),
            op,
            Box::new(resolve_expression(*r, table)?),
        )),
    }
}

pub fn evaluate_expression<H>(expression: &Expression, row: &H) -> Result<Value>
where
    H: GetData,
{
    match expression {
        Expression::Identifier(column_name) => row.get_data(column_name),
        Expression::Value(v) => Ok(v.clone()),
        Expression::BinaryOp(l, op, r) => {
            let left = evaluate_expression(l, row)?;
            let right = evaluate_expression(r, row)?;
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
