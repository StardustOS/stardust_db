use sqlparser::ast::{
    self, Join, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
};

use crate::{
    ast::{
        JoinConstraint, JoinOperator, OrderBy, OrderByDirection, Projection, SelectContents,
        SelectQuery, TableJoins, TableName, Values,
    },
    error::{ExecutionError, Result},
    query_process::parse_expression,
};

pub fn parse_select_query(query: Query) -> Result<SelectQuery> {
    Ok(match query.body {
        SetExpr::Values(v) => SelectQuery::Values(Values::new(
            v.0.into_iter()
                .map(|row| row.into_iter().map(parse_expression).collect())
                .collect(),
        )),
        SetExpr::Select(s) => SelectQuery::Select(parse_select(*s, query.order_by)?),
        _ => unimplemented!("{:?}", query.body),
    })
}

fn parse_select(select: Select, order_by: Vec<OrderByExpr>) -> Result<SelectContents> {
    let projections = select
        .projection
        .into_iter()
        .map(|p| match p {
            SelectItem::UnnamedExpr(e) => Projection::Unaliased(parse_expression(e)),
            SelectItem::ExprWithAlias { expr, alias } => {
                Projection::Aliased(parse_expression(expr), alias.to_string())
            }
            SelectItem::Wildcard => Projection::Wildcard,
            SelectItem::QualifiedWildcard(name) => Projection::QualifiedWildcard(name.to_string()),
        })
        .collect();
    let from = parse_table_joins(select.from.into_iter())?;
    let selection = select.selection.map(parse_expression);
    let order_by = order_by.into_iter().map(parse_order_by).collect();
    Ok(SelectContents::new(projections, from, selection, order_by))
}

fn parse_table_joins<I>(mut joins: I) -> Result<Option<TableJoins>>
where
    I: Iterator<Item = TableWithJoins>,
{
    joins
        .next()
        .map(|left| parse_table_joins_recursive(parse_with_join(left)?, joins))
        .transpose()
}

fn parse_table_joins_recursive<I>(left: TableJoins, mut joins: I) -> Result<TableJoins>
where
    I: Iterator<Item = TableWithJoins>,
{
    match joins.next() {
        Some(right) => {
            let right = parse_with_join(right)?;
            for name in right.table_names() {
                if left.contains_table(name) {
                    return Err(ExecutionError::DuplicateTableName(name.to_owned()).into());
                }
            }
            let new_left = TableJoins::Join {
                left: Box::new(left),
                right: Box::new(right),
                operator: JoinOperator::Inner,
                constraint: JoinConstraint::None,
            };
            parse_table_joins_recursive(new_left, joins)
        }
        None => Ok(left),
    }
}

fn parse_with_join(join: TableWithJoins) -> Result<TableJoins> {
    let left = parse_table_factor(join.relation);
    parse_joins(left?, join.joins.into_iter())
}

fn parse_table_factor(factor: TableFactor) -> Result<TableJoins> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let name = name.to_string();
            let alias = alias.map(|alias| alias.name.to_string());
            Ok(TableJoins::Table(TableName::new(name, alias)))
        }
        TableFactor::NestedJoin(join) => parse_with_join(*join),
        _ => unimplemented!("{:?}", factor),
    }
}

fn parse_joins<I>(left: TableJoins, mut joins: I) -> Result<TableJoins>
where
    I: Iterator<Item = Join>,
{
    match joins.next() {
        None => Ok(left),
        Some(join) => {
            let Join {
                relation,
                join_operator,
            } = join;
            let right = parse_table_factor(relation)?;
            for name in right.table_names() {
                if left.contains_table(name) {
                    return Err(ExecutionError::DuplicateTableName(name.to_owned()).into());
                }
            }
            let (operator, constraint) = match join_operator {
                ast::JoinOperator::Inner(constraint) => {
                    (JoinOperator::Inner, parse_join_constraint(constraint))
                }
                ast::JoinOperator::LeftOuter(constraint) => {
                    (JoinOperator::Left, parse_join_constraint(constraint))
                }
                ast::JoinOperator::RightOuter(constraint) => {
                    (JoinOperator::Right, parse_join_constraint(constraint))
                }
                ast::JoinOperator::CrossJoin => (JoinOperator::Inner, JoinConstraint::None),
                _ => unimplemented!("{:?}", join_operator),
            };
            let left = TableJoins::Join {
                left: Box::new(left),
                right: Box::new(right),
                operator,
                constraint,
            };
            parse_joins(left, joins)
        }
    }
}

fn parse_join_constraint(constraint: ast::JoinConstraint) -> JoinConstraint {
    match constraint {
        ast::JoinConstraint::On(e) => JoinConstraint::On(parse_expression(e)),
        ast::JoinConstraint::None => JoinConstraint::None,
        ast::JoinConstraint::Natural => JoinConstraint::Natural,
        ast::JoinConstraint::Using(cols) => {
            JoinConstraint::Using(cols.into_iter().map(|i| i.to_string()).collect())
        }
    }
}

fn parse_order_by(order_by: OrderByExpr) -> OrderBy {
    let expression = parse_expression(order_by.expr);
    let direction = match order_by.asc {
        Some(true) | None => OrderByDirection::Ascending,
        Some(false) => OrderByDirection::Descending,
    };
    let nulls_first = matches!(order_by.nulls_first, Some(true));
    OrderBy::new(expression, direction, nulls_first)
}
