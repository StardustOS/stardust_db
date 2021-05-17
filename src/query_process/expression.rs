use sqlparser::ast::{BinaryOperator, Expr, Ident, Value};

use crate::{
    ast::{BinaryOp, ColumnName, ComparisonOp, MathematicalOp, UnresolvedExpression},
    data_types::TypeContents,
};

pub fn parse_expression(expression: Expr) -> UnresolvedExpression {
    match expression {
        Expr::Value(v) => UnresolvedExpression::Value(parse_value(v)),
        Expr::Identifier(i) => {
            UnresolvedExpression::Identifier(ColumnName::new(None, i.to_string()))
        }
        Expr::CompoundIdentifier(i) => {
            UnresolvedExpression::Identifier(parse_compound_identifier(i))
        }
        Expr::BinaryOp { left, op, right } => {
            let left = Box::new(parse_expression(*left));
            let right = Box::new(parse_expression(*right));
            match op {
                BinaryOperator::And => UnresolvedExpression::BinaryOp(left, BinaryOp::And, right),
                BinaryOperator::Or => UnresolvedExpression::BinaryOp(left, BinaryOp::Or, right),
                BinaryOperator::Eq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Eq),
                    right,
                ),
                BinaryOperator::NotEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::NotEq),
                    right,
                ),
                BinaryOperator::Lt => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Lt),
                    right,
                ),
                BinaryOperator::Gt => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::Gt),
                    right,
                ),
                BinaryOperator::LtEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::LtEq),
                    right,
                ),
                BinaryOperator::GtEq => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Comparison(ComparisonOp::GtEq),
                    right,
                ),
                BinaryOperator::Plus => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Mathematical(MathematicalOp::Add),
                    right,
                ),
                BinaryOperator::Minus => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Mathematical(MathematicalOp::Subtract),
                    right,
                ),
                BinaryOperator::Multiply => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Mathematical(MathematicalOp::Multiply),
                    right,
                ),
                BinaryOperator::Divide => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Mathematical(MathematicalOp::Divide),
                    right,
                ),
                BinaryOperator::Modulus => UnresolvedExpression::BinaryOp(
                    left,
                    BinaryOp::Mathematical(MathematicalOp::Modulus),
                    right,
                ),
                _ => unimplemented!("{:?}", op),
            }
        }
        _ => unimplemented!("{:?}", expression),
    }
}

fn parse_value(value: Value) -> crate::data_types::Value {
    match value {
        Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => {
            crate::data_types::Value::TypedValue(TypeContents::String(s))
        }
        Value::Number(s, _) => crate::data_types::Value::TypedValue(TypeContents::Integer(
            s.parse().expect("Number string was not a number"),
        )),
        Value::Null => crate::data_types::Value::Null,
        Value::Boolean(b) => {
            crate::data_types::Value::TypedValue(TypeContents::Integer(if b { 1 } else { 0 }))
        }
        _ => unimplemented!("{:?}", value),
    }
}

fn parse_compound_identifier(identifier: Vec<Ident>) -> ColumnName {
    match identifier.as_slice() {
        [.., table, column] => ColumnName::new(Some(table.to_string()), column.to_string()),
        [column] => ColumnName::new(None, column.to_string()),
        _ => unimplemented!("{:?}", identifier),
    }
}
