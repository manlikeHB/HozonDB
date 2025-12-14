use crate::catalog::row::{Row, Value};
use crate::sql::parser::{BinaryOperator, Expr};
use std::io::{self, Error, ErrorKind};

/// Extract value from an expression (operand)
fn get_value(expr: &Expr, row: &Row, col_names: &[String]) -> io::Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(n) => {
            let index = match col_names.iter().position(|x| x == n) {
                Some(i) => i,
                None => return Err(Error::new(ErrorKind::InvalidData, "Column doesn't exist")),
            };

            row.get_value(index)
                .cloned()
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "No row"))
        }
        Expr::BinaryOp { .. } => Err(Error::new(ErrorKind::InvalidData, "Unexpected Binary Op")),
    }
}

/// Evaluate an expression to a boolean (for WHERE clause filtering)
pub fn evaluate_expr(expr: &Expr, row: &Row, col_names: &[String]) -> io::Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left = evaluate_expr(left, row, col_names)?;
                let right = evaluate_expr(right, row, col_names)?;
                Ok(left && right)
            }
            BinaryOperator::Or => {
                let left = evaluate_expr(left, row, col_names)?;
                let right = evaluate_expr(right, row, col_names)?;
                Ok(left || right)
            }
            BinaryOperator::Equals
            | BinaryOperator::NotEquals
            | BinaryOperator::LessThan
            | BinaryOperator::GreaterThan
            | BinaryOperator::LessOrEqual
            | BinaryOperator::GreaterOrEqual => {
                let left_val = get_value(left, row, col_names)?;
                let right_val = get_value(right, row, col_names)?;
                compare_values(&left_val, &right_val, op)
            }
        },
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected BinaryOp in WHERE clause",
            ));
        }
    }
}

/// Compare two values using a comparison operator
fn compare_values(left: &Value, right: &Value, op: &BinaryOperator) -> io::Result<bool> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(match op {
            BinaryOperator::Equals => a == b,
            BinaryOperator::NotEquals => a != b,
            BinaryOperator::LessThan => a < b,
            BinaryOperator::GreaterThan => a > b,
            BinaryOperator::LessOrEqual => a <= b,
            BinaryOperator::GreaterOrEqual => a >= b,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Invalid operator for integers",
                ));
            }
        }),
        (Value::Text(a), Value::Text(b)) => Ok(match op {
            BinaryOperator::Equals => a == b,
            BinaryOperator::NotEquals => a != b,
            BinaryOperator::LessThan => a < b,
            BinaryOperator::GreaterThan => a > b,
            BinaryOperator::LessOrEqual => a <= b,
            BinaryOperator::GreaterOrEqual => a >= b,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Invalid operator for text",
                ));
            }
        }),
        (Value::Boolean(a), Value::Boolean(b)) => Ok(match op {
            BinaryOperator::Equals => a == b,
            BinaryOperator::NotEquals => a != b,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Booleans only support = and !=",
                ));
            }
        }),
        (Value::Null, Value::Null) => {
            // SQL standard: NULL = NULL is false
            Ok(false)
        }
        (Value::Null, _) | (_, Value::Null) => {
            // NULL compared to anything is false
            Ok(false)
        }
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            "Type mismatch in comparison",
        )),
    }
}
