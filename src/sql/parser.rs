use crate::catalog::row::Value;
use crate::catalog::schema::{Column, DataType};
use crate::sql::tokenizer::Token;
use std::io::{self, Error, ErrorKind};

pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        values: Vec<Value>,
    },
    Select {
        table_name: String,
        columns: SelectColumns,
        where_clause: Option<Expr>,
    },
    DropTable {
        name: String,
    },
    Delete {
        table_name: String,
        where_clause: Option<Expr>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Value)>,
        where_clause: Option<Expr>,
    },
}

#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    All,
    Specific(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    Column(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessOrEqual,
    GreaterOrEqual,
    // logical
    And,
    Or,
}

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens,
            position: 0,
        }
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub fn advance(&mut self) {
        self.position += 1;
    }

    pub fn consume(&mut self) -> Option<Token> {
        let token = self.peek()?.clone();
        self.advance();
        Some(token)
    }

    pub fn expect(&mut self, expected: Token) -> io::Result<()> {
        let cur_token = self.consume().ok_or_else(|| {
            return Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input");
        })?;

        if cur_token != expected {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Expected {:?}, found {:?}", expected, cur_token),
            ));
        }

        Ok(())
    }

    pub fn parse(&mut self) -> io::Result<Statement> {
        if let Some(token) = self.peek() {
            match token {
                Token::Create => self.parse_create_table(),
                Token::Insert => self.parse_insert(),
                Token::Select => self.parse_select(),
                Token::Drop => self.parse_drop_table(),
                Token::Delete => self.parse_delete(),
                Token::Update => self.parse_update(),
                _ => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unexpected token: {:?}", token),
                )),
            }
        } else {
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Unexpected end of input",
            ))
        }
    }

    fn get_table_name(&mut self) -> io::Result<String> {
        let token = self
            .consume()
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input"))?;
        let table_name = if let Token::Identifier(name) = token {
            name
        } else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected table name".to_string(),
            ));
        };

        Ok(table_name)
    }

    fn parse_create_table(&mut self) -> io::Result<Statement> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;

        // table name
        let table_name = self.get_table_name()?;

        self.expect(Token::LeftParen)?;

        // extract columns
        let mut columns = Vec::new();
        loop {
            // column name
            let token = self
                .consume()
                .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input"))?;
            let col_name = if let Token::Identifier(name) = token {
                name
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Expected column name".to_string(),
                ));
            };

            // column data type
            let token = self
                .consume()
                .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "Unexpected end of input"))?;
            let data_type = match token {
                Token::Integer => DataType::Integer,
                Token::Text => DataType::Text,
                Token::Boolean => DataType::Boolean,
                Token::Null => DataType::Null,
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Expected data type".to_string(),
                    ));
                }
            };

            // extract is_primary_key
            let is_primary_key = match self.peek() {
                Some(Token::Primary) => {
                    self.advance();
                    self.expect(Token::Key)?;
                    // TODO: create index
                    true
                }
                _ => false,
            };

            columns.push(Column::new(&col_name, data_type, is_primary_key));

            match self.peek() {
                Some(&Token::Comma) => {
                    self.advance();
                    continue;
                }
                Some(&Token::RightParen) => {
                    self.advance();
                    break;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Expected ',' or ')' after column definition",
                    ));
                }
            }
        }

        self.expect(Token::Semicolon)?;

        Ok(Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    pub fn parse_insert(&mut self) -> io::Result<Statement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        // extract table name
        let table_name = self.get_table_name()?;
        self.expect(Token::Values)?;
        self.expect(Token::LeftParen)?;

        // extract values
        let mut values = Vec::new();
        loop {
            match self.consume() {
                Some(Token::NumberLiteral(num)) => values.push(Value::Integer(num)),
                Some(Token::StringLiteral(s)) => values.push(Value::Text(s)),
                Some(Token::BoolLiteral(bool)) => values.push(Value::Boolean(bool)),
                Some(Token::Null) => values.push(Value::Null),
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Expected value literals",
                    ));
                }
            }

            match self.peek() {
                Some(Token::Comma) => {
                    self.advance();
                    continue;
                }
                Some(Token::RightParen) => {
                    self.advance();
                    break;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Expected ',' or ')' after value",
                    ));
                }
            }
        }

        self.expect(Token::Semicolon)?;

        Ok(Statement::Insert { table_name, values })
    }

    fn parse_select(&mut self) -> io::Result<Statement> {
        self.expect(Token::Select)?;

        // Check if it's * or column list
        let columns = match self.peek() {
            Some(Token::Asterisk) => {
                self.advance();
                SelectColumns::All
            }
            Some(Token::Identifier(_)) => {
                // Parse column list: id, name, etc.
                let mut col_names = Vec::new();

                loop {
                    // Get column name
                    match self.consume() {
                        Some(Token::Identifier(name)) => col_names.push(name),
                        _ => {
                            return Err(Error::new(ErrorKind::InvalidData, "Expected column name"));
                        }
                    }

                    // Check for comma (more columns) or FROM (done)
                    match self.peek() {
                        Some(Token::Comma) => {
                            self.advance();
                            continue;
                        }
                        Some(Token::From) => break,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Expected ',' or 'FROM'",
                            ));
                        }
                    }
                }

                SelectColumns::Specific(col_names)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Expected '*' or column names after SELECT",
                ));
            }
        };

        self.expect(Token::From)?;
        let table_name = self.get_table_name()?;

        let where_clause = if matches!(self.peek(), Some(Token::Where)) {
            self.advance(); // consume WHERE token
            match self.parse_or_expr() {
                Ok(exp) => Some(exp),
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            None
        };

        self.expect(Token::Semicolon)?;

        Ok(Statement::Select {
            table_name,
            columns,
            where_clause,
        })
    }

    /// for parsing the operands, for determining the values/columns in a comparison
    /// It is of the highest precedence
    fn parse_operand(&mut self) -> io::Result<Expr> {
        match self.consume() {
            Some(Token::Identifier(c)) => Ok(Expr::Column(c)),
            Some(Token::NumberLiteral(n)) => Ok(Expr::Literal(Value::Integer(n))),
            Some(Token::StringLiteral(s)) => Ok(Expr::Literal(Value::Text(s))),
            Some(Token::BoolLiteral(b)) => Ok(Expr::Literal(Value::Boolean(b))),
            Some(Token::Null) => Ok(Expr::Literal(Value::Null)),
            _ => Err(Error::new(ErrorKind::InvalidData, "Expected an operand")),
        }
    }

    /// for building a comparison Expr
    /// left == operand, op == Binary Operator, right == operand
    fn parse_comparison(&mut self) -> io::Result<Expr> {
        let left = self.parse_operand()?;

        let op = match self.peek() {
            Some(Token::Equals) => BinaryOperator::Equals,
            Some(Token::NotEquals) => BinaryOperator::NotEquals,
            Some(Token::LessThan) => BinaryOperator::LessThan,
            Some(Token::GreaterThan) => BinaryOperator::GreaterThan,
            Some(Token::LessOrEqual) => BinaryOperator::LessOrEqual,
            Some(Token::GreaterOrEqual) => BinaryOperator::GreaterOrEqual,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Expected a comparison operator",
                ));
            }
        };

        self.advance();
        let right = self.parse_operand()?;

        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    /// for parsing an AND expr
    /// calls `parse_comparison` to build the left and right Expr for an AND Expr
    fn parse_and_expr(&mut self) -> io::Result<Expr> {
        let mut left = self.parse_comparison()?;

        while matches!(self.peek(), Some(Token::And)) {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// entry point for parsing where clause,
    /// as it is of the lowest precedence so it get's evaluated last
    /// it calls a higher precedence function `parse_and_expr`
    fn parse_or_expr(&mut self) -> io::Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while matches!(self.peek(), Some(Token::Or)) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_drop_table(&mut self) -> io::Result<Statement> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;

        let table_name = self.get_table_name()?;

        self.expect(Token::Semicolon)?;

        Ok(Statement::DropTable { name: table_name })
    }

    fn parse_delete(&mut self) -> io::Result<Statement> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table_name = self.get_table_name()?;

        let where_clause = if matches!(self.peek(), Some(Token::Where)) {
            self.advance();
            match self.parse_or_expr() {
                Ok(e) => Some(e),
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            None
        };

        self.expect(Token::Semicolon)?;

        Ok(Statement::Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_update(&mut self) -> io::Result<Statement> {
        self.expect(Token::Update)?;
        let table_name = self.get_table_name()?;
        self.expect(Token::Set)?;

        let mut assignments: Vec<(String, Value)> = Vec::new();

        loop {
            match self.peek() {
                Some(Token::Identifier(_)) => {
                    let col_name = match self.consume() {
                        Some(Token::Identifier(n)) => n,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Expected a column name",
                            ));
                        }
                    };

                    self.expect(Token::Equals)?;

                    let assignment = match self.consume() {
                        Some(Token::StringLiteral(s)) => Value::Text(s),
                        Some(Token::NumberLiteral(n)) => Value::Integer(n),
                        Some(Token::BoolLiteral(b)) => Value::Boolean(b),
                        Some(Token::Null) => Value::Null,
                        _ => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                format!("Expected a value for {}", col_name),
                            ));
                        }
                    };

                    assignments.push((col_name, assignment));
                }
                Some(Token::Comma) => {
                    self.advance();
                    continue;
                }
                Some(Token::Where) | Some(Token::Semicolon) => {
                    break;
                }
                _ => {
                    return Err(Error::new(ErrorKind::InvalidData, "Expected a column name"));
                }
            }
        }

        let where_clause = if matches!(self.peek(), Some(Token::Where)) {
            self.advance();
            match self.parse_or_expr() {
                Ok(e) => Some(e),
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            None
        };

        self.expect(Token::Semicolon)?;

        Ok(Statement::Update {
            table_name,
            assignments,
            where_clause,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::tokenizer::tokenize;

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INTEGER, name TEXT);";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name(), "id");
                assert_eq!(columns[1].name(), "name");
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_single_column() {
        let sql = "CREATE TABLE products (name TEXT);";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "products");
                assert_eq!(columns.len(), 1);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice', true);";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Insert { table_name, values } => {
                assert_eq!(table_name, "users");
                assert_eq!(values.len(), 3);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_select_all() {
        let sql = "SELECT * FROM users;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select {
                table_name,
                columns,
                where_clause: None,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns, SelectColumns::All);
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_specific() {
        let sql = "SELECT id, name FROM users;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select {
                table_name,
                columns,
                where_clause: None,
            } => {
                assert_eq!(table_name, "users");
                match columns {
                    SelectColumns::Specific(cols) => {
                        assert_eq!(cols.len(), 2);
                        assert_eq!(cols[0], "id");
                        assert_eq!(cols[1], "name");
                    }
                    _ => panic!("Expected specific columns"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_simple_equals() {
        let sql = "SELECT * FROM users WHERE id = 1;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select {
                table_name,
                where_clause,
                ..
            } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());

                match where_clause.unwrap() {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(*left, Expr::Column(_)));
                        assert_eq!(op, BinaryOperator::Equals);
                        assert!(matches!(*right, Expr::Literal(_)));
                    }
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_all_comparison_operators() {
        let test_cases = vec![
            ("SELECT * FROM users WHERE id = 1;", BinaryOperator::Equals),
            (
                "SELECT * FROM users WHERE id != 1;",
                BinaryOperator::NotEquals,
            ),
            (
                "SELECT * FROM users WHERE id < 1;",
                BinaryOperator::LessThan,
            ),
            (
                "SELECT * FROM users WHERE id > 1;",
                BinaryOperator::GreaterThan,
            ),
            (
                "SELECT * FROM users WHERE id <= 1;",
                BinaryOperator::LessOrEqual,
            ),
            (
                "SELECT * FROM users WHERE id >= 1;",
                BinaryOperator::GreaterOrEqual,
            ),
        ];

        for (sql, expected_op) in test_cases {
            let tokens = tokenize(sql).unwrap();
            let mut parser = Parser::new(tokens);
            let statement = parser.parse().unwrap();

            match statement {
                Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                    Expr::BinaryOp { op, .. } => {
                        assert_eq!(op, expected_op, "Failed for SQL: {}", sql);
                    }
                    _ => panic!("Expected BinaryOp for SQL: {}", sql),
                },
                _ => panic!("Expected Select statement for SQL: {}", sql),
            }
        }
    }

    #[test]
    fn test_parse_where_with_text_literal() {
        let sql = "SELECT * FROM users WHERE name = 'Alice';";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { left, right, .. } => match (*left, *right) {
                    (Expr::Column(col), Expr::Literal(Value::Text(text))) => {
                        assert_eq!(col, "name");
                        assert_eq!(text, "Alice");
                    }
                    _ => panic!("Expected Column and Text literal"),
                },
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_with_boolean() {
        let sql = "SELECT * FROM users WHERE active = true;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { right, .. } => {
                    assert!(matches!(*right, Expr::Literal(Value::Boolean(true))));
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_simple_and() {
        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'Alice';";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { op, left, right } => {
                    assert_eq!(op, BinaryOperator::And);
                    // Left should be "id = 1"
                    assert!(matches!(
                        *left,
                        Expr::BinaryOp {
                            op: BinaryOperator::Equals,
                            ..
                        }
                    ));
                    // Right should be "name = 'Alice'"
                    assert!(matches!(*right, Expr::BinaryOp { .. }));
                }
                _ => panic!("Expected BinaryOp with AND"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_simple_or() {
        let sql = "SELECT * FROM users WHERE id = 1 OR id = 2;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { op, left, right } => {
                    assert_eq!(op, BinaryOperator::Or);
                    assert!(matches!(*left, Expr::BinaryOp { .. }));
                    assert!(matches!(*right, Expr::BinaryOp { .. }));
                }
                _ => panic!("Expected BinaryOp with OR"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_multiple_and() {
        let sql = "SELECT * FROM users WHERE id > 1 AND age < 30 AND active = true;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => {
                assert!(where_clause.is_some());
                // Should have nested AND operations
                match where_clause.unwrap() {
                    Expr::BinaryOp { op, .. } => {
                        assert_eq!(op, BinaryOperator::And);
                    }
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_multiple_or() {
        let sql = "SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { op, .. } => {
                    assert_eq!(op, BinaryOperator::Or);
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_and_or_precedence() {
        // Should parse as: id = 1 OR (age > 18 AND active = true)
        let sql = "SELECT * FROM users WHERE id = 1 OR age > 18 AND active = true;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { op, left, right } => {
                    // Top level should be OR
                    assert_eq!(op, BinaryOperator::Or);

                    // Left should be simple comparison "id = 1"
                    match *left {
                        Expr::BinaryOp { op, .. } => {
                            assert_eq!(op, BinaryOperator::Equals);
                        }
                        _ => panic!("Expected Equals comparison on left"),
                    }

                    // Right should be AND expression
                    match *right {
                        Expr::BinaryOp { op, .. } => {
                            assert_eq!(op, BinaryOperator::And);
                        }
                        _ => panic!("Expected AND on right"),
                    }
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_complex_precedence() {
        // Should parse as: (id = 1 AND age > 18) OR (name = 'Alice' AND active = true)
        let sql =
            "SELECT * FROM users WHERE id = 1 AND age > 18 OR name = 'Alice' AND active = true;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { op, left, right } => {
                    // Top level should be OR
                    assert_eq!(op, BinaryOperator::Or);

                    // Both sides should be AND expressions
                    match (*left, *right) {
                        (
                            Expr::BinaryOp { op: left_op, .. },
                            Expr::BinaryOp { op: right_op, .. },
                        ) => {
                            assert_eq!(left_op, BinaryOperator::And);
                            assert_eq!(right_op, BinaryOperator::And);
                        }
                        _ => panic!("Expected AND on both sides"),
                    }
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_without_where() {
        let sql = "SELECT * FROM users;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => {
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_with_specific_columns() {
        let sql = "SELECT id, name FROM users WHERE age > 18;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select {
                columns,
                where_clause,
                ..
            } => {
                // Check columns
                match columns {
                    SelectColumns::Specific(cols) => {
                        assert_eq!(cols.len(), 2);
                        assert_eq!(cols[0], "id");
                        assert_eq!(cols[1], "name");
                    }
                    _ => panic!("Expected specific columns"),
                }

                // Check WHERE clause
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_with_null() {
        let sql = "SELECT * FROM users WHERE data = NULL;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { right, .. } => {
                    assert!(matches!(*right, Expr::Literal(Value::Null)));
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_negative_number() {
        let sql = "SELECT * FROM users WHERE balance < -100;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select { where_clause, .. } => match where_clause.unwrap() {
                Expr::BinaryOp { right, .. } => match *right {
                    Expr::Literal(Value::Integer(n)) => {
                        assert_eq!(n, -100);
                    }
                    _ => panic!("Expected negative integer"),
                },
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_error_missing_operand() {
        let sql = "SELECT * FROM users WHERE id =;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse();

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_where_error_missing_operator() {
        let sql = "SELECT * FROM users WHERE id 1;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse();

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_where_error_invalid_token() {
        let sql = "SELECT * FROM users WHERE * = 1;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let result = parser.parse();

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_drop_table() {
        let sql = "DROP TABLE users;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::DropTable { name } => {
                assert_eq!(name, "users");
            }
            _ => panic!("Expected DropTable statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let sql = "DELETE FROM users;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Delete {
                table_name,
                where_clause,
            } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_delete_with_where_clause() {
        let sql = "DELETE FROM users WHERE id = 1;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Delete {
                table_name,
                where_clause,
            } => {
                assert_eq!(table_name, "users");
                assert!(where_clause.is_some());

                match where_clause.unwrap() {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(*left, Expr::Column(_)));
                        assert_eq!(op, BinaryOperator::Equals);
                        assert!(matches!(*right, Expr::Literal(_)));
                    }
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_update_with_where_clause() {
        let sql = "UPDATE users SET age = 30 WHERE id = 1;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => {
                assert_eq!(table_name, "users");

                let (col, value) = &assignments[0];
                assert_eq!(col, "age");
                assert_eq!(value, &Value::Integer(30));

                assert!(where_clause.is_some());

                match where_clause.unwrap() {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(*left, Expr::Column(_)));
                        assert_eq!(op, BinaryOperator::Equals);
                        assert!(matches!(*right, Expr::Literal(_)));
                    }
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_update_double_columns() {
        let sql = "UPDATE users SET name = 'Bob', age = 25 WHERE id = 2;";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Update {
                table_name,
                assignments,
                where_clause,
            } => {
                assert_eq!(table_name, "users");

                let (col, value) = &assignments[0];
                assert_eq!(col, "name");
                assert_eq!(value, &Value::Text("Bob".to_string()));
                let (col, value) = &assignments[1];
                assert_eq!(col, "age");
                assert_eq!(value, &Value::Integer(25));

                assert!(where_clause.is_some());

                match where_clause.unwrap() {
                    Expr::BinaryOp { left, op, right } => {
                        assert!(matches!(*left, Expr::Column(_)));
                        assert_eq!(op, BinaryOperator::Equals);
                        assert!(matches!(*right, Expr::Literal(_)));
                    }
                    _ => panic!("Expected BinaryOp"),
                }
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_create_table_with_primary_key() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        let tokens = tokenize(sql).unwrap();
        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name(), "id");
                assert_eq!(columns[0].is_primary_key(), true);
                assert_eq!(columns[1].name(), "name");
                assert_eq!(columns[1].is_primary_key(), false);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }
}
