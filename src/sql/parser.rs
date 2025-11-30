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
    },
}

#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    All,
    Specific(Vec<String>),
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

            columns.push(Column::new(&col_name, data_type));

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
        self.expect(Token::Semicolon)?;

        Ok(Statement::Select {
            table_name,
            columns,
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
}
