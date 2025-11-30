use std::io::{self, Error, ErrorKind};

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // keywords
    Select,
    From,
    Where,
    Create,
    Table,
    Insert,
    Into,
    Values,

    // Data types
    Integer,
    Text,
    Boolean,
    Null,

    // Identifiers and literals
    Identifier(String),    // table names, column names
    NumberLiteral(i32),    // integer values
    StringLiteral(String), // string values
    BoolLiteral(bool),     // true/false

    // Symbols
    Comma,      // ,
    Semicolon,  // ;
    Asterisk,   // *
    LeftParen,  // (
    RightParen, // )
    Equals,     // =

    // Special
    Eof, // End of input
}

pub fn tokenize(str: &str) -> io::Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = str.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\n' | '\t' | '\r' => {
                chars.next(); // skip whitespace
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            ';' => {
                tokens.push(Token::Semicolon);
                chars.next();
            }
            '*' => {
                tokens.push(Token::Asterisk);
                chars.next();
            }
            '(' => {
                tokens.push(Token::LeftParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RightParen);
                chars.next();
            }
            '=' => {
                tokens.push(Token::Equals);
                chars.next();
            }
            '\'' => {
                chars.next(); // consume opening quote
                let mut literal = String::new();

                loop {
                    match chars.next() {
                        Some('\'') => break, // closing quote
                        Some(c) => literal.push(c),
                        None => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Unterminated string literal",
                            ));
                        }
                    }
                }

                tokens.push(Token::StringLiteral(literal));
            }
            '0'..='9' | '-' => {
                let mut num_string = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '-' {
                        num_string.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }

                let value = num_string.parse::<i32>().map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Invalid integer literal: {}", e),
                    )
                })?;
                tokens.push(Token::NumberLiteral(value));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        word.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }

                let word_upper = word.to_uppercase();

                let token = match word_upper.as_str() {
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "CREATE" => Token::Create,
                    "TABLE" => Token::Table,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "INTEGER" => Token::Integer,
                    "TEXT" => Token::Text,
                    "BOOLEAN" => Token::Boolean,
                    "NULL" => Token::Null,
                    "TRUE" => Token::BoolLiteral(true),
                    "FALSE" => Token::BoolLiteral(false),
                    _ => Token::Identifier(word),
                };

                tokens.push(token);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unexpected character: {}", ch),
                ));
            }
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_create_table() {
        let sql = "CREATE TABLE users (id INTEGER);";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
        assert_eq!(tokens[3], Token::LeftParen);
        assert_eq!(tokens[4], Token::Identifier("id".to_string()));
        assert_eq!(tokens[5], Token::Integer);
        assert_eq!(tokens[6], Token::RightParen);
        assert_eq!(tokens[7], Token::Semicolon);
        assert_eq!(tokens[8], Token::Eof);
    }

    #[test]
    fn test_tokenize_insert() {
        let sql = "INSERT INTO users VALUES (42, 'Alice');";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[0], Token::Insert);
        assert_eq!(tokens[1], Token::Into);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
        assert_eq!(tokens[3], Token::Values);
        assert_eq!(tokens[4], Token::LeftParen);
        assert_eq!(tokens[5], Token::NumberLiteral(42));
        assert_eq!(tokens[6], Token::Comma);
        assert_eq!(tokens[7], Token::StringLiteral("Alice".to_string()));
        assert_eq!(tokens[8], Token::RightParen);
        assert_eq!(tokens[9], Token::Semicolon);
    }

    #[test]
    fn test_tokenize_select() {
        let sql = "SELECT * FROM users WHERE id = 1;";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Asterisk);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::Identifier("id".to_string()));
        assert_eq!(tokens[6], Token::Equals);
        assert_eq!(tokens[7], Token::NumberLiteral(1));
        assert_eq!(tokens[8], Token::Semicolon);
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let sql = "create TABLE Users;";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Identifier("Users".to_string())); // Identifier preserves case
    }

    #[test]
    fn test_string_with_spaces() {
        let sql = "INSERT INTO users VALUES ('Hello World');";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[5], Token::StringLiteral("Hello World".to_string()));
    }

    #[test]
    fn test_negative_number() {
        let sql = "VALUES (-42);";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[2], Token::NumberLiteral(-42));
    }

    #[test]
    fn test_unterminated_string() {
        let sql = "INSERT INTO users VALUES ('Alice;";
        let result = tokenize(sql);

        assert!(result.is_err());
    }

    #[test]
    fn test_boolean_literals() {
        let sql = "VALUES (true, false);";
        let tokens = tokenize(sql).unwrap();

        assert_eq!(tokens[2], Token::BoolLiteral(true));
        assert_eq!(tokens[4], Token::BoolLiteral(false));
    }
}
