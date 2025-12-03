use crate::catalog::{row::Value, table::TableCatalog};
use crate::sql::{
    executor::{ExecutionResult, Executor},
    parser::Parser,
    tokenizer::{self},
};
use crate::storage::page::PageManager;
use std::io::{self, Write};

pub struct Repl {
    executor: Option<Executor>,
}

impl Repl {
    pub fn new() -> Self {
        Repl { executor: None }
    }

    pub fn run(&mut self) {
        println!("HozonDB v0.1.0");
        println!("Enter '.help' for usage hints.");
        println!();

        loop {
            print!("hozondb> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            if io::stdin().read_line(&mut input).is_err() {
                eprintln!("Error reading input");
                continue;
            }

            let input = input.trim();

            if input.len() == 0 {
                continue;
            }

            if input == ".exit" || input == ".quit" {
                println!("Exiting HozonDB. Goodbye!");
                break;
            }

            if let Err(e) = self.execute_command(input) {
                eprintln!("Error: {}", e);
            }
        }
    }

    pub fn execute_command(&mut self, command: &str) -> io::Result<()> {
        if command.starts_with(".") {
            self.execute_meta_command(command)
        } else {
            self.execute_sql_command(command)
        }
    }

    fn execute_meta_command(&mut self, command: &str) -> io::Result<()> {
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return Ok(());
        }

        let command = parts[0];

        match command {
            ".help" => self.cmd_help(),
            ".open" => self.cmd_open(&parts),
            _ => {
                eprintln!("Unknown command: '{}'. Type '.help' for usage.", command);
                Ok(())
            }
        }
    }

    fn execute_sql_command(&mut self, sql: &str) -> io::Result<()> {
        // check if database is open
        let executor = match self.executor.as_mut() {
            Some(exec) => exec,
            None => {
                eprintln!("No database is open. Use '.open <file>' first.");
                return Ok(());
            }
        };

        let tokens = tokenizer::tokenize(sql)?;
        let statement = Parser::new(tokens).parse()?;
        let res = executor.execute(statement)?;

        match res {
            ExecutionResult::Success { message } => {
                println!("{}", message);
            }
            ExecutionResult::Rows { columns, rows } => {
                for c in columns {
                    print!("| {c} ");
                }
                println!("|");

                for row in rows {
                    for r in row.values() {
                        match r {
                            Value::Integer(int) => print!("| {:?} ", int),
                            Value::Text(s) => print!("| {:?} ", s),
                            Value::Boolean(b) => print!("| {:?} ", b),
                            Value::Null => print!("| {} ", "Null".to_string()),
                        }
                    }
                    println!("|");
                }
            }
        }
        Ok(())
    }

    fn cmd_help(&self) -> io::Result<()> {
        println!("Available commands:");
        println!("  .help              - Show this help message");
        println!("  .open <file>       - Open or create a database file");
        println!("  .exit              - Exit the program");
        Ok(())
    }

    fn cmd_open(&mut self, parts: &[&str]) -> io::Result<()> {
        if parts.len() != 2 {
            eprintln!("Usage: .open <file>");
            return Ok(());
        }

        let filename = parts[1];

        // create new executor
        let pm = PageManager::new(filename)?;
        let catalog = TableCatalog::new(pm)?;
        let executor = Executor::new(catalog);
        self.executor = Some(executor);

        println!("Opened database file: {}", filename);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // Helper to clean up test files
    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    #[test]
    fn test_new_repl_has_no_executor() {
        let repl = Repl::new();
        assert!(repl.executor.is_none());
    }

    #[test]
    fn test_open_creates_executor() {
        cleanup("test_repl_open");

        let mut repl = Repl::new();
        let result = repl.execute_command(".open test_repl_open.hdb");

        assert!(result.is_ok());
        assert!(repl.executor.is_some());

        cleanup("test_repl_open");
    }

    #[test]
    fn test_open_existing_database() {
        cleanup("test_repl_existing");

        // Create database first
        {
            let mut repl = Repl::new();
            repl.execute_command(".open test_repl_existing.hdb")
                .unwrap();
            repl.execute_command("CREATE TABLE users (id INTEGER);")
                .unwrap();
        }

        // Open existing
        {
            let mut repl = Repl::new();
            let result = repl.execute_command(".open test_repl_existing.hdb");
            assert!(result.is_ok());
            assert!(repl.executor.is_some());
        }

        cleanup("test_repl_existing");
    }

    #[test]
    fn test_open_without_filename() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".open");

        assert!(result.is_ok()); // Doesn't error, just prints usage
        assert!(repl.executor.is_none());
    }

    #[test]
    fn test_sql_without_open_database() {
        let mut repl = Repl::new();
        let result = repl.execute_command("CREATE TABLE users (id INTEGER);");

        assert!(result.is_ok()); // Handles gracefully, prints error message
    }

    #[test]
    fn test_create_table() {
        cleanup("test_repl_create");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_create.hdb").unwrap();

        let result = repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);");
        assert!(result.is_ok());

        cleanup("test_repl_create");
    }

    #[test]
    fn test_insert_row() {
        cleanup("test_repl_insert");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_insert.hdb").unwrap();
        repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();

        let result = repl.execute_command("INSERT INTO users VALUES (1, 'Alice');");
        assert!(result.is_ok());

        cleanup("test_repl_insert");
    }

    #[test]
    fn test_select_all() {
        cleanup("test_repl_select");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_select.hdb").unwrap();
        repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        repl.execute_command("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        repl.execute_command("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();

        let result = repl.execute_command("SELECT * FROM users;");
        assert!(result.is_ok());

        cleanup("test_repl_select");
    }

    #[test]
    fn test_select_specific_columns() {
        cleanup("test_repl_select_cols");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_select_cols.hdb")
            .unwrap();
        repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT, email TEXT);")
            .unwrap();
        repl.execute_command("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com');")
            .unwrap();

        let result = repl.execute_command("SELECT name, id FROM users;");
        assert!(result.is_ok());

        cleanup("test_repl_select_cols");
    }

    #[test]
    fn test_invalid_sql_syntax() {
        cleanup("test_repl_invalid");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_invalid.hdb").unwrap();

        // Missing semicolon
        let result = repl.execute_command("CREATE TABLE users (id INTEGER)");
        assert!(result.is_err());

        cleanup("test_repl_invalid");
    }

    #[test]
    fn test_insert_wrong_type() {
        cleanup("test_repl_type_err");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_type_err.hdb")
            .unwrap();
        repl.execute_command("CREATE TABLE users (id INTEGER);")
            .unwrap();

        // Try to insert text into integer column
        let result = repl.execute_command("INSERT INTO users VALUES ('not a number');");
        assert!(result.is_err());

        cleanup("test_repl_type_err");
    }

    #[test]
    fn test_select_nonexistent_table() {
        cleanup("test_repl_no_table");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_no_table.hdb")
            .unwrap();

        let result = repl.execute_command("SELECT * FROM nonexistent;");
        assert!(result.is_err());

        cleanup("test_repl_no_table");
    }

    #[test]
    fn test_multiple_operations() {
        cleanup("test_repl_multi");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_multi.hdb").unwrap();

        // Create
        repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();

        // Insert multiple
        repl.execute_command("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        repl.execute_command("INSERT INTO users VALUES (2, 'Bob');")
            .unwrap();
        repl.execute_command("INSERT INTO users VALUES (3, 'Charlie');")
            .unwrap();

        // Select
        let result = repl.execute_command("SELECT * FROM users;");
        assert!(result.is_ok());

        cleanup("test_repl_multi");
    }

    #[test]
    fn test_case_insensitive_keywords() {
        cleanup("test_repl_case");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_case.hdb").unwrap();

        // Lowercase keywords should work
        repl.execute_command("create table users (id integer);")
            .unwrap();
        repl.execute_command("insert into users values (1);")
            .unwrap();
        let result = repl.execute_command("select * from users;");
        assert!(result.is_ok());

        cleanup("test_repl_case");
    }

    #[test]
    fn test_empty_input() {
        let mut repl = Repl::new();
        let result = repl.execute_command("");

        assert!(result.is_ok()); // Should handle gracefully
    }

    #[test]
    fn test_whitespace_only() {
        let mut repl = Repl::new();
        let result = repl.execute_command("   ");

        assert!(result.is_ok()); // Should handle gracefully
    }

    #[test]
    fn test_unknown_meta_command() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".unknown");

        assert!(result.is_ok()); // Doesn't error, just prints message
    }

    #[test]
    fn test_help_command() {
        let repl = Repl::new();
        let result = repl.cmd_help();

        assert!(result.is_ok());
    }

    #[test]
    fn test_persistence_across_sessions() {
        cleanup("test_repl_persist");

        // First session: create and insert
        {
            let mut repl = Repl::new();
            repl.execute_command(".open test_repl_persist.hdb").unwrap();
            repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);")
                .unwrap();
            repl.execute_command("INSERT INTO users VALUES (1, 'Alice');")
                .unwrap();
        } // Repl dropped, files closed

        // Second session: verify data persists
        {
            let mut repl = Repl::new();
            repl.execute_command(".open test_repl_persist.hdb").unwrap();

            // Should be able to select from existing table
            let result = repl.execute_command("SELECT * FROM users;");
            assert!(result.is_ok());

            // Should be able to insert more data
            let result = repl.execute_command("INSERT INTO users VALUES (2, 'Bob');");
            assert!(result.is_ok());
        }

        cleanup("test_repl_persist");
    }

    #[test]
    fn test_multiple_tables() {
        cleanup("test_repl_multi_tables");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_multi_tables.hdb")
            .unwrap();

        // Create multiple tables
        repl.execute_command("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        repl.execute_command("CREATE TABLE orders (id INTEGER, user_id INTEGER);")
            .unwrap();

        // Insert into both
        repl.execute_command("INSERT INTO users VALUES (1, 'Alice');")
            .unwrap();
        repl.execute_command("INSERT INTO orders VALUES (100, 1);")
            .unwrap();

        // Select from both
        let result1 = repl.execute_command("SELECT * FROM users;");
        let result2 = repl.execute_command("SELECT * FROM orders;");

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        cleanup("test_repl_multi_tables");
    }

    #[test]
    fn test_all_data_types() {
        cleanup("test_repl_types");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_types.hdb").unwrap();

        repl.execute_command(
            "CREATE TABLE test (int_col INTEGER, text_col TEXT, bool_col BOOLEAN, null_col NULL);",
        )
        .unwrap();

        repl.execute_command("INSERT INTO test VALUES (42, 'hello', true, NULL);")
            .unwrap();

        let result = repl.execute_command("SELECT * FROM test;");
        assert!(result.is_ok());

        cleanup("test_repl_types");
    }

    #[test]
    fn test_sql_with_extra_whitespace() {
        cleanup("test_repl_whitespace");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_whitespace.hdb")
            .unwrap();

        // SQL with extra spaces and newlines
        let result = repl.execute_command("  CREATE   TABLE   users  (id INTEGER)  ;  ");
        assert!(result.is_ok());

        cleanup("test_repl_whitespace");
    }

    #[test]
    fn test_insert_empty_string() {
        cleanup("test_repl_empty_str");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_empty_str.hdb")
            .unwrap();
        repl.execute_command("CREATE TABLE users (name TEXT);")
            .unwrap();

        let result = repl.execute_command("INSERT INTO users VALUES ('');");
        assert!(result.is_ok());

        cleanup("test_repl_empty_str");
    }

    #[test]
    fn test_insert_special_characters() {
        cleanup("test_repl_special");

        let mut repl = Repl::new();
        repl.execute_command(".open test_repl_special.hdb").unwrap();
        repl.execute_command("CREATE TABLE users (name TEXT);")
            .unwrap();

        // Test various special characters in strings
        let result = repl.execute_command("INSERT INTO users VALUES ('Hello, World!');");
        assert!(result.is_ok());

        cleanup("test_repl_special");
    }
}
