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
            // ".pages" => self.cmd_pages(),
            // ".allocate" => self.cmd_allocate(),
            // ".read" => self.cmd_read(&parts),
            // ".write" => self.cmd_write(&parts),
            // ".dump" => self.cmd_dump(&parts),
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
        println!("  .pages             - List all pages in the database");
        println!("  .allocate          - Allocate a new page");
        println!("  .write <id> <text> - Write text to a page");
        println!("  .read <id>         - Read data from a page");
        println!("  .dump <id>         - Show raw bytes of a page (hex)");
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

    // fn cmd_pages(&self) -> io::Result<()> {
    //     let db = match Self::get_db(&self) {
    //         Ok(db) => db,
    //         Err(_) => return Ok(()),
    //     };
    //     println!("Total pages: {}", db.num_pages());

    //     Ok(())
    // }

    // fn cmd_allocate(&mut self) -> io::Result<()> {
    //     let db = match Self::get_db_mut(self) {
    //         Ok(db) => db,
    //         Err(_) => return Ok(()),
    //     };
    //     let page_id = db.allocate_page()?;
    //     println!("Allocated new page with ID: {}", page_id);
    //     Ok(())
    // }

    // fn cmd_write(&mut self, parts: &[&str]) -> io::Result<()> {
    //     let db = match self.get_db_mut() {
    //         Ok(db) => db,
    //         Err(_) => return Ok(()),
    //     };

    //     if parts.len() < 3 {
    //         eprintln!("Usage: .write <page_id> <text>");
    //         return Ok(());
    //     }

    //     let page_id: u32 = match parts[1].parse() {
    //         Ok(id) => id,
    //         Err(_) => {
    //             eprintln!("Invalid page ID: {}", parts[1]);
    //             return Ok(());
    //         }
    //     };

    //     let data = parts[2..].join(" ").into_bytes();
    //     match db.write_page(page_id, &data) {
    //         Ok(_) => println!("Wrote {} bytes to page {}", data.len(), page_id),
    //         Err(e) => eprintln!("Error writing to page {}: {}", page_id, e),
    //     }

    //     Ok(())
    // }

    // fn cmd_read(&mut self, parts: &[&str]) -> io::Result<()> {
    //     let db = match self.get_db() {
    //         Ok(db) => db,
    //         Err(_) => return Ok(()),
    //     };

    //     if parts.len() != 2 {
    //         eprintln!("Usage: .read <page_id>");
    //         return Ok(());
    //     };

    //     let page_id: u32 = match parts[1].parse() {
    //         Ok(id) => id,
    //         Err(_) => {
    //             eprintln!("Invalid page ID: {}", parts[1]);
    //             return Ok(());
    //         }
    //     };

    //     match db.read_page(page_id) {
    //         Ok(data) => {
    //             // Find first null byte (or use full length)
    //             let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    //             let text = &data[..end];

    //             // Try to display as UTF-8, fall back to showing length
    //             match std::str::from_utf8(text) {
    //                 Ok(s) => println!("Page {} data: {}", page_id, s),
    //                 Err(_) => println!(
    //                     "Page {} contains binary data ({} bytes)",
    //                     page_id,
    //                     text.len()
    //                 ),
    //             }
    //         }
    //         Err(e) => eprintln!("Error reading page {}: {}", page_id, e),
    //     }

    //     Ok(())
    // }

    // fn cmd_dump(&mut self, parts: &[&str]) -> io::Result<()> {
    //     let db = match self.get_db_mut() {
    //         Ok(db) => db,
    //         Err(_) => return Ok(()),
    //     };

    //     if parts.len() != 2 {
    //         eprintln!("Usage: .dump <page_id>");
    //         return Ok(());
    //     }

    //     let page_id: u32 = match parts[1].parse() {
    //         Ok(id) => id,
    //         Err(_) => {
    //             eprintln!("Invalid page ID: {}", parts[1]);
    //             return Ok(());
    //         }
    //     };

    //     let data = db.read_page(page_id)?;

    //     println!("Page {} (showing first 256 bytes):", page_id);
    //     for (i, chunk) in data[..256].chunks(16).enumerate() {
    //         print!("{:04x}: ", i * 16);
    //         for byte in chunk {
    //             print!("{:02x} ", byte);
    //         }

    //         // ASCII representation
    //         print!(" |");
    //         for byte in chunk {
    //             if *byte >= 32 && *byte <= 126 {
    //                 print!("{}", *byte as char);
    //             } else {
    //                 print!(".");
    //             }
    //         }
    //         println!("|");
    //     }

    //     Ok(())
    // }

    // fn get_db(&self) -> io::Result<&PageManager> {
    //     match self.db.as_ref() {
    //         Some(db) => Ok(db),
    //         None => {
    //             eprintln!("No database is open. Use '.open <file>' to open a database.");
    //             Err(io::Error::new(io::ErrorKind::Other, "No database open"))
    //         }
    //     }
    // }

    // fn get_db_mut(&mut self) -> io::Result<&mut PageManager> {
    //     match self.db.as_mut() {
    //         Some(db) => Ok(db),
    //         None => {
    //             eprintln!("No database is open. Use '.open <file>' to open a database.");
    //             Err(io::Error::new(io::ErrorKind::Other, "No database open"))
    //         }
    //     }
    // }
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

    // #[test]
    // fn test_new_repl_has_no_database() {
    //     let repl = Repl::new();
    //     assert!(repl.db.is_none());
    // }

    // #[test]
    // fn test_open_creates_database() {
    //     cleanup("test_open");

    //     let mut repl = Repl::new();
    //     let result = repl.execute_command(".open test_open.hdb");

    //     assert!(result.is_ok());
    //     assert!(repl.db.is_some());

    //     // Verify file exists
    //     assert!(std::path::Path::new("test_open.hdb").exists());

    //     cleanup("test_open");
    // }

    // #[test]
    // fn test_open_existing_database() {
    //     cleanup("test_existing");

    //     // Create database first
    //     let mut repl1 = Repl::new();
    //     repl1.execute_command(".open test_existing.hdb").unwrap();
    //     drop(repl1); // Close it

    //     // Open existing
    //     let mut repl2 = Repl::new();
    //     let result = repl2.execute_command(".open test_existing.hdb");

    //     assert!(result.is_ok());
    //     assert!(repl2.db.is_some());

    //     cleanup("test_existing");
    // }

    // #[test]
    // fn test_open_without_filename() {
    //     let mut repl = Repl::new();
    //     let result = repl.execute_command(".open");

    //     // Should succeed but do nothing (prints usage)
    //     assert!(result.is_ok());
    //     assert!(repl.db.is_none());
    // }

    // #[test]
    // fn test_pages_without_open_database() {
    //     let mut repl = Repl::new();
    //     let result = repl.execute_command(".pages");

    //     // Should succeed but print error message
    //     assert!(result.is_ok());
    // }

    // #[test]
    // fn test_pages_shows_count() {
    //     cleanup("test_pages");

    //     let mut repl = Repl::new();
    //     repl.execute_command(".open test_pages.hdb").unwrap();

    //     let result = repl.execute_command(".pages");
    //     assert!(result.is_ok());

    //     cleanup("test_pages");
    // }

    // #[test]
    // fn test_allocate_without_database() {
    //     let mut repl = Repl::new();
    //     let result = repl.execute_command(".allocate");

    //     assert!(result.is_ok()); // Doesn't error, just prints message
    // }

    // #[test]
    // fn test_allocate_page() {
    //     cleanup("test_alloc");

    //     let mut repl = Repl::new();
    //     repl.execute_command(".open test_alloc.hdb").unwrap();

    //     let result = repl.execute_command(".allocate");
    //     assert!(result.is_ok());

    //     // Verify page was allocated
    //     assert_eq!(repl.db.as_ref().unwrap().num_pages(), 2);

    //     cleanup("test_alloc");
    // }

    #[test]
    fn test_write_without_database() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".write 1 test");

        assert!(result.is_ok()); // Handles gracefully
    }

    #[test]
    fn test_write_without_page_id() {
        cleanup("test_write_args");

        let mut repl = Repl::new();
        repl.execute_command(".open test_write_args.hdb").unwrap();

        let result = repl.execute_command(".write");
        assert!(result.is_ok()); // Prints usage

        cleanup("test_write_args");
    }

    #[test]
    fn test_write_invalid_page_id() {
        cleanup("test_write_invalid");

        let mut repl = Repl::new();
        repl.execute_command(".open test_write_invalid.hdb")
            .unwrap();

        let result = repl.execute_command(".write abc test");
        assert!(result.is_ok()); // Prints error about invalid ID

        cleanup("test_write_invalid");
    }

    #[test]
    fn test_write_and_read() {
        cleanup("test_rw");

        let mut repl = Repl::new();
        repl.execute_command(".open test_rw.hdb").unwrap();
        repl.execute_command(".allocate").unwrap();

        // Write data
        let result = repl.execute_command(".write 1 Hello World");
        assert!(result.is_ok());

        // Read should succeed
        let result = repl.execute_command(".read 1");
        assert!(result.is_ok());

        cleanup("test_rw");
    }

    #[test]
    fn test_write_multi_word_text() {
        cleanup("test_multi");

        let mut repl = Repl::new();
        repl.execute_command(".open test_multi.hdb").unwrap();
        repl.execute_command(".allocate").unwrap();

        let result = repl.execute_command(".write 1 This is a longer message");
        assert!(result.is_ok());

        cleanup("test_multi");
    }

    #[test]
    fn test_read_without_database() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".read 1");

        assert!(result.is_ok()); // Handles gracefully
    }

    #[test]
    fn test_read_without_page_id() {
        cleanup("test_read_args");

        let mut repl = Repl::new();
        repl.execute_command(".open test_read_args.hdb").unwrap();

        let result = repl.execute_command(".read");
        assert!(result.is_ok()); // Prints usage

        cleanup("test_read_args");
    }

    #[test]
    fn test_read_invalid_page_id() {
        cleanup("test_read_invalid");

        let mut repl = Repl::new();
        repl.execute_command(".open test_read_invalid.hdb").unwrap();

        let result = repl.execute_command(".read abc");
        assert!(result.is_ok()); // Prints error

        cleanup("test_read_invalid");
    }

    #[test]
    fn test_read_nonexistent_page() {
        cleanup("test_read_none");

        let mut repl = Repl::new();
        repl.execute_command(".open test_read_none.hdb").unwrap();

        let result = repl.execute_command(".read 999");
        assert!(result.is_ok()); // Error handled in command

        cleanup("test_read_none");
    }

    #[test]
    fn test_help_command() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".help");

        assert!(result.is_ok());
    }

    #[test]
    fn test_unknown_command() {
        let mut repl = Repl::new();
        let result = repl.execute_command(".unknown");

        assert!(result.is_ok()); // Prints error message but doesn't fail
    }

    #[test]
    fn test_empty_command() {
        let mut repl = Repl::new();
        let result = repl.execute_command("");

        assert!(result.is_ok()); // Ignores empty
    }

    #[test]
    fn test_command_with_extra_whitespace() {
        cleanup("test_whitespace");

        let mut repl = Repl::new();
        repl.execute_command(".open test_whitespace.hdb").unwrap();
        repl.execute_command(".allocate").unwrap();

        // Multiple spaces between arguments
        let result = repl.execute_command(".write   1    test   data");
        assert!(result.is_ok());

        cleanup("test_whitespace");
    }

    // #[test]
    // fn test_sequential_operations() {
    //     cleanup("test_seq");

    //     let mut repl = Repl::new();

    //     // Open
    //     repl.execute_command(".open test_seq.hdb").unwrap();
    //     assert_eq!(repl.db.as_ref().unwrap().num_pages(), 1);

    //     // Allocate
    //     repl.execute_command(".allocate").unwrap();
    //     assert_eq!(repl.db.as_ref().unwrap().num_pages(), 2);

    //     // Write
    //     repl.execute_command(".write 1 test").unwrap();

    //     // Read
    //     let result = repl.execute_command(".read 1");
    //     assert!(result.is_ok());

    //     cleanup("test_seq");
    // }
}
