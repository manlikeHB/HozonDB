use crate::storage::page::PageManager;
use std::io::{self, Write};

pub struct Repl {
    db: Option<PageManager>,
}

impl Repl {
    pub fn new() -> Self {
        Repl { db: None }
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
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return Ok(());
        }

        let command = parts[0];

        match command {
            ".help" => self.cmd_help(),
            ".open" => self.cmd_open(&parts),
            ".pages" => self.cmd_pages(),
            ".allocate" => self.cmd_allocate(),
            ".read" => self.cmd_read(&parts),
            ".write" => self.cmd_write(&parts),
            ".dump" => self.cmd_dump(&parts),
            _ => {
                eprintln!("Unknown command: '{}'. Type '.help' for usage.", command);
                Ok(())
            }
        }
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
        let pm = PageManager::new(filename)?;
        let page_count = pm.num_pages();
        self.db = Some(pm);
        println!("Opened database file: {} ({} pages)", filename, page_count);
        Ok(())
    }

    fn cmd_pages(&self) -> io::Result<()> {
        let db = match Self::get_db(&self) {
            Ok(db) => db,
            Err(_) => return Ok(()),
        };
        println!("Total pages: {}", db.num_pages());

        Ok(())
    }

    fn cmd_allocate(&mut self) -> io::Result<()> {
        let db = match Self::get_db_mut(self) {
            Ok(db) => db,
            Err(_) => return Ok(()),
        };
        let page_id = db.allocate_page()?;
        println!("Allocated new page with ID: {}", page_id);
        Ok(())
    }

    fn cmd_write(&mut self, parts: &[&str]) -> io::Result<()> {
        let db = match self.get_db_mut() {
            Ok(db) => db,
            Err(_) => return Ok(()),
        };

        if parts.len() < 3 {
            eprintln!("Usage: .write <page_id> <text>");
            return Ok(());
        }

        let page_id: u32 = match parts[1].parse() {
            Ok(id) => id,
            Err(_) => {
                eprintln!("Invalid page ID: {}", parts[1]);
                return Ok(());
            }
        };

        let data = parts[2..].join(" ").into_bytes();
        match db.write_page(page_id, &data) {
            Ok(_) => println!("Wrote {} bytes to page {}", data.len(), page_id),
            Err(e) => eprintln!("Error writing to page {}: {}", page_id, e),
        }

        Ok(())
    }

    fn cmd_read(&mut self, parts: &[&str]) -> io::Result<()> {
        let db = match self.get_db() {
            Ok(db) => db,
            Err(_) => return Ok(()),
        };

        if parts.len() != 2 {
            eprintln!("Usage: .read <page_id>");
            return Ok(());
        };

        let page_id: u32 = match parts[1].parse() {
            Ok(id) => id,
            Err(_) => {
                eprintln!("Invalid page ID: {}", parts[1]);
                return Ok(());
            }
        };

        match db.read_page(page_id) {
            Ok(data) => {
                for (i, byte) in data.iter().enumerate() {
                    if byte.eq(&0_u8) {
                        let text = &data[..=i];
                        println!("Page {} data: {}", page_id, String::from_utf8_lossy(text));
                        break;
                    };
                }
            }
            Err(e) => eprintln!("Error reading page {}: {}", page_id, e),
        }

        Ok(())
    }

    fn cmd_dump(&mut self, parts: &[&str]) -> io::Result<()> {
        todo!("Implement .dump")
    }

    fn get_db(&self) -> io::Result<&PageManager> {
        match self.db.as_ref() {
            Some(db) => Ok(db),
            None => {
                eprintln!("No database is open. Use '.open <file>' to open a database.");
                Err(io::Error::new(io::ErrorKind::Other, "No database open"))
            }
        }
    }

    fn get_db_mut(&mut self) -> io::Result<&mut PageManager> {
        match self.db.as_mut() {
            Some(db) => Ok(db),
            None => {
                eprintln!("No database is open. Use '.open <file>' to open a database.");
                Err(io::Error::new(io::ErrorKind::Other, "No database open"))
            }
        }
    }
}
