use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;

pub struct PageManager {
    file: File,
    num_pages: u32,
}

impl PageManager {
    /// Create a new database file or open existing one
    pub fn new(path: &str) -> io::Result<Self> {
        if Path::new(path).exists() {
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;

            // Go to start of file
            file.seek(SeekFrom::Start(0))?;

            // Read magic number
            let mut magic_bytes = [0u8; 4];
            file.read_exact(&mut magic_bytes)?;
            let magic_number = u32::from_le_bytes(magic_bytes);

            if magic_number != 0x484F5A4E {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid magic number",
                ));
            }

            // Read number of pages
            let mut num_pages_bytes = [0u8; 4];
            file.read_exact(&mut num_pages_bytes)?;
            let num_pages = u32::from_le_bytes(num_pages_bytes);

            Ok(PageManager {
                file,
                num_pages: num_pages,
            })
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            let mut headers = [0u8; PAGE_SIZE];
            headers[0..4].copy_from_slice(&0x484F5A4E_u32.to_le_bytes());
            headers[4..8].copy_from_slice(&1u32.to_le_bytes());
            file.write_all(&headers)?;

            Ok(PageManager { file, num_pages: 1 })
        }
    }

    /// Allocate a new page and return its ID
    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        let page_id: PageId = self.num_pages;
        self.num_pages += 1;

        let new_size = (self.num_pages as u64) * (PAGE_SIZE as u64);
        self.file.set_len(new_size)?;

        let num_pages_bytes = self.num_pages.to_le_bytes();
        self.file.seek(SeekFrom::Start(4))?;
        self.file.write_all(&num_pages_bytes)?;

        Ok(page_id)
    }

    /// Write data to a specific page
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        todo!("Implement this!")
    }

    /// Read data from a specific page
    pub fn read_page(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        todo!("Implement this!")
    }

    /// Get total number of pages
    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_page_manager_new() {
        // Clean up any existing test file
        let _ = fs::remove_file("test.db");

        // Test creating new database
        let pm = PageManager::new("test.db");
        assert!(pm.is_ok());
        assert_eq!(pm.unwrap().num_pages(), 1);

        // Test opening existing database
        let pm2 = PageManager::new("test.db");
        assert!(pm2.is_ok());
        assert_eq!(pm2.unwrap().num_pages(), 1);

        // Clean up
        let _ = fs::remove_file("test.db");
    }

    #[test]
    fn test_allocate_page() {
        let _ = fs::remove_file("test_alloc.db");

        let mut pm = PageManager::new("test_alloc.db").unwrap();
        assert_eq!(pm.num_pages(), 1);

        // Allocate first page
        let page_id = pm.allocate_page().unwrap();
        assert_eq!(page_id, 1); // Page 0 is header, so first data page is 1
        assert_eq!(pm.num_pages(), 2);

        // Allocate second page
        let page_id = pm.allocate_page().unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pm.num_pages(), 3);

        // Close and reopen - should remember the page count
        drop(pm);
        let pm = PageManager::new("test_alloc.db").unwrap();
        assert_eq!(pm.num_pages(), 3);

        let _ = fs::remove_file("test_alloc.db");
    }
}
