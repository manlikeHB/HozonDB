use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;

#[derive(Debug)]
pub struct PageManager {
    file: Mutex<File>,
    lock_path: PathBuf,
    num_pages: u32,
}

impl PageManager {
    /// Create a new database file or open existing one
    ///
    /// # Known Limitation
    /// If the program crashes or is forcefully terminated (Ctrl+C, kill, etc.),
    /// the lock file will remain on disk. To recover:
    /// 1. Ensure no other process is using the database
    /// 2. Manually delete the .lock file: `rm database.hdb.lock`
    /// 3. Re-open the database
    ///
    /// TODO: Implement PID-based stale lock detection
    pub fn new(path: &str) -> io::Result<Self> {
        let lock_path = PathBuf::from(format!("{}.lock", path));

        // try to acquire lock
        Self::acquire_lock(Path::new(&lock_path))?;

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
                file: Mutex::new(file),
                num_pages: num_pages,
                lock_path,
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

            Ok(PageManager {
                file: Mutex::new(file),
                num_pages: 1,
                lock_path,
            })
        }
    }

    /// Try to acquire the lock file
    fn acquire_lock(lock_path: &Path) -> io::Result<()> {
        // Try to create the lock file exclusively
        // This will fail if the file already exists
        match OpenOptions::new()
            .write(true)
            .create_new(true) // Fails if file exists!
            .open(lock_path)
        {
            Ok(_) => {
                // Successfully created lock file
                // We can close it immediately - its existence is the lock
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                // Lock file exists - database is already open
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "Database is already in use. Lock file exists: {}",
                        lock_path.display()
                    ),
                ))
            }
            Err(e) => Err(e), // Some other error
        }
    }

    fn release_lock(lock_path: &Path) -> io::Result<()> {
        if lock_path.exists() {
            std::fs::remove_file(lock_path)?;
        }
        Ok(())
    }

    /// Allocate a new page and return its ID
    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        let page_id: PageId = self.num_pages;
        self.num_pages += 1;

        let new_size = (self.num_pages as u64) * (PAGE_SIZE as u64);
        let num_pages_bytes = self.num_pages.to_le_bytes();

        {
            let mut file = self.file.lock().unwrap();
            file.set_len(new_size)?;
            file.seek(SeekFrom::Start(4))?;
            file.write_all(&num_pages_bytes)?;
        };

        Ok(page_id)
    }

    /// Write data to a specific page
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        // Check page ID validity
        if page_id >= self.num_pages {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid page ID: {} (max: {})", page_id, self.num_pages - 1),
            ));
        }

        // Check that data is not longer than PAGE_SIZE
        if data.len() > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Data length {} exceeds PAGE_SIZE {}", data.len(), PAGE_SIZE),
            ));
        }

        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut buffer = [0u8; PAGE_SIZE];
        buffer[0..data.len()].copy_from_slice(data);

        {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&buffer)?;
            file.sync_all()?;
        };

        Ok(())
    }

    /// Read data from a specific page
    pub fn read_page(&self, page_id: PageId) -> io::Result<[u8; PAGE_SIZE]> {
        // Check page ID validity
        if page_id >= self.num_pages {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid page ID: {} (max: {})", page_id, self.num_pages - 1),
            ));
        }

        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        let mut buf = [0u8; PAGE_SIZE];
        {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(offset as u64))?;
            file.read_exact(&mut buf)?;
        };

        Ok(buf)
    }

    /// Get total number of pages
    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }
}

impl Drop for PageManager {
    fn drop(&mut self) {
        // This runs automatically when PageManager is destroyed
        // Clean up the lock file
        let _ = Self::release_lock(&self.lock_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_page_manager_new() {
        let _ = fs::remove_file("test.db");
        let _ = fs::remove_file("test.db.lock");

        let pm = PageManager::new("test.db");
        assert!(pm.is_ok());
        assert_eq!(pm.unwrap().num_pages(), 1);

        let pm2 = PageManager::new("test.db");
        assert!(pm2.is_ok());
        assert_eq!(pm2.unwrap().num_pages(), 1);

        let _ = fs::remove_file("test.db");
        let _ = fs::remove_file("test.db.lock");
    }

    #[test]
    fn test_allocate_page() {
        let _ = fs::remove_file("test_alloc.db");
        let _ = fs::remove_file("test_alloc.db.lock");

        let mut pm = PageManager::new("test_alloc.db").unwrap();
        assert_eq!(pm.num_pages(), 1);

        let page_id = pm.allocate_page().unwrap();
        assert_eq!(page_id, 1);
        assert_eq!(pm.num_pages(), 2);

        let page_id = pm.allocate_page().unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pm.num_pages(), 3);

        drop(pm);
        let pm = PageManager::new("test_alloc.db").unwrap();
        assert_eq!(pm.num_pages(), 3);

        let _ = fs::remove_file("test_alloc.db");
        let _ = fs::remove_file("test_alloc.db.lock");
    }

    #[test]
    fn test_concurrent_access_prevention() {
        let _ = fs::remove_file("test_lock.db");
        let _ = fs::remove_file("test_lock.db.lock");

        // First connection acquires lock
        let _pm1 = PageManager::new("test_lock.db").unwrap();

        // Second connection should fail
        let pm2 = PageManager::new("test_lock.db");
        assert!(pm2.is_err());
        assert_eq!(pm2.unwrap_err().kind(), io::ErrorKind::WouldBlock);

        // After dropping pm1, lock should be released
        drop(_pm1);

        // Now we should be able to open again
        let pm3 = PageManager::new("test_lock.db");
        assert!(pm3.is_ok());

        let _ = fs::remove_file("test_lock.db");
        let _ = fs::remove_file("test_lock.db.lock");
    }

    #[test]
    fn test_write_and_read_page() {
        let _ = fs::remove_file("test_rw.db");
        let _ = fs::remove_file("test_rw.db.lock");

        let mut pm = PageManager::new("test_rw.db").unwrap();

        // Allocate a page
        let page_id = pm.allocate_page().unwrap();
        assert_eq!(page_id, 1);

        // Write data to the page
        let data = b"Hello, HozonDB!";
        pm.write_page(page_id, data).unwrap();

        // Read it back
        let read_data = pm.read_page(page_id).unwrap();

        // Check that data matches (first 15 bytes)
        assert_eq!(&read_data[0..data.len()], data);

        // Check that rest is zeros (padding)
        assert!(read_data[data.len()..].iter().all(|&b| b == 0));

        let _ = fs::remove_file("test_rw.db");
        let _ = fs::remove_file("test_rw.db.lock");
    }

    #[test]
    fn test_write_full_page() {
        let _ = fs::remove_file("test_full.db");
        let _ = fs::remove_file("test_full.db.lock");

        let mut pm = PageManager::new("test_full.db").unwrap();
        let page_id = pm.allocate_page().unwrap();

        // Write exactly PAGE_SIZE bytes
        let data = [42u8; PAGE_SIZE];
        pm.write_page(page_id, &data).unwrap();

        // Read it back
        let read_data = pm.read_page(page_id).unwrap();
        assert_eq!(read_data, data);

        let _ = fs::remove_file("test_full.db");
        let _ = fs::remove_file("test_full.db.lock");
    }

    #[test]
    fn test_write_invalid_page() {
        let _ = fs::remove_file("test_invalid.db");
        let _ = fs::remove_file("test_invalid.db.lock");

        let mut pm = PageManager::new("test_invalid.db").unwrap();

        // Try to write to non-existent page
        let result = pm.write_page(999, b"data");
        assert!(result.is_err());

        let _ = fs::remove_file("test_invalid.db");
        let _ = fs::remove_file("test_invalid.db.lock");
    }

    #[test]
    fn test_write_oversized_data() {
        let _ = fs::remove_file("test_oversize.db");
        let _ = fs::remove_file("test_oversize.db.lock");

        let mut pm = PageManager::new("test_oversize.db").unwrap();
        let page_id = pm.allocate_page().unwrap();

        // Try to write more than PAGE_SIZE
        let data = vec![1u8; PAGE_SIZE + 1];
        let result = pm.write_page(page_id, &data);
        assert!(result.is_err());

        let _ = fs::remove_file("test_oversize.db");
        let _ = fs::remove_file("test_oversize.db.lock");
    }
}
