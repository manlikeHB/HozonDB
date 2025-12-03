use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;

pub const PAGE_METADATA_SIZE: usize = 5;
pub const PAGE_DATA_START: usize = PAGE_METADATA_SIZE;

// Metadata offsets
const OFFSET_IS_FULL: usize = 0;
const OFFSET_LAST_OFFSET: usize = 1;
const OFFSET_NUM_ROWS: usize = 3;

#[derive(Debug)]
pub struct PageManager {
    file: Mutex<File>,
    lock_path: PathBuf,
    num_pages: u32,
}

#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub is_full: bool,
    pub last_offset: usize,
    pub num_rows: usize,
}

impl PageManager {
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
    ///
    /// Note: Page 0 is reserved for database header and created in new().
    /// This method allocates pages starting from page 1 with initialized metadata.
    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        let page_id: PageId = self.num_pages;
        self.num_pages += 1;

        let new_size = (self.num_pages as u64) * (PAGE_SIZE as u64);
        let num_pages_bytes = self.num_pages.to_le_bytes();

        // Extend db file size and set new number of pages
        {
            let mut file = self.file.lock().unwrap();
            file.set_len(new_size)?;
            file.seek(SeekFrom::Start(4))?;
            file.write_all(&num_pages_bytes)?;
        };

        let mut page_data = [0u8; PAGE_SIZE];

        // page 0 = headers, page 1 = catalog
        if self.num_pages > 2 {
            // Create page buffer with metadata
            Self::init_page_metadata_buffer(&mut page_data);
        }

        // Write initialized page
        self.write_page(page_id, &page_data)?;

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

    fn init_page_metadata_buffer(page_data: &mut [u8; PAGE_SIZE]) {
        page_data[OFFSET_IS_FULL] = 0;
        page_data[OFFSET_LAST_OFFSET..OFFSET_LAST_OFFSET + 2]
            .copy_from_slice(&(PAGE_DATA_START as u16).to_le_bytes());
        page_data[OFFSET_NUM_ROWS..OFFSET_NUM_ROWS + 2].copy_from_slice(&0u16.to_le_bytes());
    }

    /// Read metadata from a page
    pub fn read_page_metadata(&self, page_id: PageId) -> io::Result<PageMetadata> {
        let page_data = self.read_page(page_id)?;
        Ok(Self::read_metadata_from_buffer(&page_data))
    }

    /// Update metadata for a page
    pub fn update_page_metadata(
        &mut self,
        page_id: PageId,
        metadata: &PageMetadata,
    ) -> io::Result<()> {
        let mut page_data = self.read_page(page_id)?;
        Self::update_metadata_in_buffer(&mut page_data, metadata);
        self.write_page(page_id, &page_data)?;
        Ok(())
    }

    pub fn read_metadata_from_buffer(page_data: &[u8; PAGE_SIZE]) -> PageMetadata {
        let is_full = page_data[OFFSET_IS_FULL] != 0;

        let last_offset = u16::from_le_bytes([
            page_data[OFFSET_LAST_OFFSET],
            page_data[OFFSET_LAST_OFFSET + 1],
        ]) as usize;

        let num_rows =
            u16::from_le_bytes([page_data[OFFSET_NUM_ROWS], page_data[OFFSET_NUM_ROWS + 1]])
                as usize;

        PageMetadata {
            is_full,
            last_offset,
            num_rows,
        }
    }

    pub fn update_metadata_in_buffer(page_data: &mut [u8; PAGE_SIZE], metadata: &PageMetadata) {
        page_data[OFFSET_IS_FULL] = if metadata.is_full { 1 } else { 0 };
        page_data[OFFSET_LAST_OFFSET..OFFSET_LAST_OFFSET + 2]
            .copy_from_slice(&(metadata.last_offset as u16).to_le_bytes());
        page_data[OFFSET_NUM_ROWS..OFFSET_NUM_ROWS + 2]
            .copy_from_slice(&(metadata.num_rows as u16).to_le_bytes());
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

    #[test]
    fn test_page_metadata_initialization() {
        let _ = fs::remove_file("test_metadata_init.db");
        let _ = fs::remove_file("test_metadata_init.db.lock");

        let mut pm = PageManager::new("test_metadata_init.db").unwrap();

        let page_id_1 = pm.allocate_page().unwrap(); // page id 1 is for catalog
        assert_eq!(page_id_1, 1);
        // Allocate page 2 (should have initialized metadata)
        let page_id_2 = pm.allocate_page().unwrap();
        assert_eq!(page_id_2, 2);

        // Read metadata
        let metadata = pm.read_page_metadata(page_id_2).unwrap();

        // Check initial values
        assert_eq!(metadata.is_full, false);
        assert_eq!(metadata.last_offset, PAGE_DATA_START);
        assert_eq!(metadata.num_rows, 0);

        let _ = fs::remove_file("test_metadata_init.db");
        let _ = fs::remove_file("test_metadata_init.db.lock");
    }

    #[test]
    fn test_page_metadata_update() {
        let _ = fs::remove_file("test_metadata_update.db");
        let _ = fs::remove_file("test_metadata_update.db.lock");

        let mut pm = PageManager::new("test_metadata_update.db").unwrap();
        let page_id = pm.allocate_page().unwrap();

        // Update metadata
        let new_metadata = PageMetadata {
            is_full: true,
            last_offset: 100,
            num_rows: 5,
        };
        pm.update_page_metadata(page_id, &new_metadata).unwrap();

        // Read it back
        let read_metadata = pm.read_page_metadata(page_id).unwrap();

        assert_eq!(read_metadata.is_full, true);
        assert_eq!(read_metadata.last_offset, 100);
        assert_eq!(read_metadata.num_rows, 5);

        let _ = fs::remove_file("test_metadata_update.db");
        let _ = fs::remove_file("test_metadata_update.db.lock");
    }

    #[test]
    fn test_page_metadata_persistence() {
        let _ = fs::remove_file("test_metadata_persist.db");
        let _ = fs::remove_file("test_metadata_persist.db.lock");

        {
            let mut pm = PageManager::new("test_metadata_persist.db").unwrap();
            let page_id = pm.allocate_page().unwrap();

            // Update metadata
            let metadata = PageMetadata {
                is_full: false,
                last_offset: 250,
                num_rows: 10,
            };
            pm.update_page_metadata(page_id, &metadata).unwrap();
        } // pm dropped, file closed

        // Reopen database
        {
            let pm = PageManager::new("test_metadata_persist.db").unwrap();
            let metadata = pm.read_page_metadata(1).unwrap();

            // Metadata should persist
            assert_eq!(metadata.is_full, false);
            assert_eq!(metadata.last_offset, 250);
            assert_eq!(metadata.num_rows, 10);
        }

        let _ = fs::remove_file("test_metadata_persist.db");
        let _ = fs::remove_file("test_metadata_persist.db.lock");
    }

    #[test]
    fn test_multiple_pages_have_separate_metadata() {
        let _ = fs::remove_file("test_multi_meta.db");
        let _ = fs::remove_file("test_multi_meta.db.lock");

        let mut pm = PageManager::new("test_multi_meta.db").unwrap();

        // Allocate two pages
        let page1 = pm.allocate_page().unwrap();
        let page2 = pm.allocate_page().unwrap();

        // Update page1 metadata
        let meta1 = PageMetadata {
            is_full: true,
            last_offset: 100,
            num_rows: 3,
        };
        pm.update_page_metadata(page1, &meta1).unwrap();

        // Update page2 metadata
        let meta2 = PageMetadata {
            is_full: false,
            last_offset: 200,
            num_rows: 7,
        };
        pm.update_page_metadata(page2, &meta2).unwrap();

        // Read back and verify they're independent
        let read_meta1 = pm.read_page_metadata(page1).unwrap();
        let read_meta2 = pm.read_page_metadata(page2).unwrap();

        assert_eq!(read_meta1.num_rows, 3);
        assert_eq!(read_meta2.num_rows, 7);
        assert_eq!(read_meta1.last_offset, 100);
        assert_eq!(read_meta2.last_offset, 200);

        let _ = fs::remove_file("test_multi_meta.db");
        let _ = fs::remove_file("test_multi_meta.db.lock");
    }

    #[test]
    fn test_page_metadata_does_not_affect_data_area() {
        let _ = fs::remove_file("test_meta_data.db");
        let _ = fs::remove_file("test_meta_data.db.lock");

        let mut pm = PageManager::new("test_meta_data.db").unwrap();
        let page_id = pm.allocate_page().unwrap();

        // Write some data to the page (in data area)
        let mut page_data = pm.read_page(page_id).unwrap();
        let test_data = b"Hello, World!";
        page_data[PAGE_DATA_START..PAGE_DATA_START + test_data.len()].copy_from_slice(test_data);
        pm.write_page(page_id, &page_data).unwrap();

        // Update metadata
        let metadata = PageMetadata {
            is_full: false,
            last_offset: PAGE_DATA_START + test_data.len(),
            num_rows: 1,
        };
        pm.update_page_metadata(page_id, &metadata).unwrap();

        // Read page and verify data is intact
        let page_data = pm.read_page(page_id).unwrap();
        assert_eq!(
            &page_data[PAGE_DATA_START..PAGE_DATA_START + test_data.len()],
            test_data
        );

        // Verify metadata is correct
        let meta = pm.read_page_metadata(page_id).unwrap();
        assert_eq!(meta.num_rows, 1);
        assert_eq!(meta.last_offset, PAGE_DATA_START + test_data.len());

        let _ = fs::remove_file("test_meta_data.db");
        let _ = fs::remove_file("test_meta_data.db.lock");
    }
}
