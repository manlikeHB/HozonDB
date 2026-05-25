use crate::constants::PageId;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 12;
const MAGIC_NUMBER: u32 = 0x484F5A4E; // HOZN

const PAGE_METADATA_SIZE: usize = 10;
pub const SLOT_DIRECTORY_START: usize = PAGE_METADATA_SIZE;

const NULL_PAGE: u32 = 0xFFFFFFFF; // Sentinel value for Option::None

// Metadata offsets
const OFFSET_SLOT_COUNT: usize = 0;
const OFFSET_FREE_SPACE_START: usize = 2;
const OFFSET_FREE_SPACE_END: usize = 4;
const OFFSET_NEXT_PAGE: usize = 6;

#[derive(Debug)]
pub struct PageManager {
    file: Mutex<File>,
    lock_path: PathBuf,
    num_pages: u32,
    first_free_page: Option<PageId>,
}

#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub slot_count: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub next_page: Option<u32>,
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

            if magic_number != MAGIC_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid magic number",
                ));
            }

            // Read number of pages
            let mut num_pages_bytes = [0u8; 4];
            file.read_exact(&mut num_pages_bytes)?;
            let num_pages = u32::from_le_bytes(num_pages_bytes);

            // Read first free page id
            let mut f_f_page_bytes = [0u8; 4];
            file.read_exact(&mut f_f_page_bytes)?;
            let first_free_page = u32::from_le_bytes(f_f_page_bytes);

            Ok(PageManager {
                file: Mutex::new(file),
                num_pages: num_pages,
                lock_path,
                first_free_page: if first_free_page == NULL_PAGE {
                    None
                } else {
                    Some(first_free_page)
                },
            })
        } else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            let mut headers = [0u8; PAGE_SIZE];
            headers[0..4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes()); // magic number
            headers[4..8].copy_from_slice(&1u32.to_le_bytes()); // number of pages
            headers[8..12].copy_from_slice(&NULL_PAGE.to_le_bytes()); // free pages linked list first page = None
            file.write_all(&headers)?;

            Ok(PageManager {
                file: Mutex::new(file),
                num_pages: 1,
                lock_path,
                first_free_page: None,
            })
        }
    }

    /// Write header (magic, num_pages, first_free_page) to page 0 (header)
    fn write_header(&mut self) -> io::Result<()> {
        let mut headers = [0u8; HEADER_SIZE];
        headers[0..4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes()); // magic number
        headers[4..8].copy_from_slice(&self.num_pages().to_le_bytes()); // number of pages
        headers[8..12].copy_from_slice(&self.first_free_page.unwrap_or(NULL_PAGE).to_le_bytes()); // free pages linked list first page

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?; // go to start
        file.write_all(&headers)?;
        file.sync_all()?;

        Ok(())
    }

    /// Write next_free pointer to a free page
    fn write_free_page(&mut self, page_id: PageId, next_free: Option<PageId>) -> io::Result<()> {
        let mut page_buffer = [0u8; PAGE_SIZE];
        page_buffer[0..4].copy_from_slice(&next_free.unwrap_or(NULL_PAGE).to_le_bytes());
        self.write_page(page_id, &page_buffer)?;
        Ok(())
    }

    /// Read next_free pointer from a free page
    fn read_next_free(&self, page_id: PageId) -> io::Result<Option<PageId>> {
        let page_data = self.read_page(page_id)?;
        let next_page =
            u32::from_le_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);
        if next_page == NULL_PAGE {
            Ok(None)
        } else {
            Ok(Some(next_page))
        }
    }

    /// Add a page to the free list
    pub fn free_page(&mut self, page_id: PageId) -> io::Result<()> {
        // get current head of free pages list and make it the next page
        self.write_free_page(page_id, self.first_free_page)?;

        // update head to new free page
        self.first_free_page = Some(page_id);
        self.write_header()?; // update header
        Ok(())
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
        // check if there are free pages
        if let Some(free_page_id) = self.first_free_page {
            let next_free = self.read_next_free(free_page_id)?;
            // update next free page in header
            self.first_free_page = next_free;
            self.write_header()?;

            // Update page metadata to default since it's being re-allocated as a new page
            let page_meta = PageMetadata {
                slot_count: 0,
                free_space_start: SLOT_DIRECTORY_START as u16,
                free_space_end: PAGE_SIZE as u16,
                next_page: None,
            };

            self.update_page_metadata(free_page_id, &page_meta)?;

            return Ok(free_page_id);
        }

        // No free pages - extend database
        let page_id: PageId = self.num_pages;
        self.num_pages += 1;

        let new_size = (self.num_pages as u64) * (PAGE_SIZE as u64);
        let num_pages_bytes = self.num_pages.to_le_bytes();

        // Extend db file size and set new number of pages
        {
            let mut file = self.file.lock().unwrap();
            file.set_len(new_size)?;
            file.seek(SeekFrom::Start(4))?;
            file.write_all(&num_pages_bytes)?; // update number of pages in header
        };

        let mut page_data = [0u8; PAGE_SIZE];

        // page 0 = headers, page 1 = tables catalog, page 2 = index catalog
        if self.num_pages > 3 {
            // Create page buffer with metadata
            Self::init_page_metadata_buffer(&mut page_data);
        }

        // update header on disk!
        self.write_header()?;

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
        page_data[OFFSET_SLOT_COUNT..OFFSET_SLOT_COUNT + 2].copy_from_slice(&0u16.to_le_bytes());
        page_data[OFFSET_FREE_SPACE_START..OFFSET_FREE_SPACE_START + 2]
            .copy_from_slice(&(SLOT_DIRECTORY_START as u16).to_le_bytes());
        page_data[OFFSET_FREE_SPACE_END..OFFSET_FREE_SPACE_END + 2]
            .copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        page_data[OFFSET_NEXT_PAGE..OFFSET_NEXT_PAGE + 4].copy_from_slice(&NULL_PAGE.to_le_bytes());
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
        let slot_count = u16::from_le_bytes([
            page_data[OFFSET_SLOT_COUNT],
            page_data[OFFSET_SLOT_COUNT + 1],
        ]);

        let free_space_start = u16::from_le_bytes([
            page_data[OFFSET_FREE_SPACE_START],
            page_data[OFFSET_FREE_SPACE_START + 1],
        ]);

        let free_space_end = u16::from_le_bytes([
            page_data[OFFSET_FREE_SPACE_END],
            page_data[OFFSET_FREE_SPACE_END + 1],
        ]);

        let next_page = u32::from_le_bytes([
            page_data[OFFSET_NEXT_PAGE],
            page_data[OFFSET_NEXT_PAGE + 1],
            page_data[OFFSET_NEXT_PAGE + 2],
            page_data[OFFSET_NEXT_PAGE + 3],
        ]);

        PageMetadata {
            slot_count,
            free_space_start,
            free_space_end,
            next_page: if next_page == NULL_PAGE {
                None
            } else {
                Some(next_page)
            },
        }
    }

    pub fn update_metadata_in_buffer(page_data: &mut [u8; PAGE_SIZE], metadata: &PageMetadata) {
        page_data[OFFSET_SLOT_COUNT..OFFSET_SLOT_COUNT + 2]
            .copy_from_slice(&metadata.slot_count.to_le_bytes());
        page_data[OFFSET_FREE_SPACE_START..OFFSET_FREE_SPACE_START + 2]
            .copy_from_slice(&metadata.free_space_start.to_le_bytes());
        page_data[OFFSET_FREE_SPACE_END..OFFSET_FREE_SPACE_END + 2]
            .copy_from_slice(&metadata.free_space_end.to_le_bytes());
        let next_page = metadata.next_page.unwrap_or(NULL_PAGE);
        page_data[OFFSET_NEXT_PAGE..OFFSET_NEXT_PAGE + 4].copy_from_slice(&next_page.to_le_bytes());
    }

    pub fn first_free_page(&self) -> Option<PageId> {
        self.first_free_page
    }

    pub fn read_slot(page_data: &[u8; PAGE_SIZE], slot_index: u16) -> (u16, u16) {
        let mut offset = SLOT_DIRECTORY_START + slot_index as usize * 4;

        let row_offset = u16::from_le_bytes([page_data[offset], page_data[offset + 1]]);
        offset += 2; // consumed 2 bytes for row offset

        let row_length = u16::from_le_bytes([page_data[offset], page_data[offset + 1]]);

        (row_offset, row_length)
    }

    pub fn write_slot(
        page_data: &mut [u8; PAGE_SIZE],
        slot_index: u16,
        row_offset: u16,
        row_length: u16,
    ) {
        let mut offset = SLOT_DIRECTORY_START + slot_index as usize * 4;

        // write row offset
        page_data[offset..offset + 2].copy_from_slice(&row_offset.to_le_bytes());
        offset += 2;
        // write row length
        page_data[offset..offset + 2].copy_from_slice(&row_length.to_le_bytes());
    }

    pub fn mark_slot_dead(page_data: &mut [u8; PAGE_SIZE], slot_index: u16) {
        let mut offset = SLOT_DIRECTORY_START + slot_index as usize * 4;
        offset += 2; // skip row offset
        // set row length to zero
        page_data[offset..offset + 2].copy_from_slice(&0u16.to_le_bytes());
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

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

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

        let page_id_1 = pm.allocate_page().unwrap(); // page id 1 is for table catalog
        assert_eq!(page_id_1, 1);
        let page_id_2 = pm.allocate_page().unwrap(); // page id 2 is for index catalog
        assert_eq!(page_id_2, 2);
        // Allocate page 3 (should have initialized metadata)
        let page_id_3 = pm.allocate_page().unwrap();
        assert_eq!(page_id_3, 3);

        // Read metadata
        let metadata = pm.read_page_metadata(page_id_3).unwrap();

        // Check initial values
        assert_eq!(metadata.slot_count, 0);
        assert_eq!(metadata.free_space_start, SLOT_DIRECTORY_START as u16);
        assert_eq!(metadata.free_space_end, PAGE_SIZE as u16);
        assert_eq!(metadata.next_page, None);

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
            slot_count: 5,
            free_space_start: 100,
            free_space_end: 500,
            next_page: None,
        };
        pm.update_page_metadata(page_id, &new_metadata).unwrap();

        // Read it back
        let read_metadata = pm.read_page_metadata(page_id).unwrap();

        assert_eq!(read_metadata.slot_count, 5);
        assert_eq!(read_metadata.free_space_start, 100);
        assert_eq!(read_metadata.free_space_end, 500);

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
                slot_count: 5,
                free_space_start: 100,
                free_space_end: 500,
                next_page: None,
            };
            pm.update_page_metadata(page_id, &metadata).unwrap();
        } // pm dropped, file closed

        // Reopen database
        {
            let pm = PageManager::new("test_metadata_persist.db").unwrap();
            let metadata = pm.read_page_metadata(1).unwrap();

            // Metadata should persist
            assert_eq!(metadata.slot_count, 5);
            assert_eq!(metadata.free_space_start, 100);
            assert_eq!(metadata.free_space_end, 500);
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
            slot_count: 5,
            free_space_start: 100,
            free_space_end: 500,
            next_page: None,
        };
        pm.update_page_metadata(page1, &meta1).unwrap();

        // Update page2 metadata
        let meta2 = PageMetadata {
            slot_count: 20,
            free_space_start: 458,
            free_space_end: 2983,
            next_page: None,
        };
        pm.update_page_metadata(page2, &meta2).unwrap();

        // Read back and verify they're independent
        let read_meta1 = pm.read_page_metadata(page1).unwrap();
        let read_meta2 = pm.read_page_metadata(page2).unwrap();

        assert_eq!(read_meta1.slot_count, 5);
        assert_eq!(read_meta2.slot_count, 20);
        assert_eq!(read_meta1.free_space_start, 100);
        assert_eq!(read_meta2.free_space_start, 458);
        assert_eq!(read_meta1.free_space_end, 500);
        assert_eq!(read_meta2.free_space_end, 2983);

        let _ = fs::remove_file("test_multi_meta.db");
        let _ = fs::remove_file("test_multi_meta.db.lock");
    }

    // #[test]
    // fn test_page_metadata_does_not_affect_data_area() {
    //     let _ = fs::remove_file("test_meta_data.db");
    //     let _ = fs::remove_file("test_meta_data.db.lock");

    //     let mut pm = PageManager::new("test_meta_data.db").unwrap();
    //     let page_id = pm.allocate_page().unwrap();

    //     // Write some data to the page (in data area)
    //     let mut page_data = pm.read_page(page_id).unwrap();
    //     let test_data = b"Hello, World!";
    //     page_data[SLOT_DIRECTORY_START..SLOT_DIRECTORY_START + test_data.len()].copy_from_slice(test_data);
    //     pm.write_page(page_id, &page_data).unwrap();

    //     // Update metadata
    //     let metadata = PageMetadata {
    //         is_full: false,
    //         last_offset: SLOT_DIRECTORY_START + test_data.len(),
    //         num_rows: 1,
    //         next_page: None,
    //     };
    //     pm.update_page_metadata(page_id, &metadata).unwrap();

    //     // Read page and verify data is intact
    //     let page_data = pm.read_page(page_id).unwrap();
    //     assert_eq!(
    //         &page_data[SLOT_DIRECTORY_START..SLOT_DIRECTORY_START + test_data.len()],
    //         test_data
    //     );

    //     // Verify metadata is correct
    //     let meta = pm.read_page_metadata(page_id).unwrap();
    //     assert_eq!(meta.num_rows, 1);
    //     assert_eq!(meta.last_offset, SLOT_DIRECTORY_START + test_data.len());

    //     let _ = fs::remove_file("test_meta_data.db");
    //     let _ = fs::remove_file("test_meta_data.db.lock");
    // }

    #[test]
    fn test_header_with_free_list() {
        cleanup("test_header_free");

        // Create new database
        let pm = PageManager::new("test_header_free.hdb").unwrap();

        // Verify initial state
        assert_eq!(pm.num_pages(), 1);
        assert_eq!(pm.first_free_page, None);

        drop(pm);

        // Reopen and verify header persisted
        let pm = PageManager::new("test_header_free.hdb").unwrap();
        assert_eq!(pm.num_pages(), 1);
        assert_eq!(pm.first_free_page, None);

        cleanup("test_header_free");
    }

    #[test]
    fn test_free_page_adds_to_list() {
        cleanup("test_free_add");
        let mut pm = PageManager::new("test_free_add.hdb").unwrap();

        // Allocate a page
        let page1 = pm.allocate_page().unwrap();
        assert_eq!(page1, 1);
        assert_eq!(pm.first_free_page, None);

        // Free the page
        pm.free_page(page1).unwrap();
        assert_eq!(pm.first_free_page, Some(1));

        cleanup("test_free_add");
    }

    #[test]
    fn test_allocate_reuses_freed_page() {
        cleanup("test_reuse");
        let mut pm = PageManager::new("test_reuse.hdb").unwrap();

        // Allocate 3 pages
        let page1 = pm.allocate_page().unwrap();
        let page2 = pm.allocate_page().unwrap();
        let page3 = pm.allocate_page().unwrap();

        assert_eq!(page1, 1);
        assert_eq!(page2, 2);
        assert_eq!(page3, 3);
        assert_eq!(pm.num_pages(), 4); // 0, 1, 2, 3

        // Free page 2
        pm.free_page(page2).unwrap();
        assert_eq!(pm.first_free_page, Some(2));

        // Next allocation should reuse page 2
        let page4 = pm.allocate_page().unwrap();
        assert_eq!(page4, 2);
        assert_eq!(pm.first_free_page, None);
        assert_eq!(pm.num_pages(), 4); // Didn't grow

        cleanup("test_reuse");
    }

    #[test]
    fn test_free_list_lifo_order() {
        cleanup("test_lifo");
        let mut pm = PageManager::new("test_lifo.hdb").unwrap();

        // Allocate 3 pages
        let page1 = pm.allocate_page().unwrap();
        let page2 = pm.allocate_page().unwrap();
        let page3 = pm.allocate_page().unwrap();

        // Free in order: 1, 2, 3
        pm.free_page(page1).unwrap();
        pm.free_page(page2).unwrap();
        pm.free_page(page3).unwrap();

        // Free list: 3 → 2 → 1 → NULL (LIFO)
        assert_eq!(pm.first_free_page, Some(3));

        // Allocate should return in LIFO order: 3, 2, 1
        let realloc1 = pm.allocate_page().unwrap();
        assert_eq!(realloc1, 3);

        let realloc2 = pm.allocate_page().unwrap();
        assert_eq!(realloc2, 2);

        let realloc3 = pm.allocate_page().unwrap();
        assert_eq!(realloc3, 1);

        // List should be empty now
        assert_eq!(pm.first_free_page, None);

        cleanup("test_lifo");
    }

    #[test]
    fn test_free_list_persistence() {
        cleanup("test_free_persist");

        // Session 1: Create free list
        {
            let mut pm = PageManager::new("test_free_persist.hdb").unwrap();

            let _ = pm.allocate_page().unwrap();
            let page2 = pm.allocate_page().unwrap();
            let page3 = pm.allocate_page().unwrap();

            pm.free_page(page2).unwrap();
            pm.free_page(page3).unwrap();

            assert_eq!(pm.first_free_page, Some(3));
        } // Close database

        // Session 2: Verify free list persisted
        {
            let mut pm = PageManager::new("test_free_persist.hdb").unwrap();

            // Free list should still be: 3 → 2 → NULL
            assert_eq!(pm.first_free_page, Some(3));

            // Allocate should reuse page 3
            let page = pm.allocate_page().unwrap();
            assert_eq!(page, 3);

            // Now first_free should be 2
            assert_eq!(pm.first_free_page, Some(2));
        }

        cleanup("test_free_persist");
    }

    #[test]
    fn test_allocate_when_free_list_empty() {
        cleanup("test_empty_free");
        let mut pm = PageManager::new("test_empty_free.hdb").unwrap();

        // Free list is empty initially
        assert_eq!(pm.first_free_page, None);

        // Allocate should extend database
        let page1 = pm.allocate_page().unwrap();
        assert_eq!(page1, 1);
        assert_eq!(pm.num_pages(), 2);

        cleanup("test_empty_free");
    }

    #[test]
    fn test_multiple_free_and_allocate_cycles() {
        cleanup("test_cycles");
        let mut pm = PageManager::new("test_cycles.hdb").unwrap();

        // Allocate 5 pages
        for _ in 0..5 {
            pm.allocate_page().unwrap();
        }
        assert_eq!(pm.num_pages(), 6); // 0, 1, 2, 3, 4, 5

        // Free pages 2, 3, 4
        pm.free_page(2).unwrap();
        pm.free_page(3).unwrap();
        pm.free_page(4).unwrap();

        // Allocate 2 pages (should reuse 4, 3)
        let p1 = pm.allocate_page().unwrap();
        let p2 = pm.allocate_page().unwrap();
        assert_eq!(p1, 4);
        assert_eq!(p2, 3);

        // Free list: 2 → NULL
        assert_eq!(pm.first_free_page, Some(2));

        // Free page 5
        pm.free_page(5).unwrap();

        // Free list: 5 → 2 → NULL
        assert_eq!(pm.first_free_page, Some(5));

        // Allocate 3 pages (should reuse 5, 2, then extend to 6)
        let p3 = pm.allocate_page().unwrap();
        let p4 = pm.allocate_page().unwrap();
        let p5 = pm.allocate_page().unwrap();

        assert_eq!(p3, 5);
        assert_eq!(p4, 2);
        assert_eq!(p5, 6); // Extended

        assert_eq!(pm.first_free_page, None);
        assert_eq!(pm.num_pages(), 7);

        cleanup("test_cycles");
    }

    #[test]
    fn test_free_same_page_twice() {
        cleanup("test_double_free");
        let mut pm = PageManager::new("test_double_free.hdb").unwrap();

        let page1 = pm.allocate_page().unwrap();

        // Free page 1
        pm.free_page(page1).unwrap();
        assert_eq!(pm.first_free_page, Some(1));

        // Free page 1 again (should still work, creates duplicate in list)
        // Note: In production, you'd prevent this, but for now it's allowed
        pm.free_page(page1).unwrap();
        assert_eq!(pm.first_free_page, Some(1));

        // This creates a cycle: 1 → 1 → 1 → ...
        // Allocate will return page 1 twice (bug, but that's expected for now)
        let p1 = pm.allocate_page().unwrap();
        let p2 = pm.allocate_page().unwrap();
        assert_eq!(p1, 1);
        assert_eq!(p2, 1); // Same page!

        cleanup("test_double_free");
    }

    #[test]
    fn test_write_header_updates_num_pages() {
        cleanup("test_header_update");
        let mut pm = PageManager::new("test_header_update.hdb").unwrap();

        // Allocate page (increases num_pages)
        pm.allocate_page().unwrap();
        assert_eq!(pm.num_pages(), 2);

        // Manually verify header on disk
        drop(pm);
        let pm = PageManager::new("test_header_update.hdb").unwrap();
        assert_eq!(pm.num_pages(), 2); // Should persist

        cleanup("test_header_update");
    }

    #[test]
    fn test_write_header_updates_first_free() {
        cleanup("test_header_first_free");
        let mut pm = PageManager::new("test_header_first_free.hdb").unwrap();

        let page1 = pm.allocate_page().unwrap();
        pm.free_page(page1).unwrap();

        assert_eq!(pm.first_free_page, Some(1));

        // Verify persistence
        drop(pm);
        let pm = PageManager::new("test_header_first_free.hdb").unwrap();
        assert_eq!(pm.first_free_page, Some(1));

        cleanup("test_header_first_free");
    }

    #[test]
    fn test_free_list_chain_integrity() {
        cleanup("test_chain");
        let mut pm = PageManager::new("test_chain.hdb").unwrap();

        // Allocate 5 pages
        for _ in 0..5 {
            pm.allocate_page().unwrap();
        }

        // Free pages to create chain: 5 → 3 → 1 → NULL
        pm.free_page(1).unwrap();
        pm.free_page(3).unwrap();
        pm.free_page(5).unwrap();

        // Manually verify chain by reading pages
        assert_eq!(pm.first_free_page, Some(5));

        let next1 = pm.read_next_free(5).unwrap();
        assert_eq!(next1, Some(3));

        let next2 = pm.read_next_free(3).unwrap();
        assert_eq!(next2, Some(1));

        let next3 = pm.read_next_free(1).unwrap();
        assert_eq!(next3, None);

        cleanup("test_chain");
    }

    #[test]
    fn test_allocate_all_freed_pages() {
        cleanup("test_allocate_all");
        let mut pm = PageManager::new("test_allocate_all.hdb").unwrap();

        // Allocate 10 pages
        for _ in 0..10 {
            pm.allocate_page().unwrap();
        }

        // Free all (except page 0)
        for i in 1..=10 {
            pm.free_page(i).unwrap();
        }

        // Allocate all back
        for _ in 0..10 {
            pm.allocate_page().unwrap();
        }

        // Free list should be empty
        assert_eq!(pm.first_free_page, None);

        // Next allocation should extend
        let new_page = pm.allocate_page().unwrap();
        assert_eq!(new_page, 11);

        cleanup("test_allocate_all");
    }

    #[test]
    fn test_write_and_read_slot() {
        cleanup("test_write_and_read_slot");
        let mut pm = PageManager::new("test_write_and_read_slot.hdb").unwrap();

        // allocate new page
        let page_id = pm.allocate_page().unwrap();
        let row_offset = 300u16;
        let row_length = 35u16;
        let slot_index = 0;

        let mut page_data = pm.read_page(page_id).unwrap();

        // read from page without any slot
        let (row_offset_res, row_length_res) = PageManager::read_slot(&page_data, slot_index);
        assert_eq!(row_offset_res, 0);
        assert_eq!(row_length_res, 0);

        // write slot
        PageManager::write_slot(&mut page_data, slot_index, row_offset, row_length);

        // verify slot got written
        let (row_offset_res, row_length_res) = PageManager::read_slot(&page_data, slot_index);
        assert_eq!(row_offset_res, row_offset);
        assert_eq!(row_length_res, row_length);

        cleanup("test_write_and_read_slot");
    }

    #[test]
    fn test_multiple_slots_independent() {
        cleanup("test_multiple_slots_independent");
        let mut pm = PageManager::new("test_multiple_slots_independent.hdb").unwrap();

        let page_id = pm.allocate_page().unwrap();
        let mut page_data = pm.read_page(page_id).unwrap();

        PageManager::write_slot(&mut page_data, 0, 4000u16, 50u16);
        PageManager::write_slot(&mut page_data, 1, 3950u16, 50u16);

        let (offset_0, length_0) = PageManager::read_slot(&page_data, 0);
        let (offset_1, length_1) = PageManager::read_slot(&page_data, 1);

        assert_eq!(offset_0, 4000);
        assert_eq!(length_0, 50);
        assert_eq!(offset_1, 3950);
        assert_eq!(length_1, 50);

        cleanup("test_multiple_slots_independent");
    }

    #[test]
    fn test_mark_slot_dead() {
        cleanup("test_mark_slot_dead");
        let mut pm = PageManager::new("test_mark_slot_dead.hdb").unwrap();

        let page_id = pm.allocate_page().unwrap();
        let mut page_data = pm.read_page(page_id).unwrap();

        PageManager::write_slot(&mut page_data, 0, 4000u16, 50u16);
        PageManager::mark_slot_dead(&mut page_data, 0);

        let (offset, length) = PageManager::read_slot(&page_data, 0);
        assert_eq!(length, 0);
        assert_eq!(offset, 4000); // offset unchanged

        cleanup("test_mark_slot_dead");
    }
}
