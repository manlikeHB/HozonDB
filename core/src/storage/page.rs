use crate::constants::{OFFSET_RAW_PAGE_START, PageId};
use std::fs::{File, OpenOptions};
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 12;
const MAGIC_NUMBER: u32 = 0x484F5A4E; // HOZN

const PAGE_METADATA_SIZE: usize = 18;
pub const SLOT_DIRECTORY_START: usize = PAGE_METADATA_SIZE;

pub const NULL_PAGE: u32 = 0xFFFFFFFF; // Sentinel value for Option::None

// Metadata offsets slotted page
const OFFSET_SLOT_COUNT: usize = 0;
const OFFSET_FREE_SPACE_START: usize = 2;
const OFFSET_FREE_SPACE_END: usize = 4;
const OFFSET_NEXT_PAGE: usize = 6;
const OFFSET_LSN: usize = 10;

#[derive(Debug)]
pub struct PageManager {
    file: Mutex<File>,
    lock_path: PathBuf,
    num_pages: u32,
    first_free_page: Option<PageId>,
}

#[derive(Debug)]
pub enum PageType {
    Slotted,
    Raw,
    Free,
}

#[derive(Debug, Clone)]
pub enum PageMetadata {
    Slotted {
        slot_count: u16,
        free_space_start: u16,
        free_space_end: u16,
        next_page: Option<u32>,
        lsn: u64,
    },
    Raw {
        lsn: u64,
    },
    Free {
        next_page: Option<u32>,
        lsn: u64,
    },
}

impl PageMetadata {
    pub fn set_lsn(&mut self, new_lsn: u64) {
        match self {
            Self::Raw { lsn } | Self::Slotted { lsn, .. } | Self::Free { lsn, .. } => {
                *lsn = new_lsn
            }
        }
    }

    pub fn update_slot_count(&mut self) {
        match self {
            Self::Slotted { slot_count, .. } => *slot_count += 1,
            _ => {}
        }
    }

    pub fn update_free_space_start(&mut self) {
        match self {
            Self::Slotted {
                free_space_start, ..
            } => *free_space_start += 4,
            _ => {}
        }
    }

    pub fn update_free_space_end(&mut self, row_len: usize) {
        match self {
            Self::Slotted { free_space_end, .. } => *free_space_end -= row_len as u16,
            _ => {}
        }
    }

    pub fn set_next_page(&mut self, page_id: PageId) {
        match self {
            Self::Slotted { next_page, .. } | Self::Free { next_page, .. } => {
                *next_page = Some(page_id)
            }
            _ => {}
        }
    }

    pub fn lsn(&self) -> u64 {
        match self {
            Self::Raw { lsn } => *lsn,
            Self::Slotted { lsn, .. } => *lsn,
            Self::Free { lsn, .. } => *lsn,
        }
    }

    pub fn slot_count(&self) -> io::Result<u16> {
        match self {
            Self::Slotted { slot_count, .. } => Ok(*slot_count),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Only Slotted page metadata contain slot count",
            )),
        }
    }

    pub fn free_space_start(&self) -> io::Result<u16> {
        match self {
            Self::Slotted {
                free_space_start, ..
            } => Ok(*free_space_start),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Only Slotted page metadata contain free space start",
            )),
        }
    }

    pub fn free_space_end(&self) -> io::Result<u16> {
        match self {
            Self::Slotted { free_space_end, .. } => Ok(*free_space_end),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Only Slotted page metadata contain free space end",
            )),
        }
    }

    pub fn next_page(&self) -> io::Result<Option<u32>> {
        match self {
            Self::Slotted { next_page, .. } => Ok(*next_page),
            Self::Free { next_page, .. } => Ok(*next_page),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Raw page metadata does not contain next page",
            )),
        }
    }
}

impl PageManager {
    pub fn new(db_name: &str) -> io::Result<Self> {
        let path = format!("{db_name}.hdb");
        let lock_path = PathBuf::from(format!("{}.lock", path));

        // try to acquire lock
        Self::acquire_lock(Path::new(&lock_path))?;

        if Path::new(&path).exists() {
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

    /// Add a page to the free list
    pub fn set_first_free_page(&mut self, next_free: Option<PageId>) -> io::Result<()> {
        // update head to new free page
        self.first_free_page = next_free;
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
    pub fn allocate_page(&mut self, page_type: PageType) -> io::Result<PageId> {
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
        if page_id > 0 && page_id < 3 {
            // Create raw page buffer with metadata
            Self::init_page_metadata_buffer(&mut page_data, PageType::Raw);
        } else if page_id >= 3 {
            // Create page buffer with metadata
            Self::init_page_metadata_buffer(&mut page_data, page_type);
        }

        // update header on disk!
        self.write_header()?; // TODO: check if this necessary

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
            //TODO: sync_all is being removed since the WAL holds all logs and
            // flushing to disk will only be happening at checkpoints
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

    pub fn init_page_metadata_buffer(page_data: &mut [u8; PAGE_SIZE], page_type: PageType) {
        match page_type {
            PageType::Raw => {
                page_data[..OFFSET_RAW_PAGE_START].copy_from_slice(&0u64.to_le_bytes());
            }
            PageType::Slotted => {
                page_data[OFFSET_SLOT_COUNT..OFFSET_SLOT_COUNT + 2]
                    .copy_from_slice(&0u16.to_le_bytes());
                page_data[OFFSET_FREE_SPACE_START..OFFSET_FREE_SPACE_START + 2]
                    .copy_from_slice(&(SLOT_DIRECTORY_START as u16).to_le_bytes());
                page_data[OFFSET_FREE_SPACE_END..OFFSET_FREE_SPACE_END + 2]
                    .copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
                page_data[OFFSET_NEXT_PAGE..OFFSET_NEXT_PAGE + 4]
                    .copy_from_slice(&NULL_PAGE.to_le_bytes());
                page_data[OFFSET_LSN..OFFSET_LSN + 8].copy_from_slice(&0u64.to_le_bytes());
            }
            PageType::Free => {
                page_data[0..4].copy_from_slice(&NULL_PAGE.to_le_bytes());
                page_data[4..12].copy_from_slice(&0u64.to_le_bytes());
            }
        }
    }

    /// Read metadata from a page
    pub fn read_page_metadata(
        &self,
        page_id: PageId,
        page_type: PageType,
    ) -> io::Result<PageMetadata> {
        let page_data = self.read_page(page_id)?;
        Ok(Self::read_metadata_from_buffer(&page_data, page_type))
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

    pub fn read_metadata_from_buffer(
        page_data: &[u8; PAGE_SIZE],
        page_type: PageType,
    ) -> PageMetadata {
        match page_type {
            PageType::Raw => {
                let lsn = u64::from_le_bytes([
                    page_data[0],
                    page_data[1],
                    page_data[2],
                    page_data[3],
                    page_data[4],
                    page_data[5],
                    page_data[6],
                    page_data[7],
                ]);

                PageMetadata::Raw { lsn }
            }
            PageType::Slotted => {
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

                let lsn = u64::from_le_bytes([
                    page_data[OFFSET_LSN],
                    page_data[OFFSET_LSN + 1],
                    page_data[OFFSET_LSN + 2],
                    page_data[OFFSET_LSN + 3],
                    page_data[OFFSET_LSN + 4],
                    page_data[OFFSET_LSN + 5],
                    page_data[OFFSET_LSN + 6],
                    page_data[OFFSET_LSN + 7],
                ]);

                PageMetadata::Slotted {
                    slot_count,
                    free_space_start,
                    free_space_end,
                    next_page: if next_page == NULL_PAGE {
                        None
                    } else {
                        Some(next_page)
                    },
                    lsn,
                }
            }
            PageType::Free => {
                let next_page =
                    u32::from_le_bytes([page_data[0], page_data[1], page_data[2], page_data[3]]);

                let lsn = u64::from_le_bytes([
                    page_data[4],
                    page_data[5],
                    page_data[6],
                    page_data[7],
                    page_data[8],
                    page_data[9],
                    page_data[10],
                    page_data[11],
                ]);

                PageMetadata::Free {
                    next_page: if next_page == NULL_PAGE {
                        None
                    } else {
                        Some(next_page)
                    },
                    lsn,
                }
            }
        }
    }

    pub fn update_metadata_in_buffer(page_data: &mut [u8; PAGE_SIZE], metadata: &PageMetadata) {
        match metadata {
            PageMetadata::Raw { lsn } => {
                page_data[0..8].copy_from_slice(&lsn.to_le_bytes());
            }
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                next_page,
                lsn,
            } => {
                page_data[OFFSET_SLOT_COUNT..OFFSET_SLOT_COUNT + 2]
                    .copy_from_slice(&slot_count.to_le_bytes());
                page_data[OFFSET_FREE_SPACE_START..OFFSET_FREE_SPACE_START + 2]
                    .copy_from_slice(&free_space_start.to_le_bytes());
                page_data[OFFSET_FREE_SPACE_END..OFFSET_FREE_SPACE_END + 2]
                    .copy_from_slice(&free_space_end.to_le_bytes());
                let next_page = next_page.unwrap_or(NULL_PAGE);
                page_data[OFFSET_NEXT_PAGE..OFFSET_NEXT_PAGE + 4]
                    .copy_from_slice(&next_page.to_le_bytes());
                page_data[OFFSET_LSN..OFFSET_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
            }
            PageMetadata::Free { next_page, lsn } => {
                let next_page = next_page.unwrap_or(NULL_PAGE);
                page_data[0..4].copy_from_slice(&next_page.to_le_bytes());
                page_data[4..12].copy_from_slice(&lsn.to_le_bytes());
            }
        }
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
    use crate::test_helpers::*;

    #[test]
    fn test_page_manager_new() {
        cleanup("test");

        let pm = PageManager::new("test");
        assert!(pm.is_ok());
        assert_eq!(pm.unwrap().num_pages(), 1);

        let pm2 = PageManager::new("test");
        assert!(pm2.is_ok());
        assert_eq!(pm2.unwrap().num_pages(), 1);

        cleanup("test");
    }

    #[test]
    fn test_allocate_page() {
        cleanup("test_alloc");

        let mut pm = PageManager::new("test_alloc").unwrap();
        assert_eq!(pm.num_pages(), 1);

        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page_id, 1);
        assert_eq!(pm.num_pages(), 2);

        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pm.num_pages(), 3);

        drop(pm);
        let pm = PageManager::new("test_alloc").unwrap();
        assert_eq!(pm.num_pages(), 3);

        cleanup("test_alloc");
    }

    #[test]
    fn test_concurrent_access_prevention() {
        cleanup("test_lock");

        // First connection acquires lock
        let _pm1 = PageManager::new("test_lock").unwrap();

        // Second connection should fail
        let pm2 = PageManager::new("test_lock");
        assert!(pm2.is_err());
        assert_eq!(pm2.unwrap_err().kind(), io::ErrorKind::WouldBlock);

        // After dropping pm1, lock should be released
        drop(_pm1);

        // Now we should be able to open again
        let pm3 = PageManager::new("test_lock");
        assert!(pm3.is_ok());

        cleanup("test_lock");
    }

    #[test]
    fn test_write_and_read_page() {
        cleanup("test_rw");

        let mut pm = PageManager::new("test_rw").unwrap();

        // Allocate a page
        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
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

        cleanup("test_rw");
    }

    #[test]
    fn test_write_full_page() {
        cleanup("test_full");

        let mut pm = PageManager::new("test_full").unwrap();
        let page_id = pm.allocate_page(PageType::Slotted).unwrap();

        // Write exactly PAGE_SIZE bytes
        let data = [42u8; PAGE_SIZE];
        pm.write_page(page_id, &data).unwrap();

        // Read it back
        let read_data = pm.read_page(page_id).unwrap();
        assert_eq!(read_data, data);

        cleanup("test_full");
    }

    #[test]
    fn test_write_invalid_page() {
        cleanup("test_invalid");

        let mut pm = PageManager::new("test_invalid").unwrap();

        // Try to write to non-existent page
        let result = pm.write_page(999, b"data");
        assert!(result.is_err());

        cleanup("test_invalid");
    }

    #[test]
    fn test_write_oversized_data() {
        cleanup("test_oversize");

        let mut pm = PageManager::new("test_oversize").unwrap();
        let page_id = pm.allocate_page(PageType::Slotted).unwrap();

        // Try to write more than PAGE_SIZE
        let data = vec![1u8; PAGE_SIZE + 1];
        let result = pm.write_page(page_id, &data);
        assert!(result.is_err());

        cleanup("test_oversize");
    }

    #[test]
    fn test_page_metadata_initialization() {
        cleanup("test_metadata_init");

        let mut pm = PageManager::new("test_metadata_init").unwrap();

        let page_id_1 = pm.allocate_page(PageType::Slotted).unwrap(); // page id 1 is for table catalog
        assert_eq!(page_id_1, 1);
        let page_id_2 = pm.allocate_page(PageType::Slotted).unwrap(); // page id 2 is for index catalog
        assert_eq!(page_id_2, 2);
        // Allocate page 3 (should have initialized metadata)
        let page_id_3 = pm.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page_id_3, 3);

        // Read metadata
        let metadata = pm.read_page_metadata(page_id_3, PageType::Slotted).unwrap();

        match metadata {
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                next_page,
                lsn,
            } => {
                // Check initial values
                assert_eq!(slot_count, 0);
                assert_eq!(free_space_start, SLOT_DIRECTORY_START as u16);
                assert_eq!(free_space_end, PAGE_SIZE as u16);
                assert_eq!(next_page, None);
                assert_eq!(lsn, 0);
            }
            _ => panic!("Expected a slotted page metadata"),
        }

        cleanup("test_metadata_init");
    }

    #[test]
    fn test_page_metadata_update() {
        cleanup("test_metadata_update");

        let mut pm = PageManager::new("test_metadata_update").unwrap();
        let page_id = pm.allocate_page(PageType::Slotted).unwrap();

        // Update metadata
        let new_metadata = PageMetadata::Slotted {
            slot_count: 5,
            free_space_start: 100,
            free_space_end: 500,
            next_page: None,
            lsn: 45,
        };
        pm.update_page_metadata(page_id, &new_metadata).unwrap();

        // Read it back
        let read_metadata = pm.read_page_metadata(page_id, PageType::Slotted).unwrap();

        match read_metadata {
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                lsn,
                ..
            } => {
                // Check initial values
                assert_eq!(slot_count, 5);
                assert_eq!(free_space_start, 100);
                assert_eq!(free_space_end, 500);
                assert_eq!(lsn, 45);
            }
            _ => panic!("Expected a slotted page metadata"),
        }

        cleanup("test_metadata_update");
    }

    #[test]
    fn test_page_metadata_persistence() {
        cleanup("test_metadata_persist");

        {
            let mut pm = PageManager::new("test_metadata_persist").unwrap();
            let page_id = pm.allocate_page(PageType::Slotted).unwrap();

            // Update metadata
            let metadata = PageMetadata::Slotted {
                slot_count: 5,
                free_space_start: 100,
                free_space_end: 500,
                next_page: None,
                lsn: 78,
            };
            pm.update_page_metadata(page_id, &metadata).unwrap();
        } // pm dropped, file closed

        // Reopen database
        {
            let pm = PageManager::new("test_metadata_persist").unwrap();
            let metadata = pm.read_page_metadata(1, PageType::Slotted).unwrap();

            // Metadata should persist
            match metadata {
                PageMetadata::Slotted {
                    slot_count,
                    free_space_start,
                    free_space_end,
                    lsn,
                    ..
                } => {
                    // Check initial values
                    assert_eq!(slot_count, 5);
                    assert_eq!(free_space_start, 100);
                    assert_eq!(free_space_end, 500);
                    assert_eq!(lsn, 78);
                }
                _ => panic!("Expected a slotted page metadata"),
            }
        }

        cleanup("test_metadata_persist");
    }

    #[test]
    fn test_multiple_pages_have_separate_metadata() {
        cleanup("test_multi_meta");

        let mut pm = PageManager::new("test_multi_meta").unwrap();

        // Allocate two pages
        let page1 = pm.allocate_page(PageType::Slotted).unwrap();
        let page2 = pm.allocate_page(PageType::Slotted).unwrap();
        let page3 = pm.allocate_page(PageType::Raw).unwrap();

        // Update page1 metadata
        let meta1 = PageMetadata::Slotted {
            slot_count: 5,
            free_space_start: 100,
            free_space_end: 500,
            next_page: None,
            lsn: 98,
        };
        pm.update_page_metadata(page1, &meta1).unwrap();

        // Update page2 metadata
        let meta2 = PageMetadata::Slotted {
            slot_count: 20,
            free_space_start: 458,
            free_space_end: 2983,
            next_page: None,
            lsn: 65,
        };
        pm.update_page_metadata(page2, &meta2).unwrap();

        // Update page3 metadata
        let meta3 = PageMetadata::Raw { lsn: 3984 };
        pm.update_page_metadata(page3, &meta3).unwrap();

        // Read back and verify they're independent
        let read_meta1 = pm.read_page_metadata(page1, PageType::Slotted).unwrap();
        let read_meta2 = pm.read_page_metadata(page2, PageType::Slotted).unwrap();
        let read_meta3 = pm.read_page_metadata(page3, PageType::Raw).unwrap();

        match read_meta1 {
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                lsn,
                ..
            } => {
                // Check initial values
                assert_eq!(slot_count, 5);
                assert_eq!(free_space_start, 100);
                assert_eq!(free_space_end, 500);
                assert_eq!(lsn, 98);
            }
            _ => panic!("Expected a slotted page metadata"),
        }

        match read_meta2 {
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                lsn,
                ..
            } => {
                // Check initial values
                assert_eq!(slot_count, 20);
                assert_eq!(free_space_start, 458);
                assert_eq!(free_space_end, 2983);
                assert_eq!(lsn, 65);
            }
            _ => panic!("Expected a slotted page metadata"),
        }

        match read_meta3 {
            PageMetadata::Raw { lsn } => assert_eq!(lsn, 3984),
            _ => panic!("Expected a Raw page metadata"),
        }

        cleanup("test_multi_meta");
    }

    #[test]
    fn test_header_with_free_list() {
        cleanup("test_header_free");

        // Create new database
        let pm = PageManager::new("test_header_free").unwrap();

        // Verify initial state
        assert_eq!(pm.num_pages(), 1);
        assert_eq!(pm.first_free_page, None);

        drop(pm);

        // Reopen and verify header persisted
        let pm = PageManager::new("test_header_free").unwrap();
        assert_eq!(pm.num_pages(), 1);
        assert_eq!(pm.first_free_page, None);

        cleanup("test_header_free");
    }

    #[test]
    fn test_allocate_when_free_list_empty() {
        cleanup("test_empty_free");
        let mut pm = PageManager::new("test_empty_free").unwrap();

        // Free list is empty initially
        assert_eq!(pm.first_free_page, None);

        // Allocate should extend database
        let page1 = pm.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page1, 1);
        assert_eq!(pm.num_pages(), 2);

        cleanup("test_empty_free");
    }

    #[test]
    fn test_write_header_updates_num_pages() {
        cleanup("test_header_update");
        let mut pm = PageManager::new("test_header_update").unwrap();

        // Allocate page (increases num_pages)
        pm.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(pm.num_pages(), 2);

        // Manually verify header on disk
        drop(pm);
        let pm = PageManager::new("test_header_update").unwrap();
        assert_eq!(pm.num_pages(), 2); // Should persist

        cleanup("test_header_update");
    }

    #[test]
    fn test_write_and_read_slot() {
        cleanup("test_write_and_read_slot");
        let mut pm = PageManager::new("test_write_and_read_slot").unwrap();

        // allocate new page
        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
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
        let mut pm = PageManager::new("test_multiple_slots_independent").unwrap();

        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
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
        let mut pm = PageManager::new("test_mark_slot_dead").unwrap();

        let page_id = pm.allocate_page(PageType::Slotted).unwrap();
        let mut page_data = pm.read_page(page_id).unwrap();

        PageManager::write_slot(&mut page_data, 0, 4000u16, 50u16);
        PageManager::mark_slot_dead(&mut page_data, 0);

        let (offset, length) = PageManager::read_slot(&page_data, 0);
        assert_eq!(length, 0);
        assert_eq!(offset, 4000); // offset unchanged

        cleanup("test_mark_slot_dead");
    }

    // --- PageMetadata methods ---

    #[test]
    fn test_page_metadata_set_lsn() {
        let mut meta = PageMetadata::Slotted {
            slot_count: 0,
            free_space_start: 18,
            free_space_end: 4096,
            next_page: None,
            lsn: 0,
        };
        meta.set_lsn(42);
        assert_eq!(meta.lsn(), 42);

        let mut meta = PageMetadata::Raw { lsn: 0 };
        meta.set_lsn(99);
        assert_eq!(meta.lsn(), 99);

        let mut meta = PageMetadata::Free {
            next_page: None,
            lsn: 0,
        };
        meta.set_lsn(7);
        assert_eq!(meta.lsn(), 7);
    }

    #[test]
    fn test_page_metadata_update_slot_count() {
        let mut meta = PageMetadata::Slotted {
            slot_count: 2,
            free_space_start: 18,
            free_space_end: 4096,
            next_page: None,
            lsn: 0,
        };
        meta.update_slot_count();
        assert_eq!(meta.slot_count().unwrap(), 3);
    }

    #[test]
    fn test_page_metadata_update_free_space() {
        let mut meta = PageMetadata::Slotted {
            slot_count: 0,
            free_space_start: 18,
            free_space_end: 4096,
            next_page: None,
            lsn: 0,
        };
        meta.update_free_space_start();
        assert_eq!(meta.free_space_start().unwrap(), 22); // +4 per slot entry

        meta.update_free_space_end(50);
        assert_eq!(meta.free_space_end().unwrap(), 4046);
    }

    #[test]
    fn test_page_metadata_set_next_page() {
        let mut meta = PageMetadata::Slotted {
            slot_count: 0,
            free_space_start: 18,
            free_space_end: 4096,
            next_page: None,
            lsn: 0,
        };
        meta.set_next_page(5);
        assert_eq!(meta.next_page().unwrap(), Some(5));

        let mut meta = PageMetadata::Free {
            next_page: None,
            lsn: 0,
        };
        meta.set_next_page(3);
        assert_eq!(meta.next_page().unwrap(), Some(3));
    }

    #[test]
    fn test_page_metadata_accessor_errors() {
        let raw = PageMetadata::Raw { lsn: 0 };
        assert!(raw.slot_count().is_err());
        assert!(raw.free_space_start().is_err());
        assert!(raw.free_space_end().is_err());
        assert!(raw.next_page().is_err());

        let free = PageMetadata::Free {
            next_page: None,
            lsn: 0,
        };
        assert!(free.slot_count().is_err());
        assert!(free.free_space_start().is_err());
        assert!(free.free_space_end().is_err());
    }

    // --- init_page_metadata_buffer ---

    #[test]
    fn test_init_page_metadata_buffer_slotted() {
        let mut page = [0u8; PAGE_SIZE];
        PageManager::init_page_metadata_buffer(&mut page, PageType::Slotted);

        let meta = PageManager::read_metadata_from_buffer(&page, PageType::Slotted);
        match meta {
            PageMetadata::Slotted {
                slot_count,
                free_space_start,
                free_space_end,
                next_page,
                lsn,
            } => {
                assert_eq!(slot_count, 0);
                assert_eq!(free_space_start, SLOT_DIRECTORY_START as u16);
                assert_eq!(free_space_end, PAGE_SIZE as u16);
                assert_eq!(next_page, None); // NULL_PAGE sentinel correctly read as None
                assert_eq!(lsn, 0);
            }
            _ => panic!("expected slotted metadata"),
        }
    }

    #[test]
    fn test_init_page_metadata_buffer_raw() {
        let mut page = [0u8; PAGE_SIZE];
        PageManager::init_page_metadata_buffer(&mut page, PageType::Raw);

        let meta = PageManager::read_metadata_from_buffer(&page, PageType::Raw);
        match meta {
            PageMetadata::Raw { lsn } => assert_eq!(lsn, 0),
            _ => panic!("expected raw metadata"),
        }
    }

    #[test]
    fn test_init_page_metadata_buffer_free() {
        let mut page = [0u8; PAGE_SIZE];
        PageManager::init_page_metadata_buffer(&mut page, PageType::Free);

        let meta = PageManager::read_metadata_from_buffer(&page, PageType::Free);
        match meta {
            PageMetadata::Free { next_page, lsn } => {
                assert_eq!(next_page, None); // NULL_PAGE sentinel correctly read as None
                assert_eq!(lsn, 0);
            }
            _ => panic!("expected free metadata"),
        }
    }

    // --- Free page metadata round-trip ---

    #[test]
    fn test_free_page_metadata_round_trip() {
        let mut page = [0u8; PAGE_SIZE];
        let meta = PageMetadata::Free {
            next_page: Some(7),
            lsn: 123,
        };
        PageManager::update_metadata_in_buffer(&mut page, &meta);

        let read_back = PageManager::read_metadata_from_buffer(&page, PageType::Free);
        match read_back {
            PageMetadata::Free { next_page, lsn } => {
                assert_eq!(next_page, Some(7));
                assert_eq!(lsn, 123);
            }
            _ => panic!("expected free metadata"),
        }
    }

    #[test]
    fn test_free_page_metadata_null_next_page() {
        let mut page = [0u8; PAGE_SIZE];
        let meta = PageMetadata::Free {
            next_page: None,
            lsn: 0,
        };
        PageManager::update_metadata_in_buffer(&mut page, &meta);

        let read_back = PageManager::read_metadata_from_buffer(&page, PageType::Free);
        match read_back {
            PageMetadata::Free { next_page, .. } => assert_eq!(next_page, None),
            _ => panic!("expected free metadata"),
        }
    }

    // --- Raw page metadata ---

    #[test]
    fn test_raw_page_metadata_initialization() {
        cleanup("test_raw_meta_init");
        let mut pm = PageManager::new("test_raw_meta_init").unwrap();

        let page_id = pm.allocate_page(PageType::Raw).unwrap();
        let meta = pm.read_page_metadata(page_id, PageType::Raw).unwrap();

        match meta {
            PageMetadata::Raw { lsn } => assert_eq!(lsn, 0),
            _ => panic!("expected raw metadata"),
        }

        cleanup("test_raw_meta_init");
    }

    #[test]
    fn test_raw_page_metadata_update_and_persist() {
        cleanup("test_raw_meta_persist");

        {
            let mut pm = PageManager::new("test_raw_meta_persist").unwrap();
            let page_id = pm.allocate_page(PageType::Raw).unwrap();
            eprintln!("{page_id}");
            pm.update_page_metadata(page_id, &PageMetadata::Raw { lsn: 55 })
                .unwrap();
        }

        {
            let pm = PageManager::new("test_raw_meta_persist").unwrap();
            let meta = pm.read_page_metadata(1, PageType::Raw).unwrap();
            match meta {
                PageMetadata::Raw { lsn } => assert_eq!(lsn, 55),
                _ => panic!("expected raw metadata"),
            }
        }

        cleanup("test_raw_meta_persist");
    }

    // --- NULL_PAGE sentinel ---

    #[test]
    fn test_null_page_sentinel_round_trips_as_none() {
        let mut page = [0u8; PAGE_SIZE];
        PageManager::init_page_metadata_buffer(&mut page, PageType::Slotted);

        // verify NULL_PAGE bytes are written
        let next_bytes = &page[OFFSET_NEXT_PAGE..OFFSET_NEXT_PAGE + 4];
        assert_eq!(next_bytes, &NULL_PAGE.to_le_bytes());

        // verify reading back gives None
        let meta = PageManager::read_metadata_from_buffer(&page, PageType::Slotted);
        assert_eq!(meta.next_page().unwrap(), None);
    }
}
