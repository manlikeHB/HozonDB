pub mod frame;

use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};

use crate::storage::buffer_pool::frame::Frame;
use crate::storage::page::{PageMetadata, PageType};
use crate::wal::record_type::WalRecordType;
use crate::wal::writer::WalWriter;
use crate::{
    constants::PageId,
    storage::page::{PAGE_SIZE, PageManager},
};

pub struct BufferPool {
    page_manager: PageManager,
    frames: Vec<Frame>,
    page_table: HashMap<PageId, usize>,
    clock_hand: usize,
}

impl BufferPool {
    pub fn new(page_manager: PageManager, capacity: usize) -> Self {
        BufferPool {
            page_manager,
            frames: vec![Frame::default(); capacity],
            page_table: HashMap::new(),
            clock_hand: 0,
        }
    }

    pub fn read_page(&mut self, page_id: PageId) -> io::Result<&[u8; PAGE_SIZE]> {
        // check if page is already in page_table
        let idx = self.get_frame_idx(page_id)?;

        Ok(self.frames[idx].data())
    }

    pub fn allocate_slotted_page(&mut self) -> io::Result<PageId> {
        self.allocate_page(PageType::Slotted)
    }

    pub fn allocate_raw_page(&mut self) -> io::Result<PageId> {
        self.allocate_page(PageType::Raw)
    }

    fn allocate_page(&mut self, page_type: PageType) -> io::Result<PageId> {
        if let Some(free_page_id) = self.page_manager.first_free_page() {
            // read next free pointer from buffer pool frame
            let next_free = self.read_next_free(free_page_id)?;

            // update free list head
            self.page_manager.set_first_free_page(next_free)?;

            // reset page metadata of free page for reuse
            let page_data = self.get_page_mut(free_page_id)?;
            PageManager::init_page_metadata_buffer(page_data, page_type);

            return Ok(free_page_id);
        }

        // no free pages — delegate extension to PageManager
        self.page_manager.allocate_page(page_type)
    }

    // flush all dirty frames to disk — called at checkpoint
    pub(crate) fn flush_dirty(&mut self) -> io::Result<()> {
        for frame in self.frames.iter_mut() {
            if frame.is_empty() || !frame.dirty() {
                continue;
            }

            let page_id = frame.page_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    "A non empty frame should have a page ID",
                )
            })?;

            self.page_manager.write_page(page_id, frame.data())?;
            frame.mark_clean();
        }

        Ok(())
    }

    pub fn get_page_mut(&mut self, page_id: PageId) -> io::Result<&mut [u8; PAGE_SIZE]> {
        let idx = self.get_frame_idx(page_id)?;
        Ok(self.frames[idx].data_mut())
    }

    /// Get frame index of a page or assign new frame
    fn get_frame_idx(&mut self, page_id: PageId) -> io::Result<usize> {
        if !self.page_table.contains_key(&page_id) {
            let page_data = self.page_manager.read_page(page_id)?;

            // check for empty frame
            if let Some(idx) = self.frames.iter().position(|frame| frame.is_empty()) {
                self.frames[idx].load(page_id, page_data);
                self.page_table.insert(page_id, idx);
                return Ok(idx);
            }

            // evict LRU frame
            let idx = self.evict()?;
            self.frames[idx].load(page_id, page_data);
            self.page_table.insert(page_id, idx);
            return Ok(idx);
        }
        Ok(self.page_table[&page_id])
    }

    fn evict(&mut self) -> io::Result<usize> {
        let capacity = self.frames.len();

        for i in 0..capacity * 2 {
            let idx = (self.clock_hand + i) % capacity;

            if self.frames[idx].pin_count() > 0 {
                continue;
            }

            if self.frames[idx].referenced() {
                self.frames[idx].clear_referenced();
                continue;
            }

            // victim found
            if self.frames[idx].dirty() {
                let page_id = self.frames[idx].page_id().ok_or_else(|| {
                    Error::new(ErrorKind::InvalidData, "Non empty frame has no page ID")
                })?;
                self.page_manager
                    .write_page(page_id, self.frames[idx].data())?;
            }

            if let Some(page_id) = self.frames[idx].page_id() {
                self.page_table.remove(&page_id);
            }

            self.clock_hand = (idx + 1) % capacity;
            return Ok(idx);
        }

        Err(Error::new(ErrorKind::Other, "No evictable frames found"))
    }

    // TODO: - prevent making a non dirty page dirty?
    pub fn mark_dirty(&mut self, page_id: PageId, lsn: u64) {
        self.frames[self.page_table[&page_id]].mark_dirty(lsn);
    }

    pub fn read_page_metadata(
        &mut self,
        page_id: PageId,
        page_type: PageType,
    ) -> io::Result<PageMetadata> {
        let idx = self.get_frame_idx(page_id)?;

        let page_data = self.frames[idx].data();
        Ok(PageManager::read_metadata_from_buffer(page_data, page_type))
    }

    pub fn update_page_metadata(
        &mut self,
        page_id: PageId,
        metadata: &PageMetadata,
    ) -> io::Result<()> {
        let idx = self.get_frame_idx(page_id)?;

        let page_data = self.frames[idx].data_mut();
        PageManager::update_metadata_in_buffer(page_data, metadata);
        Ok(())
    }

    pub fn total_num_of_db_pages(&self) -> u32 {
        self.page_manager.num_pages()
    }

    /// Writes the full 4kb raw page and marks page dirty
    pub fn write_raw_page(
        &mut self,
        page_id: PageId,
        new_data: &[u8; PAGE_SIZE],
        lsn: u64,
    ) -> io::Result<()> {
        self.write_page(page_id, new_data, lsn, PageType::Raw)
    }

    fn write_free_page(
        &mut self,
        page_id: PageId,
        new_data: &[u8; PAGE_SIZE],
        lsn: u64,
    ) -> io::Result<()> {
        self.write_page(page_id, new_data, lsn, PageType::Free)
    }

    fn write_page(
        &mut self,
        page_id: PageId,
        new_data: &[u8; PAGE_SIZE],
        lsn: u64,
        page_type: PageType,
    ) -> io::Result<()> {
        let page_data = self.get_page_mut(page_id)?;

        // write full page
        page_data.copy_from_slice(new_data);

        // update lsn in page metadata
        let mut page_meta = PageManager::read_metadata_from_buffer(page_data, page_type);
        page_meta.set_lsn(lsn);
        PageManager::update_metadata_in_buffer(page_data, &page_meta);

        // mark dirty
        self.mark_dirty(page_id, lsn);
        Ok(())
    }

    pub fn free_page(&mut self, page_id: PageId, wal_writer: &mut WalWriter) -> io::Result<()> {
        // build new free page content
        let next_free = self.page_manager.first_free_page();
        let mut new_page = [0u8; PAGE_SIZE];
        let page_meta = PageMetadata::Free {
            next_page: next_free,
            lsn: 0,
        };
        PageManager::update_metadata_in_buffer(&mut new_page, &page_meta);

        // log and write through buffer pool
        let old_page = self.page_manager.read_page(page_id)?;
        let lsn = wal_writer.append_raw(WalRecordType::FreePage, page_id, &new_page, &old_page)?;
        self.write_free_page(page_id, &new_page, lsn)?;

        // update free list head and persist header
        self.page_manager.set_first_free_page(Some(page_id))?;

        Ok(())
    }

    /// Read next_free pointer from a free page
    pub fn read_next_free(&mut self, page_id: PageId) -> io::Result<Option<PageId>> {
        let page_data = self.read_page(page_id)?;
        let page_meta = PageManager::read_metadata_from_buffer(page_data, PageType::Free);
        page_meta.next_page()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{PageManager, PageType};
    use crate::wal::writer::WalWriter;
    use std::fs;

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
        let _ = fs::remove_file(format!("{}.wal", basename));
    }

    fn setup(basename: &str) -> (BufferPool, WalWriter) {
        let pm = PageManager::new(basename).unwrap();
        let bp = BufferPool::new(pm, 64);
        let wal = WalWriter::new(basename).unwrap();
        (bp, wal)
    }

    #[test]
    fn test_free_page_adds_to_list() {
        cleanup("test_free_add");
        let (mut bp, mut wal) = setup("test_free_add");

        let page1 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page1, 1);
        assert_eq!(bp.page_manager.first_free_page(), None);

        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        cleanup("test_free_add");
    }

    #[test]
    fn test_allocate_reuses_freed_page() {
        cleanup("test_reuse");
        let (mut bp, mut wal) = setup("test_reuse");

        let page1 = bp.allocate_page(PageType::Slotted).unwrap();
        let page2 = bp.allocate_page(PageType::Slotted).unwrap();
        let page3 = bp.allocate_page(PageType::Slotted).unwrap();

        assert_eq!(page1, 1);
        assert_eq!(page2, 2);
        assert_eq!(page3, 3);
        assert_eq!(bp.page_manager.num_pages(), 4);

        bp.free_page(page2, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(2));

        let page4 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(page4, 2);
        assert_eq!(bp.page_manager.first_free_page(), None);
        assert_eq!(bp.page_manager.num_pages(), 4);

        cleanup("test_reuse");
    }

    #[test]
    fn test_free_list_lifo_order() {
        cleanup("test_lifo");
        let (mut bp, mut wal) = setup("test_lifo");

        let page1 = bp.allocate_page(PageType::Slotted).unwrap();
        let page2 = bp.allocate_page(PageType::Slotted).unwrap();
        let page3 = bp.allocate_page(PageType::Slotted).unwrap();

        bp.free_page(page1, &mut wal).unwrap();
        bp.free_page(page2, &mut wal).unwrap();
        bp.free_page(page3, &mut wal).unwrap();

        assert_eq!(bp.page_manager.first_free_page(), Some(3));

        let realloc1 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(realloc1, 3);

        let realloc2 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(realloc2, 2);

        let realloc3 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(realloc3, 1);

        assert_eq!(bp.page_manager.first_free_page(), None);

        cleanup("test_lifo");
    }

    #[test]
    fn test_free_list_persistence() {
        cleanup("test_free_persist");

        {
            let (mut bp, mut wal) = setup("test_free_persist");

            let _ = bp.allocate_page(PageType::Slotted).unwrap();
            let page2 = bp.allocate_page(PageType::Slotted).unwrap();
            let page3 = bp.allocate_page(PageType::Slotted).unwrap();

            bp.free_page(page2, &mut wal).unwrap();
            bp.free_page(page3, &mut wal).unwrap();

            assert_eq!(bp.page_manager.first_free_page(), Some(3));
        }

        {
            let (bp, _) = setup("test_free_persist");
            assert_eq!(bp.page_manager.first_free_page(), Some(3));
        }

        cleanup("test_free_persist");
    }

    #[test]
    fn test_multiple_free_and_allocate_cycles() {
        cleanup("test_cycles");
        let (mut bp, mut wal) = setup("test_cycles");

        for _ in 0..5 {
            bp.allocate_page(PageType::Slotted).unwrap();
        }
        assert_eq!(bp.page_manager.num_pages(), 6);

        bp.free_page(2, &mut wal).unwrap();
        bp.free_page(3, &mut wal).unwrap();
        bp.free_page(4, &mut wal).unwrap();

        let p1 = bp.allocate_page(PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(p1, 4);
        assert_eq!(p2, 3);
        assert_eq!(bp.page_manager.first_free_page(), Some(2));

        bp.free_page(5, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(5));

        let p3 = bp.allocate_page(PageType::Slotted).unwrap();
        let p4 = bp.allocate_page(PageType::Slotted).unwrap();
        let p5 = bp.allocate_page(PageType::Slotted).unwrap();

        assert_eq!(p3, 5);
        assert_eq!(p4, 2);
        assert_eq!(p5, 6);

        assert_eq!(bp.page_manager.first_free_page(), None);
        assert_eq!(bp.page_manager.num_pages(), 7);

        cleanup("test_cycles");
    }

    #[test]
    // known limitation: double free creates a cycle in the free list.
    // production fix: track allocated pages and reject double frees.
    fn test_free_same_page_twice() {
        cleanup("test_double_free");
        let (mut bp, mut wal) = setup("test_double_free");

        let page1 = bp.allocate_page(PageType::Slotted).unwrap();

        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        let p1 = bp.allocate_page(PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(p1, 1);
        assert_eq!(p2, 1);

        cleanup("test_double_free");
    }

    #[test]
    fn test_write_header_updates_first_free() {
        cleanup("test_header_first_free");
        let (mut bp, mut wal) = setup("test_header_first_free");

        let page1 = bp.allocate_page(PageType::Slotted).unwrap();
        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        drop(bp);

        let (bp, _) = setup("test_header_first_free");
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        cleanup("test_header_first_free");
    }

    #[test]
    fn test_free_list_chain_integrity() {
        cleanup("test_chain");
        let (mut bp, mut wal) = setup("test_chain");

        for _ in 1..=6 {
            bp.allocate_page(PageType::Slotted).unwrap();
        }

        // page 0, 1, 2 reserved
        bp.free_page(3, &mut wal).unwrap();
        bp.free_page(4, &mut wal).unwrap();
        bp.free_page(6, &mut wal).unwrap();

        assert_eq!(bp.page_manager.first_free_page(), Some(6));

        let next1 = bp.read_next_free(6).unwrap();
        assert_eq!(next1, Some(4));

        let next2 = bp.read_next_free(4).unwrap();
        assert_eq!(next2, Some(3));

        let next3 = bp.read_next_free(3).unwrap();
        assert_eq!(next3, None);

        cleanup("test_chain");
    }

    #[test]
    fn test_allocate_all_freed_pages() {
        cleanup("test_allocate_all");
        let (mut bp, mut wal) = setup("test_allocate_all");

        for _ in 0..10 {
            bp.allocate_page(PageType::Slotted).unwrap();
        }

        for i in 0..10 {
            bp.free_page(i, &mut wal).unwrap();
        }

        for _ in 0..10 {
            bp.allocate_page(PageType::Raw).unwrap();
        }

        assert_eq!(bp.page_manager.first_free_page(), None);

        let new_page = bp.allocate_page(PageType::Slotted).unwrap();
        assert_eq!(new_page, 11);

        cleanup("test_allocate_all");
    }
}
