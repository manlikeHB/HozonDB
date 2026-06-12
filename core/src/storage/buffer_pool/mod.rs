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

    pub fn allocate_slotted_page(&mut self, wal_writer: &mut WalWriter) -> io::Result<PageId> {
        self.allocate_page(wal_writer, PageType::Slotted)
    }

    pub fn allocate_raw_page(&mut self, wal_writer: &mut WalWriter) -> io::Result<PageId> {
        self.allocate_page(wal_writer, PageType::Raw)
    }

    fn allocate_page(
        &mut self,
        wal_writer: &mut WalWriter,
        page_type: PageType,
    ) -> io::Result<PageId> {
        if let Some(free_page_id) = self.page_manager.first_free_page() {
            // read next free pointer from buffer pool frame
            let next_free = self.read_next_free(free_page_id)?;

            // log
            let lsn = wal_writer.append_allocate_page(free_page_id, page_type.to_u8())?;

            // update free list head
            self.page_manager.set_first_free_page(next_free)?;

            // reset page metadata of free page for reuse
            let page_data = self.get_page_mut(free_page_id)?;
            PageManager::init_page_metadata_buffer(page_data, page_type);

            // mark dirty
            self.mark_dirty(free_page_id, lsn)?;

            return Ok(free_page_id);
        }

        // no free pages — delegate extension to PageManager
        let page_id = self.page_manager.allocate_page(page_type)?;

        // log
        wal_writer.append_allocate_page(page_id, page_type.to_u8())?;

        Ok(page_id)
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

        // sync once after all pages written — ensures pages are durable before
        // checkpoint record is written to WAL
        self.page_manager.sync()?;

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
    pub fn mark_dirty(&mut self, page_id: PageId, lsn: u64) -> io::Result<()> {
        let idx = self.page_table.get(&page_id).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("page {} not in buffer pool", page_id),
            )
        })?;
        self.frames[*idx].mark_dirty(lsn);
        Ok(())
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

    pub fn update_next_page_in_page_metadata(
        &mut self,
        page_id: PageId,
        next_page: PageId,
        wal_writer: &mut WalWriter,
    ) -> io::Result<()> {
        // log to WAL
        let lsn = wal_writer.append_link_page(page_id, next_page)?;

        // get page data
        let idx = self.get_frame_idx(page_id)?;
        let page_data = self.frames[idx].data_mut();

        // update page meta
        let mut page_meta = PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);
        page_meta.set_next_page(next_page);
        page_meta.set_lsn(lsn);

        PageManager::update_metadata_in_buffer(page_data, &page_meta);

        self.mark_dirty(page_id, lsn)?;

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
        self.mark_dirty(page_id, lsn)?;
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
        let old_page = self.read_page(page_id)?;

        let lsn = wal_writer.append_raw(WalRecordType::FreePage, page_id, &new_page, old_page)?;
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

    pub fn first_free_page(&self) -> Option<PageId> {
        self.page_manager.first_free_page()
    }

    pub(crate) fn set_first_free_page(&mut self, next_free: Option<PageId>) -> io::Result<()> {
        self.page_manager.set_first_free_page(next_free)
    }

    pub fn is_cached(&self, page_id: PageId) -> bool {
        self.page_table.contains_key(&page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use crate::{
        storage::page::{PageManager, PageType},
        test_helpers::*,
    };

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

        let page1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
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

        let page1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let page2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let page3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        assert_eq!(page1, 1);
        assert_eq!(page2, 2);
        assert_eq!(page3, 3);
        assert_eq!(bp.page_manager.num_pages(), 4);

        bp.free_page(page2, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(2));

        let page4 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(page4, 2);
        assert_eq!(bp.page_manager.first_free_page(), None);
        assert_eq!(bp.page_manager.num_pages(), 4);

        cleanup("test_reuse");
    }

    #[test]
    fn test_free_list_lifo_order() {
        cleanup("test_lifo");
        let (mut bp, mut wal) = setup("test_lifo");

        let page1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let page2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let page3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        bp.free_page(page1, &mut wal).unwrap();
        bp.free_page(page2, &mut wal).unwrap();
        bp.free_page(page3, &mut wal).unwrap();

        assert_eq!(bp.page_manager.first_free_page(), Some(3));

        let realloc1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(realloc1, 3);

        let realloc2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(realloc2, 2);

        let realloc3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(realloc3, 1);

        assert_eq!(bp.page_manager.first_free_page(), None);

        cleanup("test_lifo");
    }

    #[test]
    fn test_free_list_persistence() {
        cleanup("test_free_persist");

        {
            let (mut bp, mut wal) = setup("test_free_persist");

            let _ = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
            let page2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
            let page3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

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
            bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        }
        assert_eq!(bp.page_manager.num_pages(), 6);

        bp.free_page(2, &mut wal).unwrap();
        bp.free_page(3, &mut wal).unwrap();
        bp.free_page(4, &mut wal).unwrap();

        let p1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(p1, 4);
        assert_eq!(p2, 3);
        assert_eq!(bp.page_manager.first_free_page(), Some(2));

        bp.free_page(5, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(5));

        let p3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p4 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p5 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

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

        let page1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        bp.free_page(page1, &mut wal).unwrap();
        assert_eq!(bp.page_manager.first_free_page(), Some(1));

        let p1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(p1, 1);
        assert_eq!(p2, 1);

        cleanup("test_double_free");
    }

    #[test]
    fn test_write_header_updates_first_free() {
        cleanup("test_header_first_free");
        let (mut bp, mut wal) = setup("test_header_first_free");

        let page1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
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
            bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
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
            bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        }

        for i in 0..10 {
            bp.free_page(i, &mut wal).unwrap();
        }

        for _ in 0..10 {
            bp.allocate_page(&mut wal, PageType::Raw).unwrap();
        }

        assert_eq!(bp.page_manager.first_free_page(), None);

        let new_page = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(new_page, 11);

        cleanup("test_allocate_all");
    }

    #[test]
    fn test_read_page_loads_into_frame() {
        cleanup("test_bp_read_page");
        let (mut bp, mut wal) = setup("test_bp_read_page");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        bp.flush_dirty().unwrap(); // ensure page is on disk

        // clear frames to force a disk read
        bp.frames = vec![Frame::default(); 64];
        bp.page_table.clear();

        let data = bp.read_page(page_id).unwrap();
        assert_eq!(data.len(), PAGE_SIZE);

        // page should now be in page_table
        assert!(bp.page_table.contains_key(&page_id));

        cleanup("test_bp_read_page");
    }

    #[test]
    fn test_read_page_cache_hit_no_disk_read() {
        cleanup("test_bp_cache_hit");
        let (mut bp, mut wal) = setup("test_bp_cache_hit");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        // read twice — second should be a cache hit
        bp.read_page(page_id).unwrap();
        let frame_count_before = bp.page_table.len();

        bp.read_page(page_id).unwrap();
        let frame_count_after = bp.page_table.len();

        // frame count unchanged — no new frame allocated
        assert_eq!(frame_count_before, frame_count_after);

        cleanup("test_bp_cache_hit");
    }

    #[test]
    fn test_read_invalid_page_returns_err() {
        cleanup("test_bp_invalid_read");
        let (mut bp, _) = setup("test_bp_invalid_read");

        let result = bp.read_page(999);
        assert!(result.is_err());

        cleanup("test_bp_invalid_read");
    }

    // --- get_page_mut and mark_dirty ---

    #[test]
    fn test_get_page_mut_and_mark_dirty() {
        cleanup("test_bp_mut_dirty");
        let (mut bp, mut wal) = setup("test_bp_mut_dirty");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        {
            let page = bp.get_page_mut(page_id).unwrap();
            page[100] = 0xAB;
        }

        bp.mark_dirty(page_id, 5).unwrap();

        let idx = bp.page_table[&page_id];
        assert!(bp.frames[idx].dirty());
        assert_eq!(bp.frames[idx].last_lsn(), 5);
        assert_eq!(bp.frames[idx].data()[100], 0xAB);

        cleanup("test_bp_mut_dirty");
    }

    #[test]
    fn test_mark_dirty_unknown_page_returns_err() {
        cleanup("test_bp_dirty_unknown");
        let (mut bp, _) = setup("test_bp_dirty_unknown");

        let result = bp.mark_dirty(999, 1);
        assert!(result.is_err());

        cleanup("test_bp_dirty_unknown");
    }

    // --- flush_dirty ---

    #[test]
    fn test_flush_dirty_writes_to_disk() {
        cleanup("test_bp_flush");
        let (mut bp, mut wal) = setup("test_bp_flush");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        {
            let page = bp.get_page_mut(page_id).unwrap();
            page[200] = 0xFF;
        }
        bp.mark_dirty(page_id, 1).unwrap();

        bp.flush_dirty().unwrap();

        // verify frame is now clean
        let idx = bp.page_table[&page_id];
        assert!(!bp.frames[idx].dirty());

        // verify data persisted — clear frames and re-read from disk
        bp.frames = vec![Frame::default(); 64];
        bp.page_table.clear();

        let data = bp.read_page(page_id).unwrap();
        assert_eq!(data[200], 0xFF);

        cleanup("test_bp_flush");
    }

    #[test]
    fn test_flush_dirty_skips_clean_frames() {
        cleanup("test_bp_flush_clean");
        let (mut bp, mut wal) = setup("test_bp_flush_clean");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        // read page into frame but don't mark dirty
        bp.read_page(page_id).unwrap();

        let idx = bp.page_table[&page_id];
        assert!(!bp.frames[idx].dirty());

        // flush should not error and frame stays clean
        bp.flush_dirty().unwrap();
        assert!(!bp.frames[idx].dirty());

        cleanup("test_bp_flush_clean");
    }

    #[test]
    fn test_flush_dirty_persistence_across_restart() {
        cleanup("test_bp_persist");

        {
            let (mut bp, mut wal) = setup("test_bp_persist");
            let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

            {
                let page = bp.get_page_mut(page_id).unwrap();
                page[50] = 0xCD;
            }
            bp.mark_dirty(page_id, 1).unwrap();
            bp.flush_dirty().unwrap();
        }

        {
            let (mut bp, _) = setup("test_bp_persist");
            let page_id = 1; // first allocated page
            let data = bp.read_page(page_id).unwrap();
            assert_eq!(data[50], 0xCD);
        }

        cleanup("test_bp_persist");
    }

    // --- write_raw_page ---

    #[test]
    fn test_write_raw_page_updates_frame_and_lsn() {
        cleanup("test_bp_raw_write");
        let (mut bp, mut wal) = setup("test_bp_raw_write");

        let page_id = bp.allocate_raw_page(&mut wal).unwrap();

        let mut new_data = [0u8; PAGE_SIZE];
        new_data[8] = 0x55; // after raw page metadata region

        bp.write_raw_page(page_id, &new_data, 7).unwrap();

        let idx = bp.page_table[&page_id];
        assert!(bp.frames[idx].dirty());
        assert_eq!(bp.frames[idx].last_lsn(), 7);
        assert_eq!(bp.frames[idx].data()[8], 0x55);

        cleanup("test_bp_raw_write");
    }

    #[test]
    fn test_write_raw_page_stamps_lsn_in_metadata() {
        cleanup("test_bp_raw_lsn");
        let (mut bp, mut wal) = setup("test_bp_raw_lsn");

        let page_id = bp.allocate_raw_page(&mut wal).unwrap();
        let new_data = [0u8; PAGE_SIZE];

        bp.write_raw_page(page_id, &new_data, 99).unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Raw).unwrap();
        assert_eq!(meta.lsn(), 99);

        cleanup("test_bp_raw_lsn");
    }

    // --- eviction ---

    #[test]
    fn test_eviction_when_pool_full() {
        cleanup("test_bp_evict");
        let pm = PageManager::new("test_bp_evict").unwrap();
        let mut bp = BufferPool::new(pm, 3); // tiny pool — 3 frames
        let mut wal = WalWriter::new("test_bp_evict").unwrap();

        // allocate 4 pages — one more than pool capacity
        let p1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p4 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        bp.flush_dirty().unwrap(); // get clean frames

        // clear frames to simulate cold start
        bp.frames = vec![Frame::default(); 3];
        bp.page_table.clear();

        // load 3 pages — fills pool
        bp.read_page(p1).unwrap();
        bp.read_page(p2).unwrap();
        bp.read_page(p3).unwrap();
        assert_eq!(bp.page_table.len(), 3);

        // reading p4 should trigger eviction
        bp.read_page(p4).unwrap();
        assert_eq!(bp.page_table.len(), 3); // still 3 frames
        assert!(bp.page_table.contains_key(&p4)); // p4 loaded

        cleanup("test_bp_evict");
    }

    #[test]
    fn test_eviction_flushes_dirty_frame() {
        cleanup("test_bp_evict_dirty");
        let pm = PageManager::new("test_bp_evict_dirty").unwrap();
        let mut bp = BufferPool::new(pm, 2); // 2 frames only
        let mut wal = WalWriter::new("test_bp_evict_dirty").unwrap();

        let p1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        bp.flush_dirty().unwrap();
        bp.frames = vec![Frame::default(); 2];
        bp.page_table.clear();

        // load p1, mutate, mark dirty
        {
            let page = bp.get_page_mut(p1).unwrap();
            page[10] = 0xBB;
        }
        bp.mark_dirty(p1, 1).unwrap();

        // load p2
        bp.read_page(p2).unwrap();

        // loading p3 must evict — p1 or p2, whichever clock lands on
        // either way no error means dirty frame was flushed correctly
        bp.read_page(p3).unwrap();

        // verify p1's mutation persisted if it was evicted
        bp.frames = vec![Frame::default(); 2];
        bp.page_table.clear();

        let data = bp.read_page(p1).unwrap();
        assert_eq!(data[10], 0xBB);

        cleanup("test_bp_evict_dirty");
    }

    #[test]
    fn test_eviction_clock_hand_advances() {
        cleanup("test_bp_clock");
        let pm = PageManager::new("test_bp_clock").unwrap();
        let mut bp = BufferPool::new(pm, 2);
        let mut wal = WalWriter::new("test_bp_clock").unwrap();

        let p1 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p2 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        let p3 = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();

        bp.flush_dirty().unwrap();
        bp.frames = vec![Frame::default(); 2];
        bp.page_table.clear();

        bp.read_page(p1).unwrap();
        bp.read_page(p2).unwrap();

        let hand_before = bp.clock_hand;

        // clear referenced bits so eviction happens on first pass
        bp.frames[0].clear_referenced();
        bp.frames[1].clear_referenced();

        bp.read_page(p3).unwrap();

        // clock hand should have advanced
        assert_ne!(bp.clock_hand, hand_before);

        cleanup("test_bp_clock");
    }

    // --- link page ---

    #[test]
    fn test_update_next_page_in_metadata() {
        cleanup("test_bp_next_page");
        let (mut bp, mut wal) = setup("test_bp_next_page");

        // page 1 and 2 for catalogs
        let _ = bp.allocate_raw_page(&mut wal).unwrap();
        let _ = bp.allocate_raw_page(&mut wal).unwrap();

        // allocate two pages
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();
        let next_page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        // verify next_page is None initially
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), None);

        // link pages
        bp.update_next_page_in_page_metadata(page_id, next_page_id, &mut wal)
            .unwrap();

        // verify next_page updated
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), Some(next_page_id));

        // verify frame is dirty with a valid lsn
        let idx = bp.page_table[&page_id];
        assert!(bp.frames[idx].dirty());
        assert!(bp.frames[idx].last_lsn() > 0);

        cleanup("test_bp_next_page");
    }

    #[test]
    fn test_update_next_page_persists_after_flush() {
        cleanup("test_bp_next_page_persist");

        let next_page_id;
        let page_id;

        {
            let (mut bp, mut wal) = setup("test_bp_next_page_persist");
            page_id = bp.allocate_slotted_page(&mut wal).unwrap();
            next_page_id = bp.allocate_slotted_page(&mut wal).unwrap();

            bp.update_next_page_in_page_metadata(page_id, next_page_id, &mut wal)
                .unwrap();
            bp.flush_dirty().unwrap();
        }

        {
            let (mut bp, _) = setup("test_bp_next_page_persist");
            let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
            assert_eq!(meta.next_page().unwrap(), Some(next_page_id));
        }

        cleanup("test_bp_next_page_persist");
    }

    #[test]
    fn test_allocate_from_free_list_stamps_real_lsn() {
        // Verifies that a page allocated from the free list is marked dirty
        // with the LSN from the AllocatePage WAL record, not 0.
        // LSN 0 means "no WAL record" — a reused page stamped with 0 would
        // be skipped by LSN comparison during recovery even if its WAL record
        // exists.

        cleanup("test_alloc_reuse_lsn");
        let (mut bp, mut wal) = setup("test_alloc_reuse_lsn");

        let page_id = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        bp.free_page(page_id, &mut wal).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // allocate from free list — should stamp real LSN
        let reused = bp.allocate_page(&mut wal, PageType::Slotted).unwrap();
        assert_eq!(reused, page_id);

        let idx = bp.page_table[&reused];
        assert!(bp.frames[idx].dirty());
        assert!(
            bp.frames[idx].last_lsn() > 0,
            "reused page should be stamped with WAL LSN, not 0"
        );

        cleanup("test_alloc_reuse_lsn");
    }
}
