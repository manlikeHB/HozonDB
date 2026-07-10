use std::io::{self, ErrorKind};

use crate::{
    storage::{
        buffer_pool::BufferPool,
        page::{PAGE_SIZE, PageManager, PageMetadata, PageType},
    },
    wal::record::WalRecord,
};

pub fn undo_insert(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Insert WAL record should have a page id",
        )
    })?;

    let slot = record.slot().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Insert WAL record should have a slot index",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;

    // mark slot dead
    PageManager::mark_slot_dead(page_data, slot);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}
pub fn undo_update(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have a page id",
        )
    })?;

    let slot_index = record.slot().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have a slot index",
        )
    })?;

    let old_data = record.old_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have old data",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;
    let (row_offset, _) = PageManager::read_slot(page_data, slot_index);

    // restore old data
    page_data[row_offset as usize..row_offset as usize + old_data.len()].copy_from_slice(old_data);
    // update slot
    PageManager::write_slot(page_data, slot_index, row_offset, old_data.len() as u16);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}

pub fn undo_delete(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Delete WAL record should have a page id",
        )
    })?;

    let slot_index = record.slot().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Delete WAL record should have a slot index",
        )
    })?;

    let old_data = record.old_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Delete WAL record should have old data",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;
    let (row_offset, _) = PageManager::read_slot(page_data, slot_index);

    // restore old data
    page_data[row_offset as usize..row_offset as usize + old_data.len()].copy_from_slice(old_data);

    // update slot (mark slot alive)
    PageManager::write_slot(page_data, slot_index, row_offset, old_data.len() as u16);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}

pub fn undo_raw(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Raw WAL record should have a page id",
        )
    })?;

    let old_data = record.old_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Raw WAL record should have old data",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;
    page_data[..].copy_from_slice(old_data);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}

pub fn undo_allocate_page(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "AllocatePage WAL record missing page_id",
        )
    })?;

    // get current free list head before mutating header
    let current_first_free = buffer_pool.first_free_page()?;

    // write Free metadata to the page, pointing to current free list head
    let page_data = buffer_pool.get_page_mut(page_id)?;
    *page_data = [0u8; PAGE_SIZE];
    let free_meta = PageMetadata::Free {
        next_page: current_first_free,
        lsn: abort_lsn,
    };
    PageManager::update_metadata_in_buffer(page_data, &free_meta);
    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    // update free list head to point to this page
    buffer_pool.set_first_free_page(Some(page_id), abort_lsn)?;

    Ok(())
}

pub fn undo_free_page(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "FreePage WAL record missing page_id",
        )
    })?;

    let old_data = record.old_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "FreePage WAL record missing old_data",
        )
    })?;

    if old_data.len() != PAGE_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("FreePage old_data length {} != PAGE_SIZE", old_data.len()),
        ));
    }

    // this page was the free list head when it was freed —
    // its next_page pointer tells us what first_free_page should be restored to
    let next_free = buffer_pool.read_next_free(page_id)?;

    // restore page to its pre-free state
    let page_data = buffer_pool.get_page_mut(page_id)?;
    page_data.copy_from_slice(old_data);
    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    // restore free list head to skip this page
    buffer_pool.set_first_free_page(next_free, abort_lsn)?;

    Ok(())
}

pub fn undo_link_page(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Link Page WAL record should have a page id",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;
    let mut meta = PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);

    // update next page to point to previous next_page
    meta.set_next_page(record.old_next_page());
    PageManager::update_metadata_in_buffer(page_data, &meta);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageManager;
    use crate::test_helpers::cleanup;
    use crate::wal::{record_type::WalRecordType, writer::WalWriter};

    fn setup(name: &str) -> (BufferPool, WalWriter) {
        let pm = crate::storage::page::PageManager::new(name).unwrap();
        let bp = BufferPool::new(pm, 64).unwrap();
        let wal = WalWriter::new(name).unwrap();
        (bp, wal)
    }

    #[test]
    fn test_undo_insert_marks_slot_dead() {
        cleanup("test_undo_insert");
        let (mut bp, mut wal) = setup("test_undo_insert");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let row_bytes = b"a row";

        // simulate an applied insert at slot 0
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let offset = meta.free_space_end().unwrap() as usize - row_bytes.len();
            page[offset..offset + row_bytes.len()].copy_from_slice(row_bytes);
            PageManager::write_slot(page, 0, offset as u16, row_bytes.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(row_bytes.len());
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        let record = WalRecord::new_slotted(
            10,
            WalRecordType::Insert,
            "users",
            page_id,
            0,
            row_bytes,
            &[],
            1,
        );

        undo_insert(&record, &mut bp, 99).unwrap();

        let page = bp.read_page(page_id).unwrap();
        let (_, len) = PageManager::read_slot(page, 0);
        assert_eq!(len, 0, "slot should be marked dead after undoing an insert");

        cleanup("test_undo_insert");
    }

    #[test]
    fn test_undo_update_restores_old_data() {
        cleanup("test_undo_update");
        let (mut bp, mut wal) = setup("test_undo_update");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let old_data = b"aaaaaaaa";
        let new_data = b"bbbbbbbb";

        // simulate page currently holding post-update ("new") bytes at slot 0
        let row_offset = {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let offset = meta.free_space_end().unwrap() as usize - new_data.len();
            page[offset..offset + new_data.len()].copy_from_slice(new_data);
            PageManager::write_slot(page, 0, offset as u16, new_data.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(new_data.len());
            PageManager::update_metadata_in_buffer(page, &meta);
            offset as u16
        };

        let record = WalRecord::new_slotted(
            10,
            WalRecordType::Update,
            "users",
            page_id,
            0,
            new_data,
            old_data,
            1,
        );

        undo_update(&record, &mut bp, 99).unwrap();

        let page = bp.read_page(page_id).unwrap();
        assert_eq!(
            &page[row_offset as usize..row_offset as usize + old_data.len()],
            old_data
        );
        let (_, len) = PageManager::read_slot(page, 0);
        assert_eq!(len as usize, old_data.len());

        cleanup("test_undo_update");
    }

    #[test]
    fn test_undo_delete_revives_slot_and_restores_row() {
        cleanup("test_undo_delete");
        let (mut bp, mut wal) = setup("test_undo_delete");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let row_bytes = b"a deleted row";

        let row_offset = {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let offset = meta.free_space_end().unwrap() as usize - row_bytes.len();
            page[offset..offset + row_bytes.len()].copy_from_slice(row_bytes);
            PageManager::write_slot(page, 0, offset as u16, row_bytes.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(row_bytes.len());
            PageManager::update_metadata_in_buffer(page, &meta);
            offset as u16
        };

        // simulate the delete: mark slot dead (bytes stay in place, only length zeroed)
        {
            let page = bp.get_page_mut(page_id).unwrap();
            PageManager::mark_slot_dead(page, 0);
        }

        let record = WalRecord::new_slotted(
            10,
            WalRecordType::Delete,
            "users",
            page_id,
            0,
            &[],
            row_bytes,
            1,
        );

        undo_delete(&record, &mut bp, 99).unwrap();

        let page = bp.read_page(page_id).unwrap();
        let (offset, len) = PageManager::read_slot(page, 0);
        assert_eq!(offset, row_offset);
        assert_eq!(len as usize, row_bytes.len());
        assert_eq!(
            &page[offset as usize..offset as usize + row_bytes.len()],
            row_bytes
        );

        cleanup("test_undo_delete");
    }

    #[test]
    fn test_undo_raw_restores_old_page_bytes() {
        cleanup("test_undo_raw");
        let (mut bp, mut wal) = setup("test_undo_raw");

        let (page_id, _, _) = bp.allocate_raw_page(&mut wal, 1).unwrap();
        let old_page = bp.read_page(page_id).unwrap().clone();

        let mut new_page = old_page;
        new_page[100] = 0xFF;
        bp.write_raw_page(page_id, &new_page, 5).unwrap();

        let record = WalRecord::new_raw(
            5,
            WalRecordType::IndexNode,
            page_id,
            &new_page,
            &old_page,
            1,
        );

        undo_raw(&record, &mut bp, 77).unwrap();

        let page = bp.read_page(page_id).unwrap();
        assert_eq!(page, &old_page);

        cleanup("test_undo_raw");
    }

    #[test]
    fn test_undo_allocate_page_returns_page_to_free_list() {
        cleanup("test_undo_alloc");
        let (mut bp, mut wal) = setup("test_undo_alloc");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let prev_head = bp.first_free_page().unwrap(); // None, nothing freed yet

        let record = WalRecord::new_allocate_page(3, page_id, PageType::Slotted.to_u8(), 1);
        undo_allocate_page(&record, &mut bp, 88).unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Free).unwrap();
        assert_eq!(meta.next_page().unwrap(), prev_head);
        assert_eq!(bp.first_free_page().unwrap(), Some(page_id));

        cleanup("test_undo_alloc");
    }

    #[test]
    fn test_undo_free_page_restores_page_and_free_list_head() {
        cleanup("test_undo_free");
        let (mut bp, mut wal) = setup("test_undo_free");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let old_page = bp.read_page(page_id).unwrap().clone();
        let old_lsn = PageManager::read_metadata_from_buffer(&old_page, PageType::Slotted).lsn();

        bp.free_page(page_id, &mut wal, 1).unwrap();
        assert_eq!(bp.first_free_page().unwrap(), Some(page_id));

        let record = WalRecord::new_raw(20, WalRecordType::FreePage, page_id, &[], &old_page, 1);
        undo_free_page(&record, &mut bp, 123).unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.lsn(), old_lsn);
        assert_eq!(bp.first_free_page().unwrap(), None);

        cleanup("test_undo_free");
    }

    #[test]
    fn test_undo_link_page_restores_previous_next_page() {
        cleanup("test_undo_link");
        let (mut bp, mut wal) = setup("test_undo_link");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (old_next_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (new_next_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        // simulate page_id already pointing at old_next_id before being relinked
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            meta.set_next_page(Some(old_next_id));
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        // simulate the link having been applied: next_page now points at new_next_id
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            meta.set_next_page(Some(new_next_id));
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        let record = WalRecord::new_link_page(10, page_id, new_next_id, 1, Some(old_next_id));
        undo_link_page(&record, &mut bp, 55).unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), Some(old_next_id));

        cleanup("test_undo_link");
    }

    #[test]
    fn test_undo_link_page_restores_to_no_next_page() {
        cleanup("test_undo_link_none");
        let (mut bp, mut wal) = setup("test_undo_link_none");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (next_page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        // a freshly allocated page starts with next_page = None, so simulating
        // just the "link applied" step covers the chain-tail-extension case
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            meta.set_next_page(Some(next_page_id));
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        let record = WalRecord::new_link_page(10, page_id, next_page_id, 1, None);
        undo_link_page(&record, &mut bp, 55).unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), None);

        cleanup("test_undo_link_none");
    }
}
