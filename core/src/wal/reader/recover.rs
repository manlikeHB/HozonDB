use crate::{
    constants::PageId,
    storage::{
        buffer_pool::BufferPool,
        page::{PAGE_SIZE, PageManager, PageMetadata, PageType},
    },
    wal::{record::WalRecord, record_type::WalRecordType},
};
use std::io::{self, ErrorKind};

pub fn recover_insert(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Insert WAL record should have a page id",
        )
    })?;
    let mut page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
    let page_data = buffer_pool.get_page_mut(page_id)?;

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    if let PageMetadata::Slotted {
        slot_count,
        free_space_end,
        ..
    } = page_meta
    {
        // insert row
        let row_bytes = record.new_data().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "Insert WAL record should have new data bytes",
            )
        })?;
        let row_offset = free_space_end as usize - row_bytes.len();
        page_data[row_offset..row_offset + row_bytes.len()].copy_from_slice(row_bytes);

        // rebuild slot
        PageManager::write_slot(
            page_data,
            slot_count,
            row_offset as u16,
            row_bytes.len() as u16,
        );

        // update metadata
        page_meta.update_slot_count();
        page_meta.update_free_space_start();
        page_meta.update_free_space_end(row_bytes.len());
        page_meta.set_lsn(record.lsn());

        PageManager::update_metadata_in_buffer(page_data, &page_meta);

        buffer_pool.mark_dirty(page_id, record.lsn())?;

        Ok(())
    } else {
        Err(io::Error::new(
            ErrorKind::InvalidData,
            "Expected slotted page metadata",
        ))
    }
}

pub fn recover_update(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have a page id",
        )
    })?;
    let mut page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
    let page_data = buffer_pool.get_page_mut(page_id)?;

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    // insert row
    let row_bytes = record.new_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have new data bytes",
        )
    })?;

    let slot_index = record.slot().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Update WAL record should have slot index",
        )
    })?;

    let (slot_offset, _) = PageManager::read_slot(page_data, slot_index);
    page_data[slot_offset as usize..slot_offset as usize + row_bytes.len()]
        .copy_from_slice(row_bytes);

    // update slot length
    PageManager::write_slot(page_data, slot_index, slot_offset, row_bytes.len() as u16);

    // update metadata
    page_meta.set_lsn(record.lsn());
    PageManager::update_metadata_in_buffer(page_data, &page_meta);

    buffer_pool.mark_dirty(page_id, record.lsn())?;

    Ok(())
}

pub fn recover_delete(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Delete WAL record should have a page id",
        )
    })?;
    let mut page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
    let page_data = buffer_pool.get_page_mut(page_id)?;

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    // mark slot dead
    PageManager::mark_slot_dead(
        page_data,
        record.slot().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                "Slotted WAL record should have a slot index",
            )
        })?,
    );

    page_meta.set_lsn(record.lsn());
    PageManager::update_metadata_in_buffer(page_data, &page_meta);

    buffer_pool.mark_dirty(page_id, record.lsn())?;

    Ok(())
}

pub fn replay_raw_page_new_data(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
) -> io::Result<()> {
    let record_type = record.record_type().ok_or_else(|| {
        io::Error::new(ErrorKind::InvalidData, "Raw WAL record missing record type")
    })?;

    let page_id = record
        .page_id()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Raw WAL record missing page id"))?;

    let new_data = record
        .new_data()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Raw WAL record missing new data"))?;

    if new_data.len() != PAGE_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Raw WAL record new_data length {} != PAGE_SIZE",
                new_data.len()
            ),
        ));
    }

    // FreePage is always applied — LSN comparison is unreliable because
    // the page type on disk may differ from what the record expects
    if record_type != WalRecordType::FreePage {
        let page_meta = buffer_pool.read_page_metadata(page_id, PageType::Raw)?;

        if page_meta.lsn() >= record.lsn() {
            return Ok(());
        }
    }

    let page_data = buffer_pool.get_page_mut(page_id)?;
    page_data.copy_from_slice(new_data);

    // stamp LSN into page metadata
    let mut meta = PageManager::read_metadata_from_buffer(page_data, PageType::Raw);
    meta.set_lsn(record.lsn());
    PageManager::update_metadata_in_buffer(page_data, &meta);

    buffer_pool.mark_dirty(page_id, record.lsn())?;

    Ok(())
}

pub fn recover_page_link(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Link page WAL record should have a page id",
        )
    })?;
    let mut page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
    let page_data = buffer_pool.get_page_mut(page_id)?;

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    // update page meta
    let next_page = record.next_page().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Link page WAL record should have a next page",
        )
    })?;
    page_meta.set_next_page(next_page);
    page_meta.set_lsn(record.lsn());

    PageManager::update_metadata_in_buffer(page_data, &page_meta);

    buffer_pool.mark_dirty(page_id, record.lsn())?;

    Ok(())
}

pub fn recover_allocate_page(
    page_id: PageId,
    page_type_u8: u8,
    buffer_pool: &mut BufferPool,
) -> io::Result<()> {
    let page_type = PageType::from_u8(page_type_u8)?;

    // reinitialize the page with clean metadata
    let page_data = buffer_pool.get_page_mut(page_id)?;
    *page_data = [0u8; PAGE_SIZE];
    PageManager::init_page_metadata_buffer(page_data, page_type);

    // update free list head — this page was allocated so it should no
    // longer be the free list head. read_next_free gives us what the
    // head should point to after this allocation.
    let next_free = buffer_pool.read_next_free(page_id)?;
    if buffer_pool.first_free_page() == Some(page_id) {
        buffer_pool.set_first_free_page(next_free)?;
    }

    buffer_pool.mark_dirty(page_id, 0)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        constants::OFFSET_RAW_PAGE_START,
        storage::{
            buffer_pool::BufferPool,
            page::{PAGE_SIZE, PageManager, PageType},
        },
        test_helpers::*,
        wal::{reader::WalReader, record_type::WalRecordType, writer::WalWriter},
    };

    fn setup(name: &str) -> (BufferPool, WalWriter) {
        let pm = PageManager::new(name).unwrap();
        let bp = BufferPool::new(pm, 64);
        let wal = WalWriter::new(name).unwrap();
        (bp, wal)
    }

    // --- recover_insert ---

    #[test]
    fn test_recover_insert_applies_row() {
        cleanup("test_rec_insert");
        let (mut bp, mut wal) = setup("test_rec_insert");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let row_bytes = b"hello world";

        wal.append_slotted(
            WalRecordType::Insert,
            "users",
            page_id,
            0,
            row_bytes,
            &[],
            24,
        )
        .unwrap();

        // no flush — simulates crash before checkpoint
        WalReader::new("test_rec_insert")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.slot_count().unwrap(), 1);

        let page = bp.read_page(page_id).unwrap();
        let (offset, len) = PageManager::read_slot(page, 0);
        assert_eq!(len as usize, row_bytes.len());
        assert_eq!(
            &page[offset as usize..offset as usize + len as usize],
            row_bytes
        );

        cleanup("test_rec_insert");
    }

    #[test]
    fn test_recover_insert_skips_if_already_applied() {
        cleanup("test_rec_insert_skip");
        let (mut bp, mut wal) = setup("test_rec_insert_skip");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let row_bytes = b"row data";

        let (lsn, _) = wal
            .append_slotted(
                WalRecordType::Insert,
                "users",
                page_id,
                0,
                row_bytes,
                &[],
                23,
            )
            .unwrap();

        // apply insert to frame and flush — simulates insert + checkpoint
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let row_offset = meta.free_space_end().unwrap() as usize - row_bytes.len();
            page[row_offset..row_offset + row_bytes.len()].copy_from_slice(row_bytes);
            PageManager::write_slot(page, 0, row_offset as u16, row_bytes.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(row_bytes.len());
            meta.set_lsn(lsn);
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // recovery starts after checkpoint — insert already applied
        WalReader::new("test_rec_insert_skip")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // slot count should still be 1 — not applied again
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.slot_count().unwrap(), 1);

        cleanup("test_rec_insert_skip");
    }

    #[test]
    fn test_recover_delete_marks_slot_dead() {
        cleanup("test_rec_delete");
        let (mut bp, mut wal) = setup("test_rec_delete");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        let row_bytes = b"some row data";

        let (insert_lsn, _) = wal
            .append_slotted(
                WalRecordType::Insert,
                "users",
                page_id,
                0,
                row_bytes,
                &[],
                24,
            )
            .unwrap();

        // apply insert to frame
        let row_offset = {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let offset = meta.free_space_end().unwrap() as usize - row_bytes.len();
            page[offset..offset + row_bytes.len()].copy_from_slice(row_bytes);
            PageManager::write_slot(page, 0, offset as u16, row_bytes.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(row_bytes.len());
            meta.set_lsn(insert_lsn);
            PageManager::update_metadata_in_buffer(page, &meta);
            offset
        };

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // log delete — what recovery will replay
        wal.append_slotted(
            WalRecordType::Delete,
            "users",
            page_id,
            0,
            &[],
            row_bytes,
            24,
        )
        .unwrap();

        WalReader::new("test_rec_delete")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        let page = bp.read_page(page_id).unwrap();
        let (offset, len) = PageManager::read_slot(page, 0);
        assert_eq!(len, 0);
        assert_eq!(offset as usize, row_offset);

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert!(meta.lsn() > insert_lsn);

        cleanup("test_rec_delete");
    }

    #[test]
    fn test_recover_delete_skips_if_already_applied() {
        cleanup("test_rec_delete_skip");
        let (mut bp, mut wal) = setup("test_rec_delete_skip");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        let row_bytes = b"some row data";

        let (insert_lsn, _) = wal
            .append_slotted(
                WalRecordType::Insert,
                "users",
                page_id,
                0,
                row_bytes,
                &[],
                654,
            )
            .unwrap();

        // apply insert
        let row_offset = {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            let offset = meta.free_space_end().unwrap() as usize - row_bytes.len();
            page[offset..offset + row_bytes.len()].copy_from_slice(row_bytes);
            PageManager::write_slot(page, 0, offset as u16, row_bytes.len() as u16);
            meta.update_slot_count();
            meta.update_free_space_start();
            meta.update_free_space_end(row_bytes.len());
            meta.set_lsn(insert_lsn);
            PageManager::update_metadata_in_buffer(page, &meta);
            offset
        };

        let (delete_lsn, _) = wal
            .append_slotted(
                WalRecordType::Delete,
                "users",
                page_id,
                0,
                &[],
                row_bytes,
                242,
            )
            .unwrap();

        // apply delete to frame — simulates delete + checkpoint
        {
            let page = bp.get_page_mut(page_id).unwrap();
            PageManager::mark_slot_dead(page, 0);
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            meta.set_lsn(delete_lsn);
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // recovery starts after checkpoint — delete already applied
        WalReader::new("test_rec_delete_skip")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // slot should still be dead and offset preserved
        let page = bp.read_page(page_id).unwrap();
        let (offset, len) = PageManager::read_slot(page, 0);
        assert_eq!(len, 0);
        assert_eq!(offset as usize, row_offset);

        cleanup("test_rec_delete_skip");
    }

    #[test]
    fn test_recover_page_link_restores_next_page() {
        cleanup("test_rec_link");
        let (mut bp, mut wal) = setup("test_rec_link");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (next_page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // log link page — what recovery will replay
        wal.append_link_page(page_id, next_page_id, 543, None)
            .unwrap();

        // verify next_page is None before recovery
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), None);

        WalReader::new("test_rec_link")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), Some(next_page_id));

        cleanup("test_rec_link");
    }

    #[test]
    fn test_recover_page_link_skips_if_already_applied() {
        cleanup("test_rec_link_skip");
        let (mut bp, mut wal) = setup("test_rec_link_skip");

        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let (next_page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        let (lsn, _) = wal
            .append_link_page(page_id, next_page_id, 432, None)
            .unwrap();

        // apply link to frame — simulates link + checkpoint
        {
            let page = bp.get_page_mut(page_id).unwrap();
            let mut meta = PageManager::read_metadata_from_buffer(page, PageType::Slotted);
            meta.set_next_page(next_page_id);
            meta.set_lsn(lsn);
            PageManager::update_metadata_in_buffer(page, &meta);
        }

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // recovery starts after checkpoint — link already applied
        WalReader::new("test_rec_link_skip")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // next_page should still be Some(next_page_id) — not reset
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.next_page().unwrap(), Some(next_page_id));

        cleanup("test_rec_link_skip");
    }

    #[test]
    fn test_replay_raw_page_restores_data() {
        cleanup("test_rec_raw");
        let (mut bp, mut wal) = setup("test_rec_raw");

        let (page_id, _, _) = bp.allocate_raw_page(&mut wal, 1).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let mut new_page = [0u8; PAGE_SIZE];
        new_page[OFFSET_RAW_PAGE_START + 1] = 0xAB;

        let old_page = bp.read_page(page_id).unwrap().clone();
        wal.append_raw(WalRecordType::IndexNode, page_id, &new_page, &old_page, 87)
            .unwrap();

        // no flush — simulates crash
        WalReader::new("test_rec_raw")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        let page = bp.read_page(page_id).unwrap();
        assert_eq!(page[OFFSET_RAW_PAGE_START + 1], 0xAB);

        let meta = bp.read_page_metadata(page_id, PageType::Raw).unwrap();
        assert!(meta.lsn() > 0);

        cleanup("test_rec_raw");
    }

    #[test]
    fn test_replay_raw_page_skips_if_already_applied() {
        cleanup("test_rec_raw_skip");
        let (mut bp, mut wal) = setup("test_rec_raw_skip");

        let (page_id, _, _) = bp.allocate_raw_page(&mut wal, 1).unwrap();

        // build new page with distinctive data
        let mut new_page = [0u8; PAGE_SIZE];
        new_page[OFFSET_RAW_PAGE_START + 1] = 0xBB;

        let old_page = bp.read_page(page_id).unwrap().clone();
        let (lsn, _) = wal
            .append_raw(WalRecordType::IndexNode, page_id, &new_page, &old_page, 234)
            .unwrap();

        // apply raw write to frame — simulates write + checkpoint
        bp.write_raw_page(page_id, &new_page, lsn).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // recovery starts after checkpoint — raw write already applied
        WalReader::new("test_rec_raw_skip")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // data should still be 0xBB — not reset
        let page = bp.read_page(page_id).unwrap();
        assert_eq!(page[OFFSET_RAW_PAGE_START + 1], 0xBB);

        cleanup("test_rec_raw_skip");
    }

    #[test]
    fn test_recover_skips_aborted_txn() {
        cleanup("test_rec_abort_skip");
        let (mut bp, mut wal) = setup("test_rec_abort_skip");

        let _ = bp.allocate_slotted_page(&mut wal, 0).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 0).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let row_bytes = b"should not survive rollback";
        let aborted_txn_id = 99;

        // log an insert under an aborted txn
        wal.append_slotted(
            WalRecordType::Insert,
            "users",
            page_id,
            0,
            row_bytes,
            &[],
            aborted_txn_id,
        )
        .unwrap();

        // log the abort record
        wal.append_abort_txn(aborted_txn_id).unwrap();

        WalReader::new("test_rec_abort_skip")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // slot should not have been applied
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.slot_count().unwrap(), 0);

        cleanup("test_rec_abort_skip");
    }

    #[test]
    fn test_recover_replays_committed_skips_aborted() {
        cleanup("test_rec_mixed_txns");
        let (mut bp, mut wal) = setup("test_rec_mixed_txns");

        let _ = bp.allocate_slotted_page(&mut wal, 0).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal, 0).unwrap();
        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let committed_row = b"committed row";
        let aborted_row = b"aborted row";
        let committed_txn_id = 2;
        let aborted_txn_id = 3;

        // committed txn write
        wal.append_slotted(
            WalRecordType::Insert,
            "users",
            page_id,
            0,
            committed_row,
            &[],
            committed_txn_id,
        )
        .unwrap();

        // aborted txn write
        wal.append_slotted(
            WalRecordType::Insert,
            "users",
            page_id,
            1,
            aborted_row,
            &[],
            aborted_txn_id,
        )
        .unwrap();

        // only txn 2 aborted
        wal.append_abort_txn(aborted_txn_id).unwrap();

        WalReader::new("test_rec_mixed_txns")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // slot 0 (committed) should exist
        let page = bp.read_page(page_id).unwrap();
        let (_, len0) = PageManager::read_slot(page, 0);
        assert_eq!(len0 as usize, committed_row.len());

        // slot 1 (aborted) should not have been applied
        let meta = bp.read_page_metadata(page_id, PageType::Slotted).unwrap();
        assert_eq!(meta.slot_count().unwrap(), 1);

        cleanup("test_rec_mixed_txns");
    }

    #[test]
    fn test_recover_abort_before_write_has_no_effect() {
        cleanup("test_rec_abort_no_writes");
        let (mut bp, mut wal) = setup("test_rec_abort_no_writes");

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // abort a txn that never wrote anything
        wal.append_abort_txn(42).unwrap();

        // should not error
        WalReader::new("test_rec_abort_no_writes")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        cleanup("test_rec_abort_no_writes");
    }

    #[test]
    fn test_recover_returns_correct_last_txn_id_with_abort() {
        cleanup("test_rec_abort_txn_id");
        let (mut bp, mut wal) = setup("test_rec_abort_txn_id");

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        wal.append_slotted(WalRecordType::Insert, "users", page_id, 0, b"row", &[], 5)
            .unwrap();

        wal.append_abort_txn(5).unwrap();

        let last_txn_id = WalReader::new("test_rec_abort_txn_id")
            .unwrap()
            .recover(&mut bp)
            .unwrap();

        // last seen txn id should be 5
        assert_eq!(last_txn_id, 5);

        cleanup("test_rec_abort_txn_id");
    }
}
