use crate::{
    constants::PageId,
    storage::{
        buffer_pool::BufferPool,
        page::{PAGE_SIZE, PageManager, PageMetadata, PageType},
    },
    wal::{constants::MAGIC_NUMBER, record::WalRecord, record_type::WalRecordType},
};
use std::{
    fs::File,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};

pub struct WalReader {
    file: File,
}

impl WalReader {
    pub fn new(db_name: &str) -> io::Result<Self> {
        let path = format!("{db_name}.wal");

        if Path::new(&path).exists() {
            let mut file = File::options().read(true).open(path)?;

            file.seek(SeekFrom::Start(0))?;

            // verify Magic
            let mut magic_bytes = [0u8; 4];
            file.read_exact(&mut magic_bytes)?;
            let magic = u32::from_le_bytes(magic_bytes);

            if magic != MAGIC_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid magic number",
                ));
            }

            // get checkpoint
            let mut checkpoint_bytes = [0u8; 8];
            file.read_exact(&mut checkpoint_bytes)?;
            let checkpoint = u64::from_le_bytes(checkpoint_bytes);

            // set cursor at checkpoint
            file.seek(SeekFrom::Start(checkpoint))?;

            return Ok(WalReader { file });
        } else {
            return Err(Error::new(
                io::ErrorKind::InvalidData,
                "Every database should have a `.wal` file",
            ));
        }
    }

    pub fn recover(mut self, buffer_pool: &mut BufferPool) -> io::Result<()> {
        // walk the records and re apply changes
        loop {
            let mut record_len_bytes = [0u8; 4];
            match self.file.read_exact(&mut record_len_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let record_len = u32::from_le_bytes(record_len_bytes);

            let mut record_bytes = vec![0u8; record_len as usize];
            match self.file.read_exact(&mut record_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let record = WalRecord::from_bytes(&record_bytes)?;

            match record {
                WalRecord::Checkpoint { .. } => {}
                WalRecord::Slotted { record_type, .. } => {
                    match record_type {
                        WalRecordType::Insert => recover_insert(&record, buffer_pool)?,
                        WalRecordType::Update => recover_update(&record, buffer_pool)?,
                        WalRecordType::Delete => recover_delete(&record, buffer_pool)?,
                        _ => {} // other slotted types don't need recovery
                    }
                }
                WalRecord::Raw { .. } => replay_raw_page_new_data(&record, buffer_pool)?,
                WalRecord::LinkPage { .. } => recover_page_link(&record, buffer_pool)?,
                WalRecord::AllocatePage {
                    page_id, page_type, ..
                } => {
                    recover_allocate_page(page_id, page_type, buffer_pool)?;
                }
            }
        }

        // flush all replayed changes to disk
        buffer_pool.flush_dirty()?;
        Ok(())
    }
}

fn recover_insert(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
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

fn recover_update(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
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

fn recover_delete(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
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

fn replay_raw_page_new_data(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
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

fn recover_page_link(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
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

fn recover_allocate_page(
    page_id: PageId,
    page_type_u8: u8,
    buffer_pool: &mut BufferPool,
) -> io::Result<()> {
    let page_type = PageType::from_u8(page_type_u8)?;

    // reinitialize the page with clean metadata
    let page_data = buffer_pool.get_page_mut(page_id)?;
    *page_data = [0u8; PAGE_SIZE];
    PageManager::init_page_metadata_buffer(page_data, page_type);

    buffer_pool.mark_dirty(page_id, 0)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::OFFSET_RAW_PAGE_START,
        storage::{
            buffer_pool::BufferPool,
            page::{PAGE_SIZE, PageManager, PageType},
        },
        test_helpers::*,
        wal::{record_type::WalRecordType, writer::WalWriter},
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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let row_bytes = b"hello world";

        wal.append_slotted(WalRecordType::Insert, "users", page_id, 0, row_bytes, &[])
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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();
        let row_bytes = b"row data";

        let lsn = wal
            .append_slotted(WalRecordType::Insert, "users", page_id, 0, row_bytes, &[])
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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        let row_bytes = b"some row data";

        let insert_lsn = wal
            .append_slotted(WalRecordType::Insert, "users", page_id, 0, row_bytes, &[])
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
        wal.append_slotted(WalRecordType::Delete, "users", page_id, 0, &[], row_bytes)
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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        let row_bytes = b"some row data";

        let insert_lsn = wal
            .append_slotted(WalRecordType::Insert, "users", page_id, 0, row_bytes, &[])
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

        let delete_lsn = wal
            .append_slotted(WalRecordType::Delete, "users", page_id, 0, &[], row_bytes)
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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();
        let next_page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        // log link page — what recovery will replay
        wal.append_link_page(page_id, next_page_id).unwrap();

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

        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let _ = bp.allocate_slotted_page(&mut wal).unwrap();
        let page_id = bp.allocate_slotted_page(&mut wal).unwrap();
        let next_page_id = bp.allocate_slotted_page(&mut wal).unwrap();

        let lsn = wal.append_link_page(page_id, next_page_id).unwrap();

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

        let page_id = bp.allocate_raw_page(&mut wal).unwrap();

        bp.flush_dirty().unwrap();
        wal.checkpoint().unwrap();

        let mut new_page = [0u8; PAGE_SIZE];
        new_page[OFFSET_RAW_PAGE_START + 1] = 0xAB;

        let old_page = bp.read_page(page_id).unwrap().clone();
        wal.append_raw(WalRecordType::IndexNode, page_id, &new_page, &old_page)
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

        let page_id = bp.allocate_raw_page(&mut wal).unwrap();

        // build new page with distinctive data
        let mut new_page = [0u8; PAGE_SIZE];
        new_page[OFFSET_RAW_PAGE_START + 1] = 0xBB;

        let old_page = bp.read_page(page_id).unwrap().clone();
        let lsn = wal
            .append_raw(WalRecordType::IndexNode, page_id, &new_page, &old_page)
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
}
