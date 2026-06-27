use std::io::{self, ErrorKind};

use crate::{
    storage::{
        buffer_pool::BufferPool,
        page::{PageManager, PageType},
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
    todo!()
}

pub fn undo_free_page(
    record: &WalRecord,
    buffer_pool: &mut BufferPool,
    abort_lsn: u64,
) -> io::Result<()> {
    todo!()
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

    let old_next_page = record.old_next_page().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Link Page WAL record should have a old next page",
        )
    })?;

    let page_data = buffer_pool.get_page_mut(page_id)?;
    let mut meta = PageManager::read_metadata_from_buffer(page_data, PageType::Slotted);

    // update next page to point to previous next_page
    meta.set_next_page(old_next_page);
    PageManager::update_metadata_in_buffer(page_data, &meta);

    buffer_pool.mark_dirty(page_id, abort_lsn)?;

    Ok(())
}
