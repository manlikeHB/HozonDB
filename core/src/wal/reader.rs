use crate::{
    storage::{
        buffer_pool::BufferPool,
        page::{PageManager, PageMetadata, PageType},
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

            if let Some(record_type) = record.record_type() {
                match record_type {
                    WalRecordType::Checkpoint => {}
                    WalRecordType::CreateIndex => {}
                    WalRecordType::CreateTable => {}
                    WalRecordType::Delete => {}
                    WalRecordType::DropIndex => {}
                    WalRecordType::DropTable => {}
                    WalRecordType::Insert => {}
                    WalRecordType::Update => {}
                    WalRecordType::AddIndex => {}
                    WalRecordType::RemoveIndex => {}
                    WalRecordType::UpdateLastPage => {}
                    WalRecordType::RemoveTableIndex => {}
                    WalRecordType::CreateBPlusTree => {}
                    WalRecordType::DeleteKey => {}
                    WalRecordType::FreePage => {}
                    WalRecordType::IndexNode => {}
                    WalRecordType::IndexRoot => {}
                }
            }
        }

        // TODO: Recovery is incomplete until the buffer pool is implemented.
        //
        // Two gaps block correct recovery:
        // 1. WAL only covers row pages — index and catalog page writes bypass
        //    WAL entirely, so recovery would leave indexes inconsistent.
        // 2. WAL records carry row bytes but not full page state (slot entries,
        //    metadata) needed to reconstruct a page write without executor logic.
        //
        // The buffer pool will intercept every write_page() call, log it to WAL

        todo!()
    }
}

// TODO: should reader update lsn as it recover changes?

fn recover_insert(record: &WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Insert WAL record should have a page id",
        )
    })?;
    let page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
    let page_data = buffer_pool.get_page_mut(page_id)?;

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    if let PageMetadata::Slotted {
        slot_count,
        free_space_end,
        lsn,
        ..
    } = page_meta
    {
        // TODO: page metadata slot should match slot count
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

        // update lsn

        Ok(())
    } else {
        Err(io::Error::new(
            ErrorKind::InvalidData,
            "Expected slotted page metadata",
        ))
    }
}

fn recover_delete(record: WalRecord, buffer_pool: &mut BufferPool) -> io::Result<()> {
    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "Delete WAL record should have a page id",
        )
    })?;
    let page_meta = buffer_pool.read_page_metadata(page_id, PageType::Slotted)?;
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

    Ok(())
}

// fn recover_add_index(record: &WalRecord, pm: &mut PageManager) -> io::Result<()> {
//     replay_raw_page_new_data(pm, record)?;
//     Ok(())
// }

// fn recover_delete_index(record: &WalRecord, pm: &mut PageManager) -> io::Result<()> {
//     replay_raw_page_new_data(pm, record)?;
//     Ok(())
// }

// fn recover_delete_index(record: &WalRecord, pm: &mut PageManager) -> io::Result<()> {
//     replay_raw_page_new_data(pm, record)?;
//     Ok(())
// }

// fn recover_remove_table_index
// create table
// drop table

fn replay_raw_page_new_data(pm: &mut PageManager, record: &WalRecord) -> io::Result<()> {
    let record_type = record.record_type().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "A wal record should have a record type",
        )
    })?;

    let page_id = record.page_id().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("{:?} WAL record should have a page id", record_type),
        )
    })?;

    let page_data = pm.read_page(page_id)?;
    let page_meta = PageManager::read_metadata_from_buffer(&page_data, PageType::Raw);

    // compare lsn
    if page_meta.lsn() >= record.lsn() {
        // changes must have been applied
        return Ok(());
    }

    let new_data = record.new_data().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("{:?} WAL record should have new data", record_type),
        )
    })?;

    // replay new data
    pm.write_page(page_id, new_data)?;
    Ok(())
}
