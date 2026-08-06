use crate::{
    constants::PageId,
    storage::buffer_pool::BufferPool,
    wal::{
        constants::MAGIC_NUMBER,
        reader::{recover::*, undo::*},
        record::WalRecord,
        record_type::WalRecordType,
    },
};
use std::{
    collections::HashSet,
    fs::File,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};

mod recover;
mod undo;

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

    pub fn recover(&mut self, buffer_pool: &mut BufferPool) -> io::Result<u64> {
        let mut last_txn_id = 0;

        // collect all wal records and aborted txn ids
        let mut records = Vec::new();
        let mut aborted_txns = HashSet::new();

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
                WalRecord::Abort { txn_id, .. } => {
                    aborted_txns.insert(txn_id);
                }
                _ => {}
            }

            if let Some(txn_id) = record.txn_id() {
                last_txn_id = txn_id
            }

            records.push(record);
        }

        // walk the records and re apply changes
        for record in records {
            // skip records with Aborted txn_id
            if let Some(txn_id) = record.txn_id() {
                if aborted_txns.contains(&txn_id) {
                    continue;
                }
            }

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
                WalRecord::LinkPage { .. } => {
                    last_txn_id = record.txn_id().ok_or_else(|| {
                        io::Error::new(
                            ErrorKind::InvalidData,
                            "LinkPage WAL record type should contain a tnx_id",
                        )
                    })?;
                    recover_page_link(&record, buffer_pool)?
                }
                WalRecord::AllocatePage {
                    page_id,
                    page_type,
                    lsn,
                    ..
                } => {
                    recover_allocate_page(page_id, page_type, buffer_pool, lsn)?;
                }
                WalRecord::Abort { txn_id, .. } => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!("Abort Wal record type with txn_id:{} not skipped", txn_id),
                    ));
                }
            }
        }

        // flush all replayed changes to disk
        buffer_pool.flush_dirty()?;
        Ok(last_txn_id)
    }

    pub fn undo_record_at(
        &mut self,
        expected_lsn: u64,
        wal_offset: u64,
        buffer_pool: &mut BufferPool,
        abort_lsn: u64,
    ) -> io::Result<Option<PageId>> {
        let record = self.read_record_at(wal_offset)?;

        // Validate record through it's LSN against expected LSN
        if record.lsn() != expected_lsn {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Record lsn doesn't match expected lsn",
            ));
        }
        Self::undo_record(&record, buffer_pool, abort_lsn)?;

        Ok(record.page_id())
    }

    /// Reads a record out of the WAL log at a specific offset
    fn read_record_at(&mut self, wal_offset: u64) -> io::Result<WalRecord> {
        // seek to the offset
        self.file.seek(SeekFrom::Start(wal_offset))?;
        // read wal record
        let mut record_len_bytes = [0u8; 4];
        self.file.read_exact(&mut record_len_bytes)?;
        let record_len = u32::from_le_bytes(record_len_bytes);

        let mut record_bytes = vec![0u8; record_len as usize];
        self.file.read_exact(&mut record_bytes)?;
        WalRecord::from_bytes(&record_bytes)
    }

    pub fn undo_record(
        record: &WalRecord,
        buffer_pool: &mut BufferPool,
        abort_lsn: u64,
    ) -> io::Result<()> {
        match record {
            WalRecord::AllocatePage { .. } => undo_allocate_page(record, buffer_pool, abort_lsn),
            WalRecord::LinkPage { .. } => undo_link_page(&record, buffer_pool, abort_lsn),

            WalRecord::Slotted { record_type, .. } => match record_type {
                WalRecordType::Insert => undo_insert(record, buffer_pool, abort_lsn),
                WalRecordType::Update => undo_update(record, buffer_pool, abort_lsn),
                WalRecordType::Delete => undo_delete(record, buffer_pool, abort_lsn),
                other => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "no undo implemented for Slotted WAL record type {:?}",
                        other
                    ),
                )),
            },

            WalRecord::Raw { record_type, .. } => match record_type {
                WalRecordType::FreePage => undo_free_page(record, buffer_pool, abort_lsn),
                _ => undo_raw(&record, buffer_pool, abort_lsn),
            },

            WalRecord::Checkpoint { .. } | WalRecord::Abort { .. } => Err(Error::new(
                ErrorKind::InvalidData,
                "attempted to undo a Checkpoint/Abort record — these should never appear in a txn's undo chain",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{PageManager, PageType};
    use crate::test_helpers::cleanup;
    use crate::wal::writer::WalWriter;

    fn setup(name: &str) -> (BufferPool, WalWriter) {
        let pm = PageManager::new(name).unwrap();
        let bp = BufferPool::new(pm, 64).unwrap();
        let wal = WalWriter::new(name).unwrap();
        (bp, wal)
    }

    #[test]
    fn test_undo_record_at_undoes_insert() {
        cleanup("test_undo_record_at");
        let (mut bp, mut wal) = setup("test_undo_record_at");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();
        let row_bytes = b"a row";

        let (insert_lsn, insert_offset) = wal
            .append_slotted(
                WalRecordType::Insert,
                "users",
                page_id,
                0,
                row_bytes,
                &[],
                1,
            )
            .unwrap();
        {
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
        }

        let mut reader = WalReader::new("test_undo_record_at").unwrap();
        reader
            .undo_record_at(insert_lsn, insert_offset, &mut bp, 999)
            .unwrap();

        let page = bp.read_page(page_id).unwrap();
        let (_, len) = PageManager::read_slot(page, 0);
        assert_eq!(len, 0, "slot should be marked dead after undo_record_at");

        cleanup("test_undo_record_at");
    }

    #[test]
    fn test_undo_record_at_detects_lsn_mismatch() {
        cleanup("test_undo_record_at_mismatch");
        let (mut bp, mut wal) = setup("test_undo_record_at_mismatch");

        let (page_id, _, _) = bp.allocate_slotted_page(&mut wal, 1).unwrap();

        let (_lsn_a, offset_a) = wal
            .append_slotted(WalRecordType::Insert, "users", page_id, 0, b"row a", &[], 1)
            .unwrap();
        let (lsn_b, _offset_b) = wal
            .append_slotted(WalRecordType::Insert, "users", page_id, 1, b"row b", &[], 1)
            .unwrap();

        let mut reader = WalReader::new("test_undo_record_at_mismatch").unwrap();

        // record B's lsn paired with record A's offset — should be rejected
        let result = reader.undo_record_at(lsn_b, offset_a, &mut bp, 999);
        assert!(result.is_err());

        cleanup("test_undo_record_at_mismatch");
    }
}
