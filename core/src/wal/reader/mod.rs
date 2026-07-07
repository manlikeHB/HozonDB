use crate::{
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
    ) -> io::Result<()> {
        let record = self.read_record_at(wal_offset)?;

        // Validate record through it's LSN against expected LSN
        if record.lsn() == expected_lsn {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Record lsn doesn't match expected lsn",
            ));
        }
        Self::undo_record(record, buffer_pool, abort_lsn)
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
        record: WalRecord,
        buffer_pool: &mut BufferPool,
        abort_lsn: u64,
    ) -> io::Result<()> {
        match record {
            WalRecord::AllocatePage { .. } => {
                todo!()
            }
            WalRecord::LinkPage { .. } => undo_link_page(&record, buffer_pool, abort_lsn),
            WalRecord::Raw { .. } => undo_raw(&record, buffer_pool, abort_lsn),
            WalRecord::Slotted { record_type, .. } => {
                match record_type {
                    WalRecordType::Insert => undo_insert(&record, buffer_pool, abort_lsn),
                    WalRecordType::Update => undo_update(&record, buffer_pool, abort_lsn),
                    WalRecordType::Delete => undo_delete(&record, buffer_pool, abort_lsn),
                    _ => {
                        todo!()
                    } // other slotted types don't need undo
                }
            }
            _ => {
                todo!()
            } // TODO: throw error?
        }
    }
}
