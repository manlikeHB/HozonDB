use std::collections::HashMap;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::{fs::File, path::Path};

use crate::constants::PageId;
use crate::wal::{constants::MAGIC_NUMBER, record::WalRecord, record_type::WalRecordType};

const WAL_METADATA_SIZE: usize = 12; // magic: u32 (4 bytes) + checkpoint: u64 (8 bytes)
pub const WAL_RECORD_START: usize = WAL_METADATA_SIZE;
const WAL_CHECKPOINT_OFFSET: usize = 4;

pub struct WalWriter {
    lsn: u64,
    file: File,
    checkpoint: u64,
    txn_last_lsn_and_offsets: HashMap<u64, (u64, u64)>, // txn_id -> (last record lsn, last record offset)
}

impl WalWriter {
    pub fn new(db_name: &str) -> io::Result<Self> {
        let path = format!("{db_name}.wal");
        // open .wal file
        if Path::new(&path).exists() {
            let mut file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            file.seek(SeekFrom::Start(0))?;

            // verify magic
            let mut magic_bytes = [0u8; 4];
            file.read_exact(&mut magic_bytes)?;
            let magic_number = u32::from_le_bytes(magic_bytes);

            if magic_number != MAGIC_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid magic number",
                ));
            }

            // get checkpoint
            let mut checkpoint_bytes = [0u8; 8];
            file.read_exact(&mut checkpoint_bytes)?;
            let checkpoint = u64::from_le_bytes(checkpoint_bytes);

            // seek to last checkpoint offset
            file.seek(SeekFrom::Start(checkpoint))?;

            // get lsn
            let file_size = file.seek(SeekFrom::End(0))?;
            if file_size == WAL_METADATA_SIZE as u64 {
                return Ok(WalWriter {
                    lsn: 1,
                    file,
                    checkpoint,
                    txn_last_lsn_and_offsets: HashMap::new(),
                });
            } else {
                file.seek(SeekFrom::Start(checkpoint))?;
                let mut last_record_offset = checkpoint;
                loop {
                    let current_offset = file.seek(SeekFrom::Current(0))?;

                    let mut len_bytes = [0u8; 4];
                    match file.read_exact(&mut len_bytes) {
                        Ok(_) => {}
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                    let record_len = u32::from_le_bytes(len_bytes);

                    match file.seek(SeekFrom::Current(i64::from(record_len))) {
                        Ok(_) => last_record_offset = current_offset,
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                }

                // seek back and read the last record
                file.seek(SeekFrom::Start(last_record_offset))?;
                let mut len_bytes = [0u8; 4];
                file.read_exact(&mut len_bytes)?;
                let record_len = u32::from_le_bytes(len_bytes) as usize;
                let mut record_bytes = vec![0u8; record_len];
                file.read_exact(&mut record_bytes)?;
                let last_record = WalRecord::from_bytes(&record_bytes)?;
                let lsn = last_record.lsn() + 1; // next lsn

                file.seek(SeekFrom::End(0))?; // position cursor for appending
                Ok(WalWriter {
                    lsn,
                    file,
                    checkpoint,
                    txn_last_lsn_and_offsets: HashMap::new(),
                })
            }
        } else {
            let mut file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            let mut wal_page_meta_bytes = [0u8; WAL_METADATA_SIZE];
            wal_page_meta_bytes[0..4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes()); // add magic
            wal_page_meta_bytes[4..12].copy_from_slice(&(WAL_RECORD_START as u64).to_le_bytes()); // set start as checkpoint

            file.write_all(&wal_page_meta_bytes)?;
            file.sync_all()?;

            file.seek(SeekFrom::End(0))?; // position cursor for appending
            Ok(WalWriter {
                lsn: 1,
                file,
                checkpoint: WAL_RECORD_START as u64,
                txn_last_lsn_and_offsets: HashMap::new(),
            })
        }
    }

    /// Append a Slotted WAL record
    /// returns the records LSN and WAL offset
    pub fn append_slotted(
        &mut self,
        record_type: WalRecordType,
        table_name: &str,
        page_id: PageId,
        slot: u16,
        new_data: &[u8],
        old_data: &[u8],
        txn_id: u64,
    ) -> io::Result<(u64, u64)> {
        // read current lsn and increment
        let lsn = self.lsn;
        self.lsn += 1;

        // create new record
        let mut record = WalRecord::new_slotted(
            lsn,
            record_type,
            table_name,
            page_id,
            slot,
            new_data,
            old_data,
            txn_id,
        );

        let offset = self.append(&mut record)?;
        Ok((lsn, offset))
    }

    /// Append a Raw WAL record
    /// returns the records LSN and WAL offset
    pub fn append_raw(
        &mut self,
        record_type: WalRecordType,
        page_id: PageId,
        new_data: &[u8],
        old_data: &[u8],
        txn_id: u64,
    ) -> io::Result<(u64, u64)> {
        // read current lsn and increment
        let lsn = self.lsn;
        self.lsn += 1;

        // create new record
        let mut record = WalRecord::new_raw(lsn, record_type, page_id, new_data, old_data, txn_id);

        let offset = self.append(&mut record)?;
        Ok((lsn, offset))
    }

    /// Append a Checkpoint WAL record
    /// returns the records LSN and WAL offset
    fn append_checkpoint(&mut self) -> io::Result<u64> {
        // read current lsn and increment
        let lsn = self.lsn;
        self.lsn += 1;

        // create new record
        let mut record = WalRecord::new_checkpoint(lsn);

        self.append(&mut record)?;
        Ok(lsn)
    }

    /// Append a Link page WAL record
    /// returns the records LSN and WAL offset
    pub fn append_link_page(
        &mut self,
        page_id: PageId,
        next_page: PageId,
        txn_id: u64,
    ) -> io::Result<(u64, u64)> {
        // read current lsn and increment
        let lsn = self.lsn;
        self.lsn += 1;

        // create new record
        let mut record = WalRecord::new_link_page(lsn, page_id, next_page, txn_id);

        let offset = self.append(&mut record)?;
        Ok((lsn, offset))
    }

    /// Append a Allocate page WAL record
    /// returns the records LSN and WAL offset
    pub fn append_allocate_page(
        &mut self,
        page_id: PageId,
        page_type: u8,
        txn_id: u64,
    ) -> io::Result<(u64, u64)> {
        let lsn = self.lsn;
        let mut record = WalRecord::new_allocate_page(lsn, page_id, page_type, txn_id);
        let offset = self.append(&mut record)?;
        self.lsn += 1;
        Ok((lsn, offset))
    }

    /// Append a abort transaction WAL record
    /// returns the LSN
    pub fn append_abort_txn(&mut self, txn_id: u64) -> io::Result<u64> {
        let lsn = self.lsn;
        let mut record = WalRecord::new_abort(lsn, txn_id);
        self.append(&mut record)?;
        self.lsn += 1;
        Ok(lsn)
    }

    // WAL is synced to disk at commit time (group commit) for explicit transactions.
    // Implicit single-statement transactions sync immediately after each operation.
    fn append(&mut self, record: &mut WalRecord) -> io::Result<u64> {
        // offset for current record ( new prev offset)
        let record_offset = self.file.seek(SeekFrom::Current(0))?;

        // set prev lsn and prev offset in record
        if let Some(txn_id) = record.txn_id() {
            if let Some((pl, po)) = self.txn_last_lsn_and_offsets.get(&txn_id) {
                record.set_prev_link(*pl, *po);
            }
        }

        let record_byte = record.to_bytes();

        // append new record (record len + record)
        let mut record_data: Vec<u8> = Vec::new();
        record_data.extend_from_slice(&(record_byte.len() as u32).to_le_bytes());
        record_data.extend_from_slice(&record_byte);

        self.file.write_all(&record_data)?;

        // update prev offset
        if let Some(txn_id) = record.txn_id() {
            self.txn_last_lsn_and_offsets
                .insert(txn_id, (record.lsn(), record_offset));
        }

        Ok(record_offset)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn checkpoint(&mut self) -> io::Result<u64> {
        // get page size
        let checkpoint = self.file.seek(SeekFrom::End(0))?;

        // append checkpoint record
        self.append_checkpoint()?;

        // update file header
        self.file
            .seek(SeekFrom::Start(WAL_CHECKPOINT_OFFSET as u64))?;
        self.file.write_all(&checkpoint.to_le_bytes())?;
        self.file.sync_all()?;

        self.checkpoint = checkpoint;

        self.file.seek(SeekFrom::End(0))?; // position cursor for appending
        Ok(checkpoint)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_open_new_wal_writer() {
        let _ = fs::remove_file("mydb.wal");

        let wal = WalWriter::new("mydb").unwrap();

        assert_eq!(wal.lsn, 1);
        assert_eq!(wal.checkpoint, WAL_RECORD_START as u64);

        let _ = fs::remove_file("mydb.wal");
    }

    #[test]
    fn test_append_single_record() {
        let _ = fs::remove_file("test_append.wal");
        let mut wal = WalWriter::new("test_append").unwrap();
        let (lsn, _) = wal
            .append_slotted(WalRecordType::Insert, "users", 1, 0, b"data", &[], 234)
            .unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(wal.lsn, 2);
        let _ = fs::remove_file("test_append.wal");
    }

    #[test]
    fn test_lsn_derived_from_existing_file() {
        let _ = fs::remove_file("test_lsn.wal");
        {
            let mut wal = WalWriter::new("test_lsn").unwrap();
            wal.append_slotted(WalRecordType::Insert, "users", 1, 0, b"data", &[], 6543)
                .unwrap();
            wal.append_slotted(WalRecordType::Insert, "users", 1, 1, b"data", &[], 652)
                .unwrap();
        }
        // reopen — should derive lsn = 3
        let wal = WalWriter::new("test_lsn").unwrap();
        assert_eq!(wal.lsn, 3);
        let _ = fs::remove_file("test_lsn.wal");
    }

    #[test]
    fn test_checkpoint_persists() {
        let _ = fs::remove_file("test_checkpoint.wal");
        let wal = WalWriter::new("test_checkpoint").unwrap();
        assert_eq!(wal.checkpoint, WAL_RECORD_START as u64);
        drop(wal);
        let wal = WalWriter::new("test_checkpoint").unwrap();
        assert_eq!(wal.checkpoint, WAL_RECORD_START as u64);
        let _ = fs::remove_file("test_checkpoint.wal");
    }

    #[test]
    fn test_checkpoint_updates_header() {
        let _ = fs::remove_file("test_cp.wal");
        let mut wal = WalWriter::new("test_cp").unwrap();
        wal.append_slotted(WalRecordType::Insert, "users", 1, 0, b"data", &[], 234)
            .unwrap();
        let cp = wal.checkpoint().unwrap(); // checkpoint
        assert!(cp > WAL_RECORD_START as u64);
        assert_eq!(wal.checkpoint, cp);
        // reopen and verify header
        let wal2 = WalWriter::new("test_cp").unwrap();
        assert_eq!(wal2.checkpoint, cp);
        let _ = fs::remove_file("test_cp.wal");
    }

    #[test]
    fn test_wal_persists_records_across_reopen() {
        let _ = fs::remove_file("test_persist.wal");

        {
            let mut wal = WalWriter::new("test_persist").unwrap();
            let (lsn, _) = wal
                .append_slotted(WalRecordType::Insert, "users", 1, 0, b"alice", &[], 2345)
                .unwrap();
            assert_eq!(lsn, 1);

            let (lsn, _) = wal
                .append_slotted(WalRecordType::Insert, "users", 1, 1, b"bob", &[], 2345)
                .unwrap();
            assert_eq!(lsn, 2);

            let (lsn, _) = wal
                .append_slotted(
                    WalRecordType::Update,
                    "users",
                    1,
                    0,
                    b"alice2",
                    b"alice",
                    7654,
                )
                .unwrap();
            assert_eq!(lsn, 3);

            assert_eq!(wal.lsn, 4);

            // dropped here — simulates clean shutdown, no checkpoint
        }

        {
            // reopen — simulates restart
            let mut wal = WalWriter::new("test_persist").unwrap();

            // LSN should continue from where we left off
            assert_eq!(wal.lsn, 4);

            // checkpoint should be unchanged
            assert_eq!(wal.checkpoint, WAL_RECORD_START as u64);

            // should still be able to append
            let (lsn, _) = wal
                .append_slotted(WalRecordType::Delete, "users", 1, 1, &[], b"bob", 52)
                .unwrap();
            assert_eq!(lsn, 4);

            // checkpoint
            let cp = wal.checkpoint().unwrap();
            assert!(cp > WAL_RECORD_START as u64);
            assert_eq!(wal.checkpoint, cp);
        }

        {
            let wal = WalWriter::new("test_persist").unwrap();

            // verify checkpoint persist
            assert!(wal.checkpoint > WAL_RECORD_START as u64);
        }

        let _ = fs::remove_file("test_persist.wal");
    }

    #[test]
    fn test_append_raw_record() {
        let _ = fs::remove_file("test_append_raw.wal");
        let mut wal = WalWriter::new("test_append_raw").unwrap();

        let (lsn, _) = wal
            .append_raw(
                WalRecordType::CreateTable,
                1,
                b"new_page_data",
                b"old_page_data",
                212,
            )
            .unwrap();

        assert_eq!(lsn, 1);
        assert_eq!(wal.lsn, 2);

        let _ = fs::remove_file("test_append_raw.wal");
    }

    #[test]
    fn test_lsn_increments_across_slotted_and_raw() {
        let _ = fs::remove_file("test_lsn_mixed.wal");
        let mut wal = WalWriter::new("test_lsn_mixed").unwrap();

        let (lsn1, _) = wal
            .append_slotted(WalRecordType::Insert, "users", 1, 0, b"row", &[], 654)
            .unwrap();
        let (lsn2, _) = wal
            .append_raw(WalRecordType::IndexNode, 2, b"node", b"old", 86)
            .unwrap();
        let (lsn3, _) = wal
            .append_slotted(WalRecordType::Delete, "users", 1, 0, &[], b"row", 124)
            .unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
        assert_eq!(wal.lsn, 4);

        let _ = fs::remove_file("test_lsn_mixed.wal");
    }

    #[test]
    fn test_multiple_checkpoints() {
        let _ = fs::remove_file("test_multi_cp.wal");
        let mut wal = WalWriter::new("test_multi_cp").unwrap();

        wal.append_slotted(WalRecordType::Insert, "t", 1, 0, b"a", &[], 876)
            .unwrap();
        let cp1 = wal.checkpoint().unwrap();

        wal.append_slotted(WalRecordType::Insert, "t", 1, 1, b"b", &[], 2345)
            .unwrap();
        let cp2 = wal.checkpoint().unwrap();

        // second checkpoint should be ahead of first
        assert!(cp2 > cp1);
        assert_eq!(wal.checkpoint, cp2);

        // reopen — should use latest checkpoint
        drop(wal);
        let wal = WalWriter::new("test_multi_cp").unwrap();
        assert_eq!(wal.checkpoint, cp2);

        let _ = fs::remove_file("test_multi_cp.wal");
    }

    #[test]
    fn test_checkpoint_with_no_new_records() {
        let _ = fs::remove_file("test_cp_empty.wal");
        let mut wal = WalWriter::new("test_cp_empty").unwrap();

        // checkpoint immediately with no records
        let cp = wal.checkpoint().unwrap();
        assert!(cp >= WAL_RECORD_START as u64);

        let _ = fs::remove_file("test_cp_empty.wal");
    }

    #[test]
    fn test_append_raw_persists_across_reopen() {
        let _ = fs::remove_file("test_raw_persist.wal");

        {
            let mut wal = WalWriter::new("test_raw_persist").unwrap();
            wal.append_raw(WalRecordType::IndexNode, 5, b"new", b"old", 8234)
                .unwrap();
            wal.append_raw(WalRecordType::CreateTable, 1, b"catalog", &[], 6543)
                .unwrap();
        }

        {
            let wal = WalWriter::new("test_raw_persist").unwrap();
            assert_eq!(wal.lsn, 3); // 2 records written, next lsn = 3
        }

        let _ = fs::remove_file("test_raw_persist.wal");
    }

    #[test]
    fn test_append_link_page() {
        let _ = fs::remove_file("test_link_page.wal");
        let mut wal = WalWriter::new("test_link_page").unwrap();

        let (lsn, _) = wal.append_link_page(3, 7, 234).unwrap(); // page_id=3, next_page=7
        assert_eq!(lsn, 1);
        assert_eq!(wal.lsn, 2);

        let _ = fs::remove_file("test_link_page.wal");
    }

    #[test]
    fn test_append_link_page_lsn_increments_with_other_records() {
        let _ = fs::remove_file("test_link_lsn.wal");
        let mut wal = WalWriter::new("test_link_lsn").unwrap();

        let (lsn1, _) = wal
            .append_slotted(WalRecordType::Insert, "users", 3, 0, b"row", &[], 234)
            .unwrap();
        let (lsn2, _) = wal.append_link_page(3, 4, 6543).unwrap();
        let (lsn3, _) = wal
            .append_slotted(WalRecordType::Insert, "users", 4, 0, b"row2", &[], 6543)
            .unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        let _ = fs::remove_file("test_link_lsn.wal");
    }

    #[test]
    fn test_append_link_page_persists_across_reopen() {
        let _ = fs::remove_file("test_link_persist.wal");

        {
            let mut wal = WalWriter::new("test_link_persist").unwrap();
            wal.append_link_page(3, 4, 1234).unwrap();
            wal.append_link_page(4, 5, 5421).unwrap();
        }

        {
            let wal = WalWriter::new("test_link_persist").unwrap();
            assert_eq!(wal.lsn, 3);
        }

        let _ = fs::remove_file("test_link_persist.wal");
    }
}
