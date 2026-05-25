use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::{fs::File, path::Path};

use crate::constants::PageId;
use crate::wal::record::WalRecord;
use crate::wal::record_type::WalRecordType;

const MAGIC_NUMBER: u32 = 0x4857414C; // HWAL
const WAL_METADATA_SIZE: usize = 12; // magic: u32 (4 bytes) + checkpoint: u64 (8 bytes)
pub const WAL_RECORD_START: usize = WAL_METADATA_SIZE;
const WAL_CHECKPOINT_OFFSET: usize = 4;

pub struct WalWriter {
    lsn: u64,
    file: File,
    checkpoint: u64,
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
            })
        }
    }

    pub fn append(
        &mut self,
        record_type: WalRecordType,
        table_name: &str,
        page_id: PageId,
        slot: u16,
        new_data: &[u8],
        old_data: &[u8],
    ) -> io::Result<u64> {
        // read current lsn and increment
        let lsn = self.lsn;
        self.lsn += 1;

        // create new record
        let record = WalRecord::new(
            lsn,
            record_type,
            table_name,
            page_id,
            slot,
            new_data,
            old_data,
        );
        let record_byte = record.to_bytes();

        // append new record (record len + record)
        let mut record_data: Vec<u8> = Vec::new();
        record_data.extend_from_slice(&(record_byte.len() as u32).to_le_bytes());
        record_data.extend_from_slice(&record_byte);

        self.file.write_all(&record_data)?;
        self.file.sync_all()?;

        Ok(lsn)
    }

    pub fn checkpoint(&mut self) -> io::Result<u64> {
        // get page size
        let checkpoint = self.file.seek(SeekFrom::End(0))?;

        // append checkpoint record
        self.append(WalRecordType::Checkpoint, "", 0, 0, &[], &[])?;

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
        let lsn = wal
            .append(WalRecordType::Insert, "users", 1, 0, b"data", &[])
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
            wal.append(WalRecordType::Insert, "users", 1, 0, b"data", &[])
                .unwrap();
            wal.append(WalRecordType::Insert, "users", 1, 1, b"data", &[])
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
        wal.append(WalRecordType::Insert, "users", 1, 0, b"data", &[])
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
            let lsn = wal
                .append(WalRecordType::Insert, "users", 1, 0, b"alice", &[])
                .unwrap();
            assert_eq!(lsn, 1);

            let lsn = wal
                .append(WalRecordType::Insert, "users", 1, 1, b"bob", &[])
                .unwrap();
            assert_eq!(lsn, 2);

            let lsn = wal
                .append(WalRecordType::Update, "users", 1, 0, b"alice2", b"alice")
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
            let lsn = wal
                .append(WalRecordType::Delete, "users", 1, 1, &[], b"bob")
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
}
