use crate::{
    storage::page::PageManager,
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

    pub fn recover(mut self, _pm: &mut PageManager) -> io::Result<()> {
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

            match record.record_type() {
                WalRecordType::Checkpoint => {}
                WalRecordType::CreateIndex => {}
                WalRecordType::CreateTable => {}
                WalRecordType::Delete => {}
                WalRecordType::DropIndex => {}
                WalRecordType::DropTable => {}
                WalRecordType::Insert => {}
                WalRecordType::Update => {}
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
