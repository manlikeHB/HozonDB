use std::io::{Error, ErrorKind};

use crate::{constants::PageId, wal::record_type::WalRecordType};

#[derive(Debug, PartialEq, Eq)]
pub struct WalRecord {
    lsn: u64,
    record_type: WalRecordType,
    table_name: String,
    page_id: PageId,
    slot: u16,
    new_data: Vec<u8>,
    old_data: Vec<u8>,
}

impl WalRecord {
    pub fn new(
        lsn: u64,
        record_type: WalRecordType,
        table_name: &str,
        page_id: PageId,
        slot: u16,
        new_data: &[u8],
        old_data: &[u8],
    ) -> Self {
        WalRecord {
            lsn,
            record_type,
            table_name: table_name.to_string(),
            page_id,
            slot,
            new_data: new_data.to_vec(),
            old_data: old_data.to_vec(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        let start = buf.len();

        // add lsn
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        // add record_type
        buf.push(self.record_type.to_u8());
        // add table_name (len + [u8])
        let table_name_bytes = self.table_name.as_bytes();
        buf.extend_from_slice(&(table_name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(table_name_bytes);
        // add page_id
        buf.extend_from_slice(&self.page_id.to_le_bytes());
        // add slot
        buf.extend_from_slice(&self.slot.to_le_bytes());
        // add new data (len + [u8])
        buf.extend_from_slice(&(self.new_data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.new_data);
        // add old data (len + [u8])
        buf.extend_from_slice(&(self.old_data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.old_data);

        // append checksum
        let checksum = crc32fast::hash(&buf[start..]);
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut offset = 0;

        if bytes.len() < offset + 8 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for lsn",
            ));
        }
        let lsn = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        if bytes.len() < offset + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for record type",
            ));
        }
        let record_type = WalRecordType::try_from(bytes[offset])?;
        offset += 1;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for table name length",
            ));
        }
        let table_name_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + table_name_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for table name",
            ));
        }
        let table_name = String::from_utf8(bytes[offset..offset + table_name_len].to_vec())
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid UTF-8 in table name: {}", e),
                )
            })?;
        offset += table_name_len;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for page id",
            ));
        }
        let page_id = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        if bytes.len() < offset + 2 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for slot",
            ));
        }
        let slot = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for new data length",
            ));
        }
        let new_data_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + new_data_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for new data",
            ));
        }
        let new_data = bytes[offset..offset + new_data_len].to_vec();
        offset += new_data_len;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for old data length",
            ));
        }
        let old_data_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + old_data_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for old data",
            ));
        }
        let old_data = bytes[offset..offset + old_data_len].to_vec();
        offset += old_data_len;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for stored checksum",
            ));
        }

        let stored_checksum = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        let record = Self::new(
            lsn,
            record_type,
            &table_name,
            page_id,
            slot,
            &new_data,
            &old_data,
        );

        // verify record checksum value
        let computed = crc32fast::hash(&bytes[..offset]);

        if computed != stored_checksum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "WAL record checksum mismatch",
            ));
        }

        Ok(record)
    }

    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_serialization() {
        let new_data = "Welcome".as_bytes();
        let old_data = "Hello world".as_bytes();
        let record = WalRecord::new(
            120,
            WalRecordType::Insert,
            "users",
            5,
            17,
            new_data,
            old_data,
        );

        let record_bytes = record.to_bytes();
        assert_eq!(WalRecord::from_bytes(&record_bytes).unwrap(), record);
    }

    #[test]
    fn test_wal_record_serialization_empty_data() {
        let record = WalRecord::new(0, WalRecordType::Checkpoint, "", 0, 0, &[], &[]);
        let bytes = record.to_bytes();
        assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn test_wal_record_checksum_mismatch() {
        let record = WalRecord::new(1, WalRecordType::Insert, "users", 5, 0, b"data", &[]);
        let mut bytes = record.to_bytes();
        // corrupt a byte in the middle
        bytes[4] ^= 0xFF;
        assert!(WalRecord::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_wal_record_truncated_input() {
        let record = WalRecord::new(1, WalRecordType::Insert, "users", 5, 0, b"data", &[]);
        let bytes = record.to_bytes();
        // try every truncation length
        for len in 0..bytes.len() {
            assert!(WalRecord::from_bytes(&bytes[..len]).is_err());
        }
    }

    #[test]
    fn test_wal_record_all_types() {
        let types = [
            WalRecordType::Insert,
            WalRecordType::Delete,
            WalRecordType::Update,
            WalRecordType::CreateTable,
            WalRecordType::DropTable,
            WalRecordType::CreateIndex,
            WalRecordType::DropIndex,
            WalRecordType::Checkpoint,
        ];
        for rt in types {
            let record = WalRecord::new(1, rt, "t", 1, 0, b"new", b"old");
            let bytes = record.to_bytes();
            assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn test_write_to_non_empty_buffer() {
        let record = WalRecord::new(1, WalRecordType::Insert, "users", 1, 0, b"data", &[]);
        let mut buf = vec![0xAAu8; 10]; // pre-existing data
        record.write_to(&mut buf);
        // the appended record should deserialize correctly
        assert_eq!(WalRecord::from_bytes(&buf[10..]).unwrap(), record);
    }
}
