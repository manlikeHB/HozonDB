use std::io::{Error, ErrorKind};

use crate::{
    constants::{
        PageId, WAL_RECORD_ALLOCATE_PAGE_TYPE, WAL_RECORD_CHECKPOINT_TYPE,
        WAL_RECORD_LINK_PAGE_TYPE, WAL_RECORD_RAW_TYPE, WAL_RECORD_SLOTTED_TYPE,
    },
    storage::page::PageType,
    wal::record_type::WalRecordType,
};

#[derive(Debug, PartialEq, Eq)]
pub enum WalRecord {
    Slotted {
        lsn: u64,
        record_type: WalRecordType,
        table_name: String,
        page_id: PageId,
        slot: u16,
        new_data: Vec<u8>,
        old_data: Vec<u8>,
    },
    Raw {
        lsn: u64,
        record_type: WalRecordType,
        page_id: PageId,
        new_data: Vec<u8>,
        old_data: Vec<u8>,
    },
    Checkpoint {
        lsn: u64,
    },
    LinkPage {
        lsn: u64,
        page_id: PageId,
        next_page: PageId,
    },
    AllocatePage {
        lsn: u64,
        page_id: PageId,
        page_type: u8, // serialized as u8
    },
}

impl WalRecord {
    pub fn new_slotted(
        lsn: u64,
        record_type: WalRecordType,
        table_name: &str,
        page_id: PageId,
        slot: u16,
        new_data: &[u8],
        old_data: &[u8],
    ) -> Self {
        WalRecord::Slotted {
            lsn,
            record_type,
            table_name: table_name.to_string(),
            page_id,
            slot,
            new_data: new_data.to_vec(),
            old_data: old_data.to_vec(),
        }
    }

    pub fn new_raw(
        lsn: u64,
        record_type: WalRecordType,
        page_id: PageId,
        new_data: &[u8],
        old_data: &[u8],
    ) -> Self {
        WalRecord::Raw {
            lsn,
            record_type,
            page_id,
            new_data: new_data.to_vec(),
            old_data: old_data.to_vec(),
        }
    }

    pub fn new_checkpoint(lsn: u64) -> Self {
        WalRecord::Checkpoint { lsn }
    }

    pub fn new_link_page(lsn: u64, page_id: PageId, next_page: PageId) -> Self {
        WalRecord::LinkPage {
            lsn,
            page_id,
            next_page,
        }
    }

    pub fn new_allocate_page(lsn: u64, page_id: PageId, page_type: u8) -> Self {
        WalRecord::AllocatePage {
            lsn,
            page_id,
            page_type,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        let start = buf.len();

        match self {
            WalRecord::Slotted {
                lsn,
                record_type,
                table_name,
                page_id,
                slot,
                new_data,
                old_data,
            } => {
                // add record type
                buf.push(WAL_RECORD_SLOTTED_TYPE);
                // add lsn
                buf.extend_from_slice(&lsn.to_le_bytes());
                // add record_type
                buf.push(record_type.to_u8());
                // add table_name (len + [u8])
                let table_name_bytes = table_name.as_bytes();
                buf.extend_from_slice(&(table_name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(table_name_bytes);
                // add page_id
                buf.extend_from_slice(&page_id.to_le_bytes());
                // add slot
                buf.extend_from_slice(&slot.to_le_bytes());
                // add new data (len + [u8])
                buf.extend_from_slice(&(new_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&new_data);
                // add old data (len + [u8])
                buf.extend_from_slice(&(old_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&old_data);
            }
            WalRecord::Raw {
                lsn,
                record_type,
                page_id,
                new_data,
                old_data,
            } => {
                // add record type
                buf.push(WAL_RECORD_RAW_TYPE);
                // add lsn
                buf.extend_from_slice(&lsn.to_le_bytes());
                // add record_type
                buf.push(record_type.to_u8());
                // add page_id
                buf.extend_from_slice(&page_id.to_le_bytes());
                // add new data (len + [u8])
                buf.extend_from_slice(&(new_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&new_data);
                // add old data (len + [u8])
                buf.extend_from_slice(&(old_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&old_data);
            }
            WalRecord::Checkpoint { lsn } => {
                // add record type
                buf.push(WAL_RECORD_CHECKPOINT_TYPE);
                // add lsn
                buf.extend_from_slice(&lsn.to_le_bytes());
            }
            WalRecord::LinkPage {
                lsn,
                page_id,
                next_page,
            } => {
                // add record type
                buf.push(WAL_RECORD_LINK_PAGE_TYPE);
                // add lsn
                buf.extend_from_slice(&lsn.to_le_bytes());
                // add page id
                buf.extend_from_slice(&page_id.to_le_bytes());
                // add next page
                buf.extend_from_slice(&next_page.to_le_bytes());
            }
            WalRecord::AllocatePage {
                lsn,
                page_id,
                page_type,
            } => {
                buf.push(WAL_RECORD_ALLOCATE_PAGE_TYPE);
                buf.extend_from_slice(&lsn.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.push(*page_type);
            }
        }

        // append checksum
        let checksum = crc32fast::hash(&buf[start..]);
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut offset = 0;

        if bytes.len() < offset + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes WAL record type: (slotted/raw/checkpoint)",
            ));
        }

        let wal_record_type = u8::from_le_bytes([bytes[offset]]);
        offset += 1;

        let (record, stored_checksum) = match wal_record_type {
            WAL_RECORD_SLOTTED_TYPE => {
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

                let record = Self::new_slotted(
                    lsn,
                    record_type,
                    &table_name,
                    page_id,
                    slot,
                    &new_data,
                    &old_data,
                );

                (record, stored_checksum)
            }
            WAL_RECORD_RAW_TYPE => {
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

                let record = Self::new_raw(lsn, record_type, page_id, &new_data, &old_data);

                (record, stored_checksum)
            }
            WAL_RECORD_CHECKPOINT_TYPE => {
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

                let record = Self::new_checkpoint(lsn);

                (record, stored_checksum)
            }
            WAL_RECORD_LINK_PAGE_TYPE => {
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

                if bytes.len() < offset + 4 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Not enough bytes for next page",
                    ));
                }
                let next_page = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                offset += 4;

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

                let record = Self::new_link_page(lsn, page_id, next_page);

                (record, stored_checksum)
            }
            WAL_RECORD_ALLOCATE_PAGE_TYPE => {
                let lsn = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let page_id = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                offset += 4;
                let page_type = bytes[offset];
                offset += 1;
                let stored_checksum =
                    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                (
                    WalRecord::AllocatePage {
                        lsn,
                        page_id,
                        page_type,
                    },
                    stored_checksum,
                )
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid WAL record type (slotted/raw): {}", other),
                ));
            }
        };

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
        match self {
            WalRecord::Slotted { lsn, .. } => *lsn,
            WalRecord::Raw { lsn, .. } => *lsn,
            WalRecord::Checkpoint { lsn } => *lsn,
            WalRecord::LinkPage { lsn, .. } => *lsn,
            WalRecord::AllocatePage { lsn, .. } => *lsn,
        }
    }

    pub fn record_type(&self) -> Option<WalRecordType> {
        match self {
            WalRecord::Slotted { record_type, .. } => Some(*record_type),
            WalRecord::Raw { record_type, .. } => Some(*record_type),
            _ => None,
        }
    }

    pub fn table_name(&self) -> Option<&str> {
        match self {
            WalRecord::Slotted { table_name, .. } => Some(table_name),
            WalRecord::Raw { .. } => None,
            _ => None,
        }
    }

    pub fn page_id(&self) -> Option<PageId> {
        match self {
            WalRecord::Slotted { page_id, .. } => Some(*page_id),
            WalRecord::Raw { page_id, .. } => Some(*page_id),
            WalRecord::Checkpoint { .. } => None,
            WalRecord::LinkPage { page_id, .. } => Some(*page_id),
            WalRecord::AllocatePage { page_id, .. } => Some(*page_id),
        }
    }

    pub fn slot(&self) -> Option<u16> {
        match self {
            WalRecord::Slotted { slot, .. } => Some(*slot),
            WalRecord::Raw { .. } => None,
            _ => None,
        }
    }

    pub fn new_data(&self) -> Option<&[u8]> {
        match self {
            WalRecord::Slotted { new_data, .. } => Some(new_data),
            WalRecord::Raw { new_data, .. } => Some(new_data),
            _ => None,
        }
    }

    pub fn old_data(&self) -> Option<&[u8]> {
        match self {
            WalRecord::Slotted { old_data, .. } => Some(old_data),
            WalRecord::Raw { old_data, .. } => Some(old_data),
            _ => None,
        }
    }

    pub fn page_type(&self) -> Option<PageType> {
        match self {
            WalRecord::AllocatePage { page_type, .. } => PageType::from_u8(*page_type).ok(),
            _ => None,
        }
    }

    pub fn next_page(&self) -> Option<PageId> {
        match self {
            WalRecord::LinkPage { next_page, .. } => Some(*next_page),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_serialization_slotted() {
        let new_data = "Welcome".as_bytes();
        let old_data = "Hello world".as_bytes();
        let record = WalRecord::new_slotted(
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
    fn test_wal_record_serialization_empty_data_slotted() {
        let record = WalRecord::new_slotted(0, WalRecordType::Checkpoint, "", 0, 0, &[], &[]);
        let bytes = record.to_bytes();
        assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn test_wal_record_checksum_mismatch_slotted() {
        let record = WalRecord::new_slotted(1, WalRecordType::Insert, "users", 5, 0, b"data", &[]);
        let mut bytes = record.to_bytes();
        // corrupt a byte in the middle
        bytes[4] ^= 0xFF;
        assert!(WalRecord::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_wal_record_truncated_input_slotted() {
        let record = WalRecord::new_slotted(1, WalRecordType::Insert, "users", 5, 0, b"data", &[]);
        let bytes = record.to_bytes();
        // try every truncation length
        for len in 0..bytes.len() {
            assert!(WalRecord::from_bytes(&bytes[..len]).is_err());
        }
    }

    #[test]
    fn test_wal_record_all_types_slotted() {
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
            let record = WalRecord::new_slotted(1, rt, "t", 1, 0, b"new", b"old");
            let bytes = record.to_bytes();
            assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn test_write_to_non_empty_buffer_slotted() {
        let record = WalRecord::new_slotted(1, WalRecordType::Insert, "users", 1, 0, b"data", &[]);
        let mut buf = vec![0xAAu8; 10]; // pre-existing data
        record.write_to(&mut buf);
        // the appended record should deserialize correctly
        assert_eq!(WalRecord::from_bytes(&buf[10..]).unwrap(), record);
    }

    // add to record.rs mod tests

    #[test]
    fn test_wal_record_serialization_link_page() {
        let record = WalRecord::new_link_page(5, 3, 7); // lsn=5, page_id=3, next_page=7
        let bytes = record.to_bytes();
        assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn test_wal_record_link_page_accessors() {
        let record = WalRecord::new_link_page(1, 4, 9);
        assert_eq!(record.lsn(), 1);
        assert_eq!(record.page_id(), Some(4));
        assert_eq!(record.next_page(), Some(9));
        assert!(record.new_data().is_none());
        assert!(record.slot().is_none());
        assert!(record.record_type().is_none());
    }

    #[test]
    fn test_wal_record_link_page_checksum_mismatch() {
        let record = WalRecord::new_link_page(1, 3, 7);
        let mut bytes = record.to_bytes();
        bytes[4] ^= 0xFF; // corrupt a byte
        assert!(WalRecord::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_wal_record_link_page_truncated() {
        let record = WalRecord::new_link_page(1, 3, 7);
        let bytes = record.to_bytes();
        for len in 0..bytes.len() {
            assert!(WalRecord::from_bytes(&bytes[..len]).is_err());
        }
    }

    #[test]
    fn test_wal_record_checkpoint_accessors() {
        let record = WalRecord::new_checkpoint(42);
        assert_eq!(record.lsn(), 42);
        assert!(record.page_id().is_none());
        assert!(record.new_data().is_none());
        assert!(record.old_data().is_none());
        assert!(record.slot().is_none());
        assert!(record.record_type().is_none());
        assert!(record.next_page().is_none());
    }

    #[test]
    fn test_wal_record_serialization_raw() {
        let record =
            WalRecord::new_raw(10, WalRecordType::IndexNode, 5, &[0u8; 4096], &[1u8; 4096]);
        let bytes = record.to_bytes();
        assert_eq!(WalRecord::from_bytes(&bytes).unwrap(), record);
    }

    #[test]
    fn test_wal_record_raw_checksum_mismatch() {
        let record = WalRecord::new_raw(1, WalRecordType::IndexNode, 3, b"new", b"old");
        let mut bytes = record.to_bytes();
        bytes[4] ^= 0xFF;
        assert!(WalRecord::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_wal_record_raw_truncated() {
        let record = WalRecord::new_raw(1, WalRecordType::CreateTable, 1, b"data", &[]);
        let bytes = record.to_bytes();
        for len in 0..bytes.len() {
            assert!(WalRecord::from_bytes(&bytes[..len]).is_err());
        }
    }
}
