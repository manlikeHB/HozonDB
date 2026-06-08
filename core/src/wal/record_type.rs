use std::io::{self, Error, ErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    // Slotted variants (DML)
    Insert,
    Delete,
    Update,

    // Raw variants (DDL)
    // Catalog
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    AddIndex,
    RemoveIndex,
    RemoveTableIndex,
    UpdateLastPage,

    // B+ tree
    CreateBPlusTree,
    IndexNode,
    IndexRoot,
    DeleteKey,

    FreePage,

    // Checkpoint
    Checkpoint,
}

impl WalRecordType {
    pub fn to_u8(&self) -> u8 {
        u8::from(*self)
    }
}

impl From<WalRecordType> for u8 {
    fn from(value: WalRecordType) -> Self {
        match value {
            WalRecordType::Insert => 1,
            WalRecordType::Delete => 2,
            WalRecordType::Update => 3,
            WalRecordType::CreateTable => 4,
            WalRecordType::DropTable => 5,
            WalRecordType::CreateIndex => 6,
            WalRecordType::DropIndex => 7,
            WalRecordType::Checkpoint => 8,
            WalRecordType::AddIndex => 9,
            WalRecordType::RemoveIndex => 10,
            WalRecordType::RemoveTableIndex => 11,
            WalRecordType::UpdateLastPage => 12,
            WalRecordType::CreateBPlusTree => 13,
            WalRecordType::IndexNode => 14,
            WalRecordType::IndexRoot => 15,
            WalRecordType::DeleteKey => 16,
            WalRecordType::FreePage => 17,
        }
    }
}

impl TryFrom<u8> for WalRecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WalRecordType::Insert),
            2 => Ok(WalRecordType::Delete),
            3 => Ok(WalRecordType::Update),
            4 => Ok(WalRecordType::CreateTable),
            5 => Ok(WalRecordType::DropTable),
            6 => Ok(WalRecordType::CreateIndex),
            7 => Ok(WalRecordType::DropIndex),
            8 => Ok(WalRecordType::Checkpoint),
            9 => Ok(WalRecordType::AddIndex),
            10 => Ok(WalRecordType::RemoveIndex),
            11 => Ok(WalRecordType::RemoveTableIndex),
            12 => Ok(WalRecordType::UpdateLastPage),
            13 => Ok(WalRecordType::CreateBPlusTree),
            14 => Ok(WalRecordType::IndexNode),
            15 => Ok(WalRecordType::IndexRoot),
            16 => Ok(WalRecordType::DeleteKey),
            17 => Ok(WalRecordType::FreePage),
            other => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Unknown value for WAL record type: {}", other),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_type_conversion_to_u8() {
        assert_eq!(WalRecordType::Insert.to_u8(), 1);
        assert_eq!(WalRecordType::Delete.to_u8(), 2);
        assert_eq!(WalRecordType::Update.to_u8(), 3);
        assert_eq!(WalRecordType::CreateTable.to_u8(), 4);
        assert_eq!(WalRecordType::DropTable.to_u8(), 5);
        assert_eq!(WalRecordType::CreateIndex.to_u8(), 6);
        assert_eq!(WalRecordType::DropIndex.to_u8(), 7);
        assert_eq!(WalRecordType::Checkpoint.to_u8(), 8);
        assert_eq!(WalRecordType::AddIndex.to_u8(), 9);
        assert_eq!(WalRecordType::RemoveIndex.to_u8(), 10);
        assert_eq!(WalRecordType::RemoveTableIndex.to_u8(), 11);
        assert_eq!(WalRecordType::UpdateLastPage.to_u8(), 12);
        assert_eq!(WalRecordType::CreateBPlusTree.to_u8(), 13);
        assert_eq!(WalRecordType::IndexNode.to_u8(), 14);
        assert_eq!(WalRecordType::IndexRoot.to_u8(), 15);
        assert_eq!(WalRecordType::DeleteKey.to_u8(), 16);
        assert_eq!(WalRecordType::FreePage.to_u8(), 17);
    }

    #[test]
    fn test_wal_record_type_conversion_from_u8() {
        assert_eq!(WalRecordType::try_from(1).unwrap(), WalRecordType::Insert);
        assert_eq!(WalRecordType::try_from(2).unwrap(), WalRecordType::Delete);
        assert_eq!(WalRecordType::try_from(3).unwrap(), WalRecordType::Update);
        assert_eq!(
            WalRecordType::try_from(4).unwrap(),
            WalRecordType::CreateTable
        );
        assert_eq!(
            WalRecordType::try_from(5).unwrap(),
            WalRecordType::DropTable
        );
        assert_eq!(
            WalRecordType::try_from(6).unwrap(),
            WalRecordType::CreateIndex
        );
        assert_eq!(
            WalRecordType::try_from(7).unwrap(),
            WalRecordType::DropIndex
        );
        assert_eq!(
            WalRecordType::try_from(8).unwrap(),
            WalRecordType::Checkpoint
        );
        assert_eq!(WalRecordType::try_from(9).unwrap(), WalRecordType::AddIndex);
        assert_eq!(
            WalRecordType::try_from(10).unwrap(),
            WalRecordType::RemoveIndex
        );
        assert_eq!(
            WalRecordType::try_from(11).unwrap(),
            WalRecordType::RemoveTableIndex
        );
        assert_eq!(
            WalRecordType::try_from(12).unwrap(),
            WalRecordType::UpdateLastPage
        );
        assert_eq!(
            WalRecordType::try_from(13).unwrap(),
            WalRecordType::CreateBPlusTree
        );
        assert_eq!(
            WalRecordType::try_from(14).unwrap(),
            WalRecordType::IndexNode
        );
        assert_eq!(
            WalRecordType::try_from(15).unwrap(),
            WalRecordType::IndexRoot
        );
        assert_eq!(
            WalRecordType::try_from(16).unwrap(),
            WalRecordType::DeleteKey
        );
        assert_eq!(
            WalRecordType::try_from(17).unwrap(),
            WalRecordType::FreePage
        );
    }

    #[test]
    fn test_wal_record_type_conversion_from_unsupported_u8() {
        for i in 18..255 {
            assert!(WalRecordType::try_from(i).is_err());
        }
    }
}
