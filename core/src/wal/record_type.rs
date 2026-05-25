use std::io::{self, Error, ErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    // DML
    Insert,
    Delete,
    Update,

    // DDL
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,

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
    }

    #[test]
    fn test_wal_record_type_conversion_from_unsupported_u8() {
        for i in 9..255 {
            assert!(WalRecordType::try_from(i).is_err());
        }
    }
}
