use std::io::{self, Error, ErrorKind};

use crate::{catalog::schema::DataType, constants};

pub const INTEGER_INDEX_COLUMN_TYPE: u8 = 1;
pub const TEXT_INDEX_COLUMN_TYPE: u8 = 2;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IndexColumnType {
    Text,
    Integer,
}

impl IndexColumnType {
    pub fn order(&self) -> usize {
        match self {
            IndexColumnType::Integer => constants::BTREE_INTEGER_ORDER,
            IndexColumnType::Text => constants::BTREE_TEXT_ORDER,
        }
    }
}

impl TryFrom<DataType> for IndexColumnType {
    type Error = io::Error;
    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Integer => Ok(IndexColumnType::Integer),
            DataType::Text => Ok(IndexColumnType::Text),
            other => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("{:?}, is not a supported Index column type", other),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    index_name: String,
    table_name: String,
    column_name: String,
    column_type: IndexColumnType,
    is_primary: bool,
    root_page_id: u32,
}

impl IndexEntry {
    pub fn new(
        index_name: &str,
        table_name: &str,
        column_name: &str,
        column_type: IndexColumnType,
        is_primary: bool,
        root_page_id: u32,
    ) -> Self {
        IndexEntry {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            column_type,
            is_primary,
            root_page_id,
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn column_type(&self) -> IndexColumnType {
        self.column_type
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn root_page_id(&self) -> u32 {
        self.root_page_id
    }

    pub fn set_root_page_id(&mut self, root_page_id: u32) {
        self.root_page_id = root_page_id;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // write index name (length + name)
        let index_name_bytes = self.index_name.as_bytes();
        bytes.extend_from_slice(&(index_name_bytes.len() as u32).to_le_bytes()); // len = 4 bytes
        bytes.extend_from_slice(index_name_bytes);

        // write table name (length + name)
        let table_name_bytes = self.table_name.as_bytes();
        bytes.extend_from_slice(&(table_name_bytes.len() as u32).to_le_bytes()); // len = 4 bytes
        bytes.extend_from_slice(table_name_bytes);

        // write column name (length + name)
        let col_name_bytes = self.column_name.as_bytes();
        bytes.extend_from_slice(&(col_name_bytes.len() as u32).to_le_bytes()); // len = 4 bytes
        bytes.extend_from_slice(col_name_bytes);

        // write column type
        bytes.push(match self.column_type() {
            IndexColumnType::Integer => INTEGER_INDEX_COLUMN_TYPE,
            IndexColumnType::Text => TEXT_INDEX_COLUMN_TYPE,
        });

        // write is_primary
        bytes.push(match self.is_primary {
            false => 0,
            true => 1,
        });

        // write root_page_id
        bytes.extend_from_slice(&(self.root_page_id).to_le_bytes()); // len = 4 bytes

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;

        // extract index name
        let index_name_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4; // 4 bytes for length

        if bytes.len() < offset + index_name_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for index name length".to_string(),
            ));
        }

        let index_name = String::from_utf8(bytes[offset..offset + index_name_len].to_vec())
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid UTF-8 in index name: {}", e),
                )
            })?;
        offset += index_name_len;

        // extract table name
        let table_name_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4; // 4 bytes for length

        if bytes.len() < offset + table_name_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for table name length".to_string(),
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

        // extract column name
        let col_name_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4; // 4 bytes for length

        if bytes.len() < offset + col_name_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for column name length".to_string(),
            ));
        }
        let column_name = String::from_utf8(bytes[offset..offset + col_name_len].to_vec())
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid UTF-8 in table name: {}", e),
                )
            })?;
        offset += col_name_len;

        // extract column type
        if bytes.len() < offset + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for column type".to_string(),
            ));
        }

        let column_type = match bytes[offset] {
            INTEGER_INDEX_COLUMN_TYPE => IndexColumnType::Integer,
            TEXT_INDEX_COLUMN_TYPE => IndexColumnType::Text,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Unknown value for column type".to_string(),
                ));
            }
        };
        offset += 1;

        // extract is_primary
        let is_primary = match bytes[offset] {
            0 => false,
            1 => true,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Unknown value for if index is primary key".to_string(),
                ));
            }
        };

        offset += 1;

        // extract root page id
        let root_page_id = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);

        offset += 4;

        Ok((
            IndexEntry {
                index_name,
                table_name,
                column_name,
                column_type,
                is_primary,
                root_page_id,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_serialization_integer_column_type() {
        let index_entry = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            5_u32,
        );

        let bytes = index_entry.to_bytes();
        let (decoded, _) = IndexEntry::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.index_name(), "idx_users_id");
        assert_eq!(decoded.table_name(), "users");
        assert_eq!(decoded.column_name(), "id");
        assert_eq!(decoded.column_type(), IndexColumnType::Integer);
        assert_eq!(decoded.is_primary(), true);
        assert_eq!(decoded.root_page_id(), 5_u32);
    }

    #[test]
    fn test_index_entry_serialization_text_column_type() {
        let index_entry = IndexEntry::new(
            "idx_users_email",
            "users",
            "email",
            IndexColumnType::Text,
            false,
            5_u32,
        );

        let bytes = index_entry.to_bytes();
        let (decoded, _) = IndexEntry::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.index_name(), "idx_users_email");
        assert_eq!(decoded.table_name(), "users");
        assert_eq!(decoded.column_name(), "email");
        assert_eq!(decoded.column_type(), IndexColumnType::Text);
        assert_eq!(decoded.is_primary(), false);
        assert_eq!(decoded.root_page_id(), 5_u32);
    }

    #[test]
    fn test_try_from_data_types() {
        let bool_data_type = DataType::Boolean;
        let int_data_type = DataType::Integer;
        let null_data_type = DataType::Null;
        let text_data_type = DataType::Text;

        assert!(IndexColumnType::try_from(bool_data_type).is_err());
        assert!(IndexColumnType::try_from(null_data_type).is_err());
        assert_eq!(
            IndexColumnType::try_from(int_data_type).unwrap(),
            IndexColumnType::Integer
        );
        assert_eq!(
            IndexColumnType::try_from(text_data_type).unwrap(),
            IndexColumnType::Text
        );
    }
}
