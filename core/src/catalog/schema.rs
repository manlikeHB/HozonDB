use std::io::{self, Error, ErrorKind};

// TODO: use constant values to represent DataType u8 values
#[derive(Debug, Clone, Copy)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Null,
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
    is_primary_key: bool,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, is_primary_key: bool) -> Self {
        Column {
            name: name.to_string(),
            data_type,
            is_primary_key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    table_name: String,
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(table_name: &str, columns: Vec<Column>) -> io::Result<Self> {
        let primary_keys = columns.iter().filter(|c| c.is_primary_key).count();

        if primary_keys > 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("{} table can only have one primary key", table_name),
            ));
        }

        Ok(Schema {
            table_name: table_name.to_string(),
            columns,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // write table name (length + name)
        let name_bytes = self.table_name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes()); // len = 4 bytes
        bytes.extend_from_slice(name_bytes);

        // write number of columns
        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // write each column (name length + name + data type + is_primary_key)
        for column in self.columns.iter() {
            let col_name_bytes = column.name.as_bytes();
            bytes.extend_from_slice(&(col_name_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(col_name_bytes);
            bytes.push(match column.data_type {
                DataType::Integer => 0,
                DataType::Text => 1,
                DataType::Boolean => 2,
                DataType::Null => 3,
            });
            bytes.push(match column.is_primary_key {
                false => 0,
                true => 1,
            });
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;

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

        // extract columns
        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for number of columns".to_string(),
            ));
        }

        let num_columns = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4; // 4 bytes for number of columns

        let mut columns = Vec::new();
        for _ in 0..num_columns {
            if bytes.len() < offset + 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for column name length".to_string(),
                ));
            }
            let col_name_len = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4; // 4 bytes for column name length

            if bytes.len() < offset + col_name_len {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for column name".to_string(),
                ));
            }

            let col_name = String::from_utf8(bytes[offset..offset + col_name_len].to_vec())
                .map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Invalid UTF8 in column name: {}", e),
                    )
                })?;

            offset += col_name_len;

            if bytes.len() < offset + 1 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for column data type".to_string(),
                ));
            }

            let data_type = match bytes[offset] {
                0 => DataType::Integer,
                1 => DataType::Text,
                2 => DataType::Boolean,
                3 => DataType::Null,
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Unknown data type byte: {}", bytes[offset]),
                    ));
                }
            };

            offset += 1; // 1 byte for data type

            if bytes.len() < offset + 1 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for if column is primary key".to_string(),
                ));
            }

            let is_primary_key = match bytes[offset] {
                0 => false,
                1 => true,
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Unknown data type for if column is primary key".to_string(),
                    ));
                }
            };

            offset += 1; // 1 byte for is_primary_key

            columns.push(Column {
                name: col_name,
                data_type,
                is_primary_key,
            });
        }

        Ok((
            Schema {
                table_name,
                columns,
            },
            offset,
        ))
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_serialization() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
                is_primary_key: true,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
            },
        ];

        let schema = Schema::new("users", columns).unwrap();
        let bytes = schema.to_bytes();
        let (decoded, _) = Schema::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.table_name, "users");
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.columns[0].name, "id");
        assert_eq!(decoded.columns[0].is_primary_key, true);
        assert_eq!(decoded.columns[1].name, "name");
        assert_eq!(decoded.columns[1].is_primary_key, false);
    }

    #[test]
    fn test_schema_creation_with_two_primary_keys() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
                is_primary_key: true,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Text,
                is_primary_key: true,
            },
        ];

        let schema = Schema::new("users", columns);
        assert!(schema.is_err());
    }
}
