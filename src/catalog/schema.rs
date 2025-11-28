use std::io::{self, Error, ErrorKind};

#[derive(Debug)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Null,
}

#[derive(Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
}

#[derive(Debug)]
pub struct Schema {
    table_name: String,
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(table_name: &str, columns: Vec<Column>) -> Self {
        Schema {
            table_name: table_name.to_string(),
            columns,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // write table name (length + name)
        let name_bytes = self.table_name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes()); // len = 4 bytes
        bytes.extend_from_slice(name_bytes);

        // write number of columns
        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());

        // write each column (name length + name + data type)
        for column in self.columns.iter() {
            let col_name_bytes = column.name.as_bytes();
            bytes.extend_from_slice(&(col_name_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(col_name_bytes);
            bytes.push(match column.data_type {
                DataType::Integer => 0,
                DataType::Text => 1,
                DataType::Boolean => 2,
                DataType::Null => 3,
            })
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
                _ => panic!("Unknown data type"),
            };

            offset += 1; // 1 byte for data type

            columns.push(Column {
                name: col_name,
                data_type,
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
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        let schema = Schema::new("users", columns);
        let bytes = schema.to_bytes();
        let (decoded, _) = Schema::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.table_name, "users");
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.columns[0].name, "id");
        assert_eq!(decoded.columns[1].name, "name");
    }
}
