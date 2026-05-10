use std::io::{self, Error, ErrorKind};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum IndexKey {
    Integer(i32),
    Text(String),
}

const INTEGER_INDEX_KEY: u8 = 0;
const TEXT_INDEX_KEY: u8 = 1;

impl IndexKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match self {
            IndexKey::Integer(value) => {
                // add index type
                bytes.extend_from_slice(&INTEGER_INDEX_KEY.to_le_bytes());
                // add value
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            IndexKey::Text(value) => {
                // add index type
                bytes.extend_from_slice(&TEXT_INDEX_KEY.to_le_bytes());
                // add text (length + value)
                let value_bytes = value.as_bytes();
                bytes.extend_from_slice(&(value_bytes.len() as u8).to_le_bytes());
                bytes.extend_from_slice(&value_bytes);
            }
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(IndexKey, usize)> {
        let mut offset = 0;

        if bytes.len() < offset + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for IndexKey type",
            ));
        }

        let index_type = u8::from_le_bytes([bytes[offset]]);
        offset += 1;

        match index_type {
            INTEGER_INDEX_KEY => {
                if bytes.len() < offset + 4 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Not enough bytes for Integer IndexKey value",
                    ));
                }

                let value = i32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);
                offset += 4;

                Ok((IndexKey::Integer(value), offset))
            }
            TEXT_INDEX_KEY => {
                if bytes.len() < offset + 1 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Not enough bytes for Text IndexKey value length",
                    ));
                }

                let value_len = u8::from_le_bytes([bytes[offset]]) as usize;
                offset += 1;

                if bytes.len() < offset + value_len {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Not enough bytes for Text IndexKey value",
                    ));
                }

                let value =
                    String::from_utf8(bytes[offset..offset + value_len].to_vec()).map_err(|e| {
                        Error::new(
                            ErrorKind::InvalidData,
                            format!("Invalid UTF-8 in Text IntexKey value: {}", e),
                        )
                    })?;
                offset += value_len;

                Ok((IndexKey::Text(value), offset))
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Unknown value for index key type",
                ));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_integer_index_key_serialization() {
        let key = IndexKey::Integer(5);

        let bytes = key.to_bytes();
        let (index_key, _) = IndexKey::from_bytes(&bytes).unwrap();
        assert_eq!(index_key, key);
    }

    #[test]
    fn test_text_index_key_serialization() {
        let key = IndexKey::Text("hello@email.com".to_string());

        let bytes = key.to_bytes();
        let (index_key, _) = IndexKey::from_bytes(&bytes).unwrap();
        assert_eq!(index_key, key);
    }
}
