#[derive(Debug)]
pub enum Value {
    Integer(i32),
    Text(String),
    Boolean(bool),
    Null,
}

#[derive(Debug)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        for value in self.values.iter() {
            match value {
                Value::Integer(val) => {
                    bytes.push(0);
                    bytes.extend_from_slice(&val.to_le_bytes());
                }
                Value::Text(text) => {
                    bytes.push(1);
                    let text_bytes = text.as_bytes();
                    bytes.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                Value::Boolean(bool) => {
                    bytes.push(2);
                    bytes.push(if *bool { 1 } else { 0 });
                }
                Value::Null => {
                    bytes.push(3);
                }
            }
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let mut values = Vec::new();
        let mut offset = 0;

        while offset < bytes.len() {
            let value_type = bytes[offset];
            offset += 1;

            match value_type {
                0 => {
                    if bytes.len() < offset + 4 {
                        return Err("Not enough bytes for Integer".to_string());
                    }

                    let int_val = i32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]);
                    values.push(Value::Integer(int_val));
                    offset += 4;
                }
                1 => {
                    if bytes.len() < offset + 4 {
                        return Err("Not enough bytes for Text length".to_string());
                    }

                    let text_len = u32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    offset += 4;

                    if bytes.len() < offset + text_len {
                        return Err("Not enough bytes for Text".to_string());
                    }

                    let text = String::from_utf8(bytes[offset..offset + text_len].to_vec())
                        .map_err(|e| format!("Invald UTF in Text: {}", e))?;
                    values.push(Value::Text(text));
                    offset += text_len;
                }
                2 => {
                    if bytes.len() < offset + 1 {
                        return Err("Not enough bytes for Boolean".to_string());
                    }

                    let bool_val = bytes[offset] != 0;
                    values.push(Value::Boolean(bool_val));
                    offset += 1;
                }
                3 => {
                    values.push(Value::Null);
                }
                _ => return Err("Unknown value type".to_string()),
            }
        }
        Ok(Row { values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_serialization() {
        let row = Row {
            values: vec![
                Value::Integer(42),
                Value::Text("Hello".to_string()),
                Value::Boolean(true),
                Value::Null,
            ],
        };

        let bytes = row.to_bytes();
        let deserialized_row = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row.values.len(), deserialized_row.values.len());
        for (original, deserialized) in row.values.iter().zip(deserialized_row.values.iter()) {
            match (original, deserialized) {
                (Value::Integer(a), Value::Integer(b)) => assert_eq!(a, b),
                (Value::Text(a), Value::Text(b)) => assert_eq!(a, b),
                (Value::Boolean(a), Value::Boolean(b)) => assert_eq!(a, b),
                (Value::Null, Value::Null) => (),
                _ => panic!("Mismatched value types"),
            }
        }
    }
}
