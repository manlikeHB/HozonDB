use std::io::{self, Error, ErrorKind};

#[derive(Debug, Clone)]
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
    pub fn new(values: Vec<Value>) -> Self {
        Row { values }
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    // Get a specific column value by index
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        for value in self.values.iter() {
            match value {
                Value::Integer(val) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&val.to_le_bytes());
                }
                Value::Text(text) => {
                    bytes.push(2);
                    let text_bytes = text.as_bytes();
                    bytes.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                Value::Boolean(bool) => {
                    bytes.push(3);
                    bytes.push(if *bool { 1 } else { 0 });
                }
                Value::Null => {
                    bytes.push(4);
                }
            }
        }

        bytes.push(0); // Row terminator
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut values = Vec::new();
        let mut offset = 0;

        // Read until we hit zero terminator
        while offset < bytes.len() && bytes[offset] != 0 {
            let value_type = bytes[offset];
            offset += 1;

            match value_type {
                1 => {
                    if bytes.len() < offset + 4 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Not enough bytes for Integer",
                        ));
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
                2 => {
                    if bytes.len() < offset + 4 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Not enough bytes for Text length",
                        ));
                    }

                    let text_len = u32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    offset += 4;

                    if bytes.len() < offset + text_len {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Not enough bytes for Text",
                        ));
                    }

                    let text = String::from_utf8(bytes[offset..offset + text_len].to_vec())
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::InvalidData,
                                format!("Invalid UTF-8 in Text value: {}", e),
                            )
                        })?;
                    values.push(Value::Text(text));
                    offset += text_len;
                }
                3 => {
                    if bytes.len() < offset + 1 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Not enough bytes for Boolean",
                        ));
                    }

                    let bool_val = bytes[offset] != 0;
                    values.push(Value::Boolean(bool_val));
                    offset += 1;
                }
                4 => {
                    values.push(Value::Null);
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Unknown value type: {}", value_type),
                    ));
                }
            }
        }

        // Skip the terminator
        if offset < bytes.len() && bytes[offset] == 0 {
            offset += 1;
        }

        Ok((Row { values }, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_serialization() {
        let mut values = Vec::new();
        values.push(Value::Integer(42));
        values.push(Value::Text("Hello".to_string()));
        values.push(Value::Boolean(true));
        values.push(Value::Null);
        let row = Row::new(values);

        let bytes = row.to_bytes();
        let (deserialized_row, _) = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row.values.len(), deserialized_row.values().len());
        for (original, deserialized) in row.values.iter().zip(deserialized_row.values().iter()) {
            match (original, deserialized) {
                (Value::Integer(a), Value::Integer(b)) => assert_eq!(a, b),
                (Value::Text(a), Value::Text(b)) => assert_eq!(a, b),
                (Value::Boolean(a), Value::Boolean(b)) => assert_eq!(a, b),
                (Value::Null, Value::Null) => (),
                _ => panic!("Mismatched value types"),
            }
        }
    }

    #[test]
    fn test_row_with_terminator() {
        let row = Row::new(vec![Value::Integer(42), Value::Text("test".to_string())]);

        let bytes = row.to_bytes();

        // Should end with 0
        assert_eq!(bytes[bytes.len() - 1], 0);

        let (parsed_row, _) = Row::from_bytes(&bytes).unwrap();
        assert_eq!(parsed_row.values().len(), 2);
    }
}
