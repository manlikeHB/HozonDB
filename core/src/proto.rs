use crate::catalog::row::{Row as CatalogRow, Value as CatalogValue};

tonic::include_proto!("hozondb");

impl From<CatalogValue> for Value {
    fn from(value: CatalogValue) -> Self {
        match value {
            CatalogValue::Boolean(b) => Value {
                kind: Some(value::Kind::Boolean(b)),
            },
            CatalogValue::Integer(i) => Value {
                kind: Some(value::Kind::Integer(i)),
            },
            CatalogValue::Text(t) => Value {
                kind: Some(value::Kind::Text(t)),
            },
            CatalogValue::Null => Value {
                kind: Some(value::Kind::IsNull(true)),
            },
        }
    }
}

impl From<CatalogRow> for Row {
    fn from(row: CatalogRow) -> Self {
        Self {
            values: row
                .values()
                .to_vec()
                .into_iter()
                .map(|v| From::from(v))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::row::{Row as CatalogRow, Value as CatalogValue};

    #[test]
    fn test_integer_conversion() {
        let val = CatalogValue::Integer(42);
        let proto_val: Value = val.into();
        assert!(matches!(proto_val.kind, Some(value::Kind::Integer(42))));
    }

    #[test]
    fn test_text_conversion() {
        let val = CatalogValue::Text("hello".to_string());
        let proto_val: Value = val.into();
        assert!(matches!(proto_val.kind, Some(value::Kind::Text(ref s)) if s == "hello"));
    }

    #[test]
    fn test_boolean_conversion() {
        let val = CatalogValue::Boolean(true);
        let proto_val: Value = val.into();
        assert!(matches!(proto_val.kind, Some(value::Kind::Boolean(true))));
    }

    #[test]
    fn test_null_conversion() {
        let val = CatalogValue::Null;
        let proto_val: Value = val.into();
        assert!(matches!(proto_val.kind, Some(value::Kind::IsNull(true))));
    }

    #[test]
    fn test_row_conversion() {
        let row = CatalogRow::new(vec![
            CatalogValue::Integer(1),
            CatalogValue::Text("Alice".to_string()),
            CatalogValue::Boolean(true),
            CatalogValue::Null,
        ]);
        let proto_row: Row = row.into();
        assert_eq!(proto_row.values.len(), 4);
        assert!(matches!(
            proto_row.values[0].kind,
            Some(value::Kind::Integer(1))
        ));
        assert!(matches!(
            proto_row.values[3].kind,
            Some(value::Kind::IsNull(true))
        ));
    }
}
