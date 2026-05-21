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
