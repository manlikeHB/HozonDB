use crate::catalog::schema::{Schema};
use crate::storage::page::PageManager;
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};
pub struct TableMetadata {
    schema: Schema,
    first_page: u32,
}

pub struct TableCatalog {
    tables: HashMap<String, TableMetadata>,
    page_manager: PageManager,
}

impl TableCatalog {
    pub fn new(page_manager: PageManager) -> io::Result<Self> {
        // try reading existing catalog
        let catalog_data = match page_manager.read_page(1u32) {
            Ok(data) => data,
            Err(e) if e.kind() == ErrorKind::InvalidInput => {
                // no existing catalog, return empty
                [0u8; 4096] // page size is 4096 bytes
            }
            Err(e) => return Err(e),
        };

        if catalog_data.iter().all(|&b| b == 0) {
            // empty catalog
            return Ok(TableCatalog {
                tables: HashMap::new(),
                page_manager,
            });
        }

        // parse catalog data
        let mut offset = 0;

        if catalog_data.len() < 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for number of tables".to_string(),
            ));
        }

        let num_tables = u32::from_le_bytes([
            catalog_data[offset],
            catalog_data[offset + 1],
            catalog_data[offset + 2],
            catalog_data[offset + 3],
        ]) as usize;
        offset += 4;

        let mut tables = HashMap::new();

        for _ in 0..num_tables {
            let (schema, bytes_consumed) = Schema::from_bytes(&catalog_data[offset..])?;
            offset += bytes_consumed;

            if catalog_data.len() < offset + 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for first page".to_string(),
                ));
            }

            let first_page = u32::from_le_bytes([
                catalog_data[offset],
                catalog_data[offset + 1],
                catalog_data[offset + 2],
                catalog_data[offset + 3],
            ]);
            offset += 4;

            let table_metadata = TableMetadata { schema, first_page };

            tables.insert(
                table_metadata.schema.table_name().to_string(),
                table_metadata,
            );
        }

        Ok(TableCatalog {
            tables,
            page_manager,
        })
    }

    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        // allocate first page for table data
        let first_page = self.page_manager.allocate_page()?;

        let table_name = schema.table_name().to_string();
        let table_metadata = TableMetadata { schema, first_page };

        self.tables.insert(table_name, table_metadata);

        // save to disk
        self.save()?;

        Ok(())
    }

    pub fn save(&mut self) -> io::Result<()> {
        let bytes = self.to_bytes();
        self.page_manager.write_page(1u32, &bytes)?;
        Ok(())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // number of tables
        bytes.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (_, metadata) in self.tables.iter() {
            let schema_bytes = metadata.schema.to_bytes();
            bytes.extend_from_slice(&schema_bytes);

            // first page
            bytes.extend_from_slice(&metadata.first_page.to_le_bytes());
        }

        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{Column, DataType, Schema};
    use std::fs;

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    #[test]
    fn test_new_catalog_empty() {
        cleanup("test_new_catalog");

        let pm = PageManager::new("test_new_catalog.hdb").unwrap();
        let catalog = TableCatalog::new(pm).unwrap();

        assert_eq!(catalog.tables.len(), 0);

        cleanup("test_new_catalog");
    }

    #[test]
    fn test_create_single_table() {
        cleanup("test_single");

        let pm = PageManager::new("test_single.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        let schema = Schema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer),
                Column::new("name", DataType::Text),
            ],
        );

        catalog.create_table(schema).unwrap();

        assert_eq!(catalog.tables.len(), 1);
        assert!(catalog.tables.contains_key("users"));

        cleanup("test_single");
    }

    #[test]
    fn test_create_multiple_tables() {
        cleanup("test_multiple");

        let pm = PageManager::new("test_multiple.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        // Create first table
        let users_schema = Schema::new("users", vec![Column::new("id", DataType::Integer)]);
        catalog.create_table(users_schema).unwrap();

        // Create second table
        let orders_schema = Schema::new(
            "orders",
            vec![
                Column::new("id", DataType::Integer),
                Column::new("total", DataType::Integer),
            ],
        );
        catalog.create_table(orders_schema).unwrap();

        assert_eq!(catalog.tables.len(), 2);
        assert!(catalog.tables.contains_key("users"));
        assert!(catalog.tables.contains_key("orders"));

        cleanup("test_multiple");
    }

    #[test]
    fn test_catalog_persistence() {
        cleanup("test_persist");

        // Create catalog and add table
        {
            let pm = PageManager::new("test_persist.hdb").unwrap();
            let mut catalog = TableCatalog::new(pm).unwrap();

            let schema = Schema::new(
                "users",
                vec![
                    Column::new("id", DataType::Integer),
                    Column::new("name", DataType::Text),
                ],
            );

            catalog.create_table(schema).unwrap();
            assert_eq!(catalog.tables.len(), 1);
        } // catalog dropped, file closed

        // Re-open and verify table still exists
        {
            let pm = PageManager::new("test_persist.hdb").unwrap();
            let catalog = TableCatalog::new(pm).unwrap();

            assert_eq!(catalog.tables.len(), 1);
            assert!(catalog.tables.contains_key("users"));

            let metadata = catalog.tables.get("users").unwrap();
            assert_eq!(metadata.schema.table_name(), "users");
            assert_eq!(metadata.schema.columns().len(), 2);
        }

        cleanup("test_persist");
    }

    #[test]
    fn test_multiple_tables_persistence() {
        cleanup("test_multi_persist");

        // Create and save multiple tables
        {
            let pm = PageManager::new("test_multi_persist.hdb").unwrap();
            let mut catalog = TableCatalog::new(pm).unwrap();

            catalog
                .create_table(Schema::new(
                    "users",
                    vec![Column::new("id", DataType::Integer)],
                ))
                .unwrap();

            catalog
                .create_table(Schema::new(
                    "orders",
                    vec![
                        Column::new("id", DataType::Integer),
                        Column::new("user_id", DataType::Integer),
                    ],
                ))
                .unwrap();

            catalog
                .create_table(Schema::new(
                    "products",
                    vec![
                        Column::new("name", DataType::Text),
                        Column::new("price", DataType::Integer),
                    ],
                ))
                .unwrap();
        }

        // Reload and verify all tables
        {
            let pm = PageManager::new("test_multi_persist.hdb").unwrap();
            let catalog = TableCatalog::new(pm).unwrap();

            assert_eq!(catalog.tables.len(), 3);
            assert!(catalog.tables.contains_key("users"));
            assert!(catalog.tables.contains_key("orders"));
            assert!(catalog.tables.contains_key("products"));

            // Verify schema details
            let users = catalog.tables.get("users").unwrap();
            assert_eq!(users.schema.columns().len(), 1);

            let orders = catalog.tables.get("orders").unwrap();
            assert_eq!(orders.schema.columns().len(), 2);

            let products = catalog.tables.get("products").unwrap();
            assert_eq!(products.schema.columns().len(), 2);
        }

        cleanup("test_multi_persist");
    }

    #[test]
    fn test_first_page_allocation() {
        cleanup("test_page_alloc");

        let pm = PageManager::new("test_page_alloc.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        let initial_pages = catalog.page_manager.num_pages();

        // Create first table
        catalog
            .create_table(Schema::new(
                "users",
                vec![Column::new("id", DataType::Integer)],
            ))
            .unwrap();

        let users_page = catalog.tables.get("users").unwrap().first_page;
        assert_eq!(users_page, initial_pages); // Should allocate next available page

        // Create second table
        catalog
            .create_table(Schema::new(
                "orders",
                vec![Column::new("id", DataType::Integer)],
            ))
            .unwrap();

        let orders_page = catalog.tables.get("orders").unwrap().first_page;
        assert_eq!(orders_page, users_page + 1); // Should allocate next page

        cleanup("test_page_alloc");
    }

    #[test]
    fn test_table_with_all_data_types() {
        cleanup("test_all_types");

        let pm = PageManager::new("test_all_types.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        let schema = Schema::new(
            "test_table",
            vec![
                Column::new("int_col", DataType::Integer),
                Column::new("text_col", DataType::Text),
                Column::new("bool_col", DataType::Boolean),
                Column::new("null_col", DataType::Null),
            ],
        );

        catalog.create_table(schema).unwrap();

        // Reload and verify
        drop(catalog);
        let pm = PageManager::new("test_all_types.hdb").unwrap();
        let catalog = TableCatalog::new(pm).unwrap();

        let metadata = catalog.tables.get("test_table").unwrap();
        assert_eq!(metadata.schema.columns().len(), 4);

        cleanup("test_all_types");
    }

    #[test]
    fn test_empty_table_name() {
        cleanup("test_empty_name");

        let pm = PageManager::new("test_empty_name.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        let schema = Schema::new("", vec![Column::new("id", DataType::Integer)]);

        // Should still work (validation not implemented yet)
        catalog.create_table(schema).unwrap();
        assert!(catalog.tables.contains_key(""));

        cleanup("test_empty_name");
    }

    #[test]
    fn test_table_with_long_name() {
        cleanup("test_long_name");

        let pm = PageManager::new("test_long_name.hdb").unwrap();
        let mut catalog = TableCatalog::new(pm).unwrap();

        let long_name = "a".repeat(1000);
        let schema = Schema::new(&long_name, vec![Column::new("id", DataType::Integer)]);

        catalog.create_table(schema).unwrap();

        // Reload and verify
        drop(catalog);
        let pm = PageManager::new("test_long_name.hdb").unwrap();
        let catalog = TableCatalog::new(pm).unwrap();

        assert!(catalog.tables.contains_key(&long_name));

        cleanup("test_long_name");
    }
}
