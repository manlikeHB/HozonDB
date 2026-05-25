use crate::catalog::schema::Schema;
use crate::constants::{self, PageId};
use crate::storage::page::{PAGE_SIZE, PageManager};
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};
pub struct TableMetadata {
    schema: Schema,
    first_page: PageId,
    last_page: PageId,
}

impl TableMetadata {
    pub fn first_page(&self) -> PageId {
        self.first_page
    }

    pub fn last_page(&self) -> PageId {
        self.last_page
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn update_last_page(&mut self, page_id: PageId) {
        self.last_page = page_id
    }
}

pub struct TableCatalog {
    tables: HashMap<String, TableMetadata>,
}

impl TableCatalog {
    pub fn new(page_manager: &mut PageManager) -> io::Result<Self> {
        // If this is a new database (only page 0 exists), allocate page 1 for catalog
        if page_manager.num_pages() == 1 {
            page_manager.allocate_page()?;
        }

        let catalog_data = page_manager.read_page(constants::TABLE_CATALOG_PAGE_ID)?;

        // check if catalog is empty
        if catalog_data.iter().all(|&b| b == 0) {
            // empty catalog - new db
            return Ok(TableCatalog {
                tables: HashMap::new(),
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

        // Reconstruct catalog
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

            if catalog_data.len() < offset + 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Not enough bytes for last page".to_string(),
                ));
            }

            let last_page = u32::from_le_bytes([
                catalog_data[offset],
                catalog_data[offset + 1],
                catalog_data[offset + 2],
                catalog_data[offset + 3],
            ]);
            offset += 4;

            let table_metadata = TableMetadata {
                schema,
                first_page,
                last_page,
            };

            tables.insert(
                table_metadata.schema.table_name().to_string(),
                table_metadata,
            );
        }

        Ok(TableCatalog { tables })
    }

    pub fn create_table(
        &mut self,
        schema: Schema,
        page_manager: &mut PageManager,
    ) -> io::Result<()> {
        // allocate first page for table data
        let first_page = page_manager.allocate_page()?;

        let table_name = schema.table_name().to_string();
        let table_metadata = TableMetadata {
            schema,
            first_page,
            last_page: first_page,
        };

        self.tables.insert(table_name, table_metadata);

        // save catalog
        self.save(page_manager)?;

        Ok(())
    }

    pub fn save(&mut self, page_manager: &mut PageManager) -> io::Result<()> {
        let bytes = self.to_bytes();

        // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
        // support with a page chain, similar to how table data pages are chained.
        if bytes.len() > PAGE_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "catalog exceeds page size limit",
            ));
        }
        page_manager.write_page(constants::TABLE_CATALOG_PAGE_ID, &bytes)?;
        Ok(())
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // number of tables
        bytes.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());

        for (_, metadata) in self.tables.iter() {
            let schema_bytes = metadata.schema.to_bytes();
            bytes.extend_from_slice(&schema_bytes);

            // first page
            bytes.extend_from_slice(&metadata.first_page.to_le_bytes());

            // last page
            bytes.extend_from_slice(&metadata.last_page.to_le_bytes());
        }

        bytes
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMetadata> {
        self.tables.get(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn drop_table(&mut self, name: &str, page_manager: &mut PageManager) -> io::Result<()> {
        match self.tables.remove(name) {
            Some(_) => {
                self.save(page_manager)?;
                return Ok(());
            }
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", name),
                ));
            }
        }
    }

    pub fn update_last_page(
        &mut self,
        table_name: &str,
        page_id: PageId,
        page_manager: &mut PageManager,
    ) -> io::Result<()> {
        if let Some(meta) = self.tables.get_mut(table_name) {
            meta.update_last_page(page_id);
        }

        self.save(page_manager)?;
        Ok(())
    }

    pub fn get_last_page(&self, table_name: &str) -> Option<PageId> {
        self.tables.get(table_name).map(|meta| meta.last_page())
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

        let mut pm = PageManager::new("test_new_catalog").unwrap();
        let catalog = TableCatalog::new(&mut pm).unwrap();

        assert_eq!(catalog.tables.len(), 0);

        cleanup("test_new_catalog");
    }

    #[test]
    fn test_create_single_table() {
        cleanup("test_single");

        let mut pm = PageManager::new("test_single").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let schema = Schema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer, true),
                Column::new("name", DataType::Text, false),
            ],
        )
        .unwrap();

        catalog.create_table(schema, &mut pm).unwrap();

        assert_eq!(catalog.tables.len(), 1);
        assert!(catalog.tables.contains_key("users"));
        // check fist page and last page is same for new table
        let table_meta = catalog.tables.get("users").unwrap();
        assert_eq!(table_meta.first_page(), table_meta.last_page());

        cleanup("test_single");
    }

    #[test]
    fn test_create_multiple_tables() {
        cleanup("test_multiple");

        let mut pm = PageManager::new("test_multiple").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        // Create first table
        let users_schema =
            Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap();
        catalog.create_table(users_schema, &mut pm).unwrap();

        // Create second table
        let orders_schema = Schema::new(
            "orders",
            vec![
                Column::new("id", DataType::Integer, true),
                Column::new("total", DataType::Integer, false),
            ],
        )
        .unwrap();
        catalog.create_table(orders_schema, &mut pm).unwrap();

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
            let mut pm = PageManager::new("test_persist").unwrap();
            let mut catalog = TableCatalog::new(&mut pm).unwrap();

            let schema = Schema::new(
                "users",
                vec![
                    Column::new("id", DataType::Integer, true),
                    Column::new("name", DataType::Text, false),
                ],
            )
            .unwrap();

            catalog.create_table(schema, &mut pm).unwrap();
            assert_eq!(catalog.tables.len(), 1);
        } // catalog dropped, file closed

        // Re-open and verify table still exists
        {
            let mut pm = PageManager::new("test_persist").unwrap();
            let catalog = TableCatalog::new(&mut pm).unwrap();

            assert_eq!(catalog.tables.len(), 1);
            assert!(catalog.tables.contains_key("users"));

            let metadata = catalog.get_table("users").unwrap();
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
            let mut pm = PageManager::new("test_multi_persist").unwrap();
            let mut catalog = TableCatalog::new(&mut pm).unwrap();

            catalog
                .create_table(
                    Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap(),
                    &mut pm,
                )
                .unwrap();

            catalog
                .create_table(
                    Schema::new(
                        "orders",
                        vec![
                            Column::new("id", DataType::Integer, true),
                            Column::new("user_id", DataType::Integer, false),
                        ],
                    )
                    .unwrap(),
                    &mut pm,
                )
                .unwrap();

            catalog
                .create_table(
                    Schema::new(
                        "products",
                        vec![
                            Column::new("name", DataType::Text, false),
                            Column::new("price", DataType::Integer, false),
                        ],
                    )
                    .unwrap(),
                    &mut pm,
                )
                .unwrap();
        }

        // Reload and verify all tables
        {
            let mut pm = PageManager::new("test_multi_persist").unwrap();
            let catalog = TableCatalog::new(&mut pm).unwrap();

            assert_eq!(catalog.tables.len(), 3);
            assert!(catalog.tables.contains_key("users"));
            assert!(catalog.tables.contains_key("orders"));
            assert!(catalog.tables.contains_key("products"));

            // Verify schema details
            let users = catalog.get_table("users").unwrap();
            assert_eq!(users.schema.columns().len(), 1);

            let orders = catalog.get_table("orders").unwrap();
            assert_eq!(orders.schema.columns().len(), 2);

            let products = catalog.get_table("products").unwrap();
            assert_eq!(products.schema.columns().len(), 2);
        }

        cleanup("test_multi_persist");
    }

    #[test]
    fn test_first_page_allocation() {
        cleanup("test_page_alloc");

        let mut pm = PageManager::new("test_page_alloc").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let initial_pages = pm.num_pages();

        // Create first table
        catalog
            .create_table(
                Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap(),
                &mut pm,
            )
            .unwrap();

        let users_page = catalog.get_table("users").unwrap().first_page;
        assert_eq!(users_page, initial_pages); // Should allocate next available page

        // Create second table
        catalog
            .create_table(
                Schema::new("orders", vec![Column::new("id", DataType::Integer, false)]).unwrap(),
                &mut pm,
            )
            .unwrap();

        let orders_page = catalog.get_table("orders").unwrap().first_page;
        assert_eq!(orders_page, users_page + 1); // Should allocate next page

        cleanup("test_page_alloc");
    }

    #[test]
    fn test_table_with_all_data_types() {
        cleanup("test_all_types");

        let mut pm = PageManager::new("test_all_types").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let schema = Schema::new(
            "test_table",
            vec![
                Column::new("int_col", DataType::Integer, false),
                Column::new("text_col", DataType::Text, false),
                Column::new("bool_col", DataType::Boolean, false),
                Column::new("null_col", DataType::Null, false),
            ],
        )
        .unwrap();

        catalog.create_table(schema, &mut pm).unwrap();

        // Reload and verify
        drop(pm);
        let mut pm = PageManager::new("test_all_types").unwrap();
        let catalog = TableCatalog::new(&mut pm).unwrap();

        let metadata = catalog.get_table("test_table").unwrap();
        assert_eq!(metadata.schema.columns().len(), 4);

        cleanup("test_all_types");
    }

    #[test]
    fn test_empty_table_name() {
        cleanup("test_empty_name");

        let mut pm = PageManager::new("test_empty_name").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let schema = Schema::new("", vec![Column::new("id", DataType::Integer, true)]).unwrap();

        // Should still work (validation not implemented yet)
        catalog.create_table(schema, &mut pm).unwrap();
        assert!(catalog.tables.contains_key(""));

        cleanup("test_empty_name");
    }

    #[test]
    fn test_table_with_long_name() {
        cleanup("test_long_name");

        let mut pm = PageManager::new("test_long_name").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let long_name = "a".repeat(1000);
        let schema = Schema::new(
            &long_name,
            vec![Column::new("id", DataType::Integer, false)],
        )
        .unwrap();

        catalog.create_table(schema, &mut pm).unwrap();

        // Reload and verify
        drop(pm);
        let mut pm = PageManager::new("test_long_name").unwrap();
        let catalog = TableCatalog::new(&mut pm).unwrap();

        assert!(catalog.tables.contains_key(&long_name));

        cleanup("test_long_name");
    }

    #[test]
    fn test_get_table_exists() {
        cleanup("test_get");

        let mut pm = PageManager::new("test_get").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let schema =
            Schema::new("users", vec![Column::new("id", DataType::Integer, false)]).unwrap();
        catalog.create_table(schema, &mut pm).unwrap();

        let result = catalog.get_table("users");
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema.table_name(), "users");

        cleanup("test_get");
    }

    #[test]
    fn test_get_table_not_exists() {
        cleanup("test_get_none");

        let mut pm = PageManager::new("test_get_none").unwrap();
        let catalog = TableCatalog::new(&mut pm).unwrap();

        assert!(catalog.get_table("nonexistent").is_none());

        cleanup("test_get_none");
    }

    #[test]
    fn test_list_tables() {
        cleanup("test_list");

        let mut pm = PageManager::new("test_list").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        catalog
            .create_table(Schema::new("users", vec![]).unwrap(), &mut pm)
            .unwrap();
        catalog
            .create_table(Schema::new("orders", vec![]).unwrap(), &mut pm)
            .unwrap();
        catalog
            .create_table(Schema::new("products", vec![]).unwrap(), &mut pm)
            .unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"products".to_string()));

        cleanup("test_list");
    }

    #[test]
    fn test_drop_table() {
        cleanup("test_drop");

        let mut pm = PageManager::new("test_drop").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        catalog
            .create_table(Schema::new("users", vec![]).unwrap(), &mut pm)
            .unwrap();
        catalog
            .create_table(Schema::new("orders", vec![]).unwrap(), &mut pm)
            .unwrap();

        assert_eq!(catalog.tables.len(), 2);

        catalog.drop_table("users", &mut pm).unwrap();

        assert_eq!(catalog.tables.len(), 1);
        assert!(catalog.get_table("users").is_none());
        assert!(catalog.get_table("orders").is_some());

        cleanup("test_drop");
    }

    #[test]
    fn test_drop_table_persists() {
        cleanup("test_drop_persist");

        {
            let mut pm = PageManager::new("test_drop_persist").unwrap();
            let mut catalog = TableCatalog::new(&mut pm).unwrap();

            catalog
                .create_table(Schema::new("users", vec![]).unwrap(), &mut pm)
                .unwrap();
            catalog
                .create_table(Schema::new("orders", vec![]).unwrap(), &mut pm)
                .unwrap();
            catalog.drop_table("users", &mut pm).unwrap();
        }

        // Reload and verify drop persisted
        {
            let mut pm = PageManager::new("test_drop_persist").unwrap();
            let catalog = TableCatalog::new(&mut pm).unwrap();

            assert_eq!(catalog.tables.len(), 1);
            assert!(catalog.get_table("users").is_none());
            assert!(catalog.get_table("orders").is_some());
        }

        cleanup("test_drop_persist");
    }

    #[test]
    fn test_drop_nonexistent_table() {
        cleanup("test_drop_none");

        let mut pm = PageManager::new("test_drop_none").unwrap();
        let mut catalog = TableCatalog::new(&mut pm).unwrap();

        let result = catalog.drop_table("nonexistent", &mut pm);
        assert!(result.is_err()); // Should return error

        cleanup("test_drop_none");
    }

    #[test]
    fn test_table_first_and_last_name_persists() {
        cleanup("test_table_first_and_last_name_persists");

        {
            let mut pm = PageManager::new("test_table_first_and_last_name_persists").unwrap();
            let mut catalog = TableCatalog::new(&mut pm).unwrap();

            catalog
                .create_table(Schema::new("users", vec![]).unwrap(), &mut pm)
                .unwrap();

            let table_meta = catalog.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());
        }

        // Reload and verify drop persisted
        {
            let mut pm = PageManager::new("test_table_first_and_last_name_persists").unwrap();
            let catalog = TableCatalog::new(&mut pm).unwrap();

            let table_meta = catalog.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());
        }

        cleanup("test_table_first_and_last_name_persists");
    }

    #[test]
    fn test_update_table_last_page_persists() {
        cleanup("test_update_table_last_page");
        let new_last_page = 128;

        {
            let mut pm = PageManager::new("test_update_table_last_page").unwrap();
            let mut catalog = TableCatalog::new(&mut pm).unwrap();

            catalog
                .create_table(Schema::new("users", vec![]).unwrap(), &mut pm)
                .unwrap();

            let table_meta = catalog.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());

            // update last page
            catalog
                .update_last_page("users", new_last_page, &mut pm)
                .unwrap();
            assert_eq!(catalog.get_last_page("users").unwrap(), new_last_page);
        }

        // Reload and verify drop persisted
        {
            let mut pm = PageManager::new("test_update_table_last_page").unwrap();
            let catalog = TableCatalog::new(&mut pm).unwrap();

            assert_eq!(catalog.get_last_page("users").unwrap(), new_last_page);
        }

        cleanup("test_update_table_last_page");
    }
}
