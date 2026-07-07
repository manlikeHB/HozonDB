use crate::catalog::schema::Schema;
use crate::constants::{self, Lsn, PageId, TxnId};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::PAGE_SIZE;
use crate::wal::record_type::WalRecordType;
use crate::wal::writer::WalWriter;
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
    pub fn new(buffer_pool: &mut BufferPool, wal_writer: &mut WalWriter) -> io::Result<Self> {
        // If this is a new database (only page 0 exists), allocate page 1 for catalog
        if buffer_pool.total_num_of_db_pages() == 1 {
            // This is only on start up when there no Transactions yet
            // hence 0 as the txn_id
            buffer_pool.allocate_raw_page(wal_writer, 0)?;
        }

        let catalog_data = buffer_pool.read_page(constants::TABLE_CATALOG_PAGE_ID)?;

        // check if catalog is empty
        if catalog_data[constants::OFFSET_RAW_PAGE_START..]
            .iter()
            .all(|&b| b == 0)
        {
            // empty catalog - new db
            return Ok(TableCatalog {
                tables: HashMap::new(),
            });
        }

        // parse catalog data
        let mut offset = constants::OFFSET_RAW_PAGE_START;

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

    // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
    // support with a page chain, similar to how table data pages are chained.
    pub fn create_table(
        &mut self,
        schema: Schema,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<Vec<(Lsn, u64)>> {
        let (first_page, lsn1, wal_offset1) =
            buffer_pool.allocate_slotted_page(wal_writer, txn_id)?;

        let table_name = schema.table_name().to_string();
        let table_metadata = TableMetadata {
            schema,
            first_page,
            last_page: first_page,
        };
        self.tables.insert(table_name, table_metadata);

        let table_catalog = self.to_bytes();

        // read old_data from CATALOG page
        let old_data = buffer_pool.read_page(constants::TABLE_CATALOG_PAGE_ID)?;

        let mut new_data = old_data.clone();
        new_data[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + table_catalog.len()]
            .copy_from_slice(&table_catalog);
        new_data[constants::OFFSET_RAW_PAGE_START + table_catalog.len()..].fill(0);

        // log to WAL
        let (lsn2, wal_offset2) = wal_writer.append_raw(
            WalRecordType::CreateTable,
            constants::TABLE_CATALOG_PAGE_ID,
            &new_data,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_data, lsn2)?;
        Ok(vec![(lsn1, wal_offset1), (lsn2, wal_offset2)])
    }

    pub fn save(
        &mut self,
        buffer_pool: &mut BufferPool,
        new_data: &[u8; PAGE_SIZE],
        lsn: u64,
    ) -> io::Result<()> {
        buffer_pool.write_raw_page(constants::TABLE_CATALOG_PAGE_ID, new_data, lsn)?;
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

    pub fn drop_table(
        &mut self,
        name: &str,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: TxnId,
    ) -> io::Result<(Lsn, u64)> {
        match self.tables.remove(name) {
            Some(_) => {
                let table_catalog = self.to_bytes();

                let old_data = buffer_pool.read_page(constants::TABLE_CATALOG_PAGE_ID)?;

                let mut new_data = old_data.clone();
                new_data[constants::OFFSET_RAW_PAGE_START
                    ..constants::OFFSET_RAW_PAGE_START + table_catalog.len()]
                    .copy_from_slice(&table_catalog);
                new_data[constants::OFFSET_RAW_PAGE_START + table_catalog.len()..].fill(0);

                // log to WAL
                let (lsn, wal_offset) = wal_writer.append_raw(
                    WalRecordType::DropTable,
                    constants::TABLE_CATALOG_PAGE_ID,
                    &new_data,
                    old_data,
                    txn_id,
                )?;

                self.save(buffer_pool, &new_data, lsn)?;
                return Ok((lsn, wal_offset));
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
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        if let Some(meta) = self.tables.get_mut(table_name) {
            meta.update_last_page(page_id);
        }

        let table_catalog = self.to_bytes();

        let old_data = buffer_pool.read_page(constants::TABLE_CATALOG_PAGE_ID)?;

        let mut new_data = old_data.clone();
        new_data[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + table_catalog.len()]
            .copy_from_slice(&table_catalog);
        new_data[constants::OFFSET_RAW_PAGE_START + table_catalog.len()..].fill(0);

        // log to WAL
        let (lsn, wal_offset) = wal_writer.append_raw(
            WalRecordType::UpdateLastPage,
            constants::TABLE_CATALOG_PAGE_ID,
            &new_data,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_data, lsn)?;
        Ok((lsn, wal_offset))
    }

    pub fn get_last_page(&self, table_name: &str) -> Option<PageId> {
        self.tables.get(table_name).map(|meta| meta.last_page())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::schema::{Column, DataType, Schema},
        storage::page::PageManager,
        test_helpers::*,
    };

    fn setup(db_name: &str) -> (TableCatalog, BufferPool, WalWriter) {
        let pm = PageManager::new(db_name).unwrap();
        let mut buffer_pool = BufferPool::new(pm, 5).unwrap();
        let mut wal_writer = WalWriter::new(db_name).unwrap();
        let catalog = TableCatalog::new(&mut buffer_pool, &mut wal_writer).unwrap();
        (catalog, buffer_pool, wal_writer)
    }

    const TEST_TXN_ID: u64 = 1;

    #[test]
    fn test_new_catalog_empty() {
        cleanup("test_new_catalog");

        let (catalog, _, _) = setup("test_new_catalog");

        assert_eq!(catalog.tables.len(), 0);

        cleanup("test_new_catalog");
    }

    #[test]
    fn test_create_single_table() {
        cleanup("test_single");

        let (mut tc, mut bp, mut ww) = setup("test_single");

        let schema = Schema::new(
            "users",
            vec![
                Column::new("id", DataType::Integer, true),
                Column::new("name", DataType::Text, false),
            ],
        )
        .unwrap();

        tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        assert_eq!(tc.tables.len(), 1);
        assert!(tc.tables.contains_key("users"));
        // check fist page and last page is same for new table
        let table_meta = tc.tables.get("users").unwrap();
        assert_eq!(table_meta.first_page(), table_meta.last_page());

        cleanup("test_single");
    }

    #[test]
    fn test_create_multiple_tables() {
        cleanup("test_multiple");

        let (mut tc, mut bp, mut ww) = setup("test_multiple");

        // Create first table
        let users_schema =
            Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap();
        tc.create_table(users_schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        // Create second table
        let orders_schema = Schema::new(
            "orders",
            vec![
                Column::new("id", DataType::Integer, true),
                Column::new("total", DataType::Integer, false),
            ],
        )
        .unwrap();
        tc.create_table(orders_schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        assert_eq!(tc.tables.len(), 2);
        assert!(tc.tables.contains_key("users"));
        assert!(tc.tables.contains_key("orders"));

        cleanup("test_multiple");
    }

    #[test]
    fn test_catalog_persistence() {
        cleanup("test_persist");

        // Create catalog and add table
        {
            let (mut tc, mut bp, mut ww) = setup("test_persist");

            let schema = Schema::new(
                "users",
                vec![
                    Column::new("id", DataType::Integer, true),
                    Column::new("name", DataType::Text, false),
                ],
            )
            .unwrap();

            tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
                .unwrap();
            assert_eq!(tc.tables.len(), 1);
            bp.flush_dirty().unwrap();
        } // catalog dropped, file closed

        // Re-open and verify table still exists
        {
            let (tc, _, _) = setup("test_persist");

            assert_eq!(tc.tables.len(), 1);
            assert!(tc.tables.contains_key("users"));

            let metadata = tc.get_table("users").unwrap();
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
            let (mut tc, mut bp, mut ww) = setup("test_multi_persist");

            tc.create_table(
                Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();

            tc.create_table(
                Schema::new(
                    "orders",
                    vec![
                        Column::new("id", DataType::Integer, true),
                        Column::new("user_id", DataType::Integer, false),
                    ],
                )
                .unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();

            tc.create_table(
                Schema::new(
                    "products",
                    vec![
                        Column::new("name", DataType::Text, false),
                        Column::new("price", DataType::Integer, false),
                    ],
                )
                .unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();
            bp.flush_dirty().unwrap();
        }

        // Reload and verify all tables
        {
            let (tc, _, _) = setup("test_multi_persist");

            assert_eq!(tc.tables.len(), 3);
            assert!(tc.tables.contains_key("users"));
            assert!(tc.tables.contains_key("orders"));
            assert!(tc.tables.contains_key("products"));

            // Verify schema details
            let users = tc.get_table("users").unwrap();
            assert_eq!(users.schema.columns().len(), 1);

            let orders = tc.get_table("orders").unwrap();
            assert_eq!(orders.schema.columns().len(), 2);

            let products = tc.get_table("products").unwrap();
            assert_eq!(products.schema.columns().len(), 2);
        }

        cleanup("test_multi_persist");
    }

    #[test]
    fn test_first_page_allocation() {
        cleanup("test_page_alloc");

        let (mut tc, mut bp, mut ww) = setup("test_page_alloc");

        let initial_pages = bp.total_num_of_db_pages();

        // Create first table
        tc.create_table(
            Schema::new("users", vec![Column::new("id", DataType::Integer, true)]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();

        let users_page = tc.get_table("users").unwrap().first_page;
        assert_eq!(users_page, initial_pages); // Should allocate next available page

        // Create second table
        tc.create_table(
            Schema::new("orders", vec![Column::new("id", DataType::Integer, false)]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();

        let orders_page = tc.get_table("orders").unwrap().first_page;
        assert_eq!(orders_page, users_page + 1); // Should allocate next page

        cleanup("test_page_alloc");
    }

    #[test]
    fn test_table_with_all_data_types() {
        cleanup("test_all_types");

        let (mut tc, mut bp, mut ww) = setup("test_all_types");

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

        tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();
        bp.flush_dirty().unwrap();

        // Reload and verify
        drop(bp);

        let (tc, _, _) = setup("test_all_types");

        let metadata = tc.get_table("test_table").unwrap();
        assert_eq!(metadata.schema.columns().len(), 4);

        cleanup("test_all_types");
    }

    #[test]
    fn test_empty_table_name() {
        cleanup("test_empty_name");

        let (mut tc, mut bp, mut ww) = setup("test_empty_name");

        let schema = Schema::new("", vec![Column::new("id", DataType::Integer, true)]).unwrap();

        // Should still work (validation not implemented yet)
        tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();
        assert!(tc.tables.contains_key(""));

        cleanup("test_empty_name");
    }

    #[test]
    fn test_table_with_long_name() {
        cleanup("test_long_name");

        let (mut tc, mut bp, mut ww) = setup("test_long_name");

        let long_name = "a".repeat(1000);
        let schema = Schema::new(
            &long_name,
            vec![Column::new("id", DataType::Integer, false)],
        )
        .unwrap();

        tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();
        bp.flush_dirty().unwrap();

        // Reload and verify
        drop(bp);

        let (tc, _, _) = setup("test_long_name");

        assert!(tc.tables.contains_key(&long_name));

        cleanup("test_long_name");
    }

    #[test]
    fn test_get_table_exists() {
        cleanup("test_get");

        let (mut tc, mut bp, mut ww) = setup("test_get");

        let schema =
            Schema::new("users", vec![Column::new("id", DataType::Integer, false)]).unwrap();
        tc.create_table(schema, &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        let result = tc.get_table("users");
        assert!(result.is_some());
        assert_eq!(result.unwrap().schema.table_name(), "users");

        cleanup("test_get");
    }

    #[test]
    fn test_get_table_not_exists() {
        cleanup("test_get_none");

        let (tc, _, _) = setup("test_get_none");

        assert!(tc.get_table("nonexistent").is_none());

        cleanup("test_get_none");
    }

    #[test]
    fn test_list_tables() {
        cleanup("test_list");

        let (mut tc, mut bp, mut ww) = setup("test_list");

        tc.create_table(
            Schema::new("users", vec![]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();
        tc.create_table(
            Schema::new("orders", vec![]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();
        tc.create_table(
            Schema::new("products", vec![]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();

        let tables = tc.list_tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"products".to_string()));

        cleanup("test_list");
    }

    #[test]
    fn test_drop_table() {
        cleanup("test_drop");

        let (mut tc, mut bp, mut ww) = setup("test_drop");

        tc.create_table(
            Schema::new("users", vec![]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();
        tc.create_table(
            Schema::new("orders", vec![]).unwrap(),
            &mut bp,
            &mut ww,
            TEST_TXN_ID,
        )
        .unwrap();

        assert_eq!(tc.tables.len(), 2);

        tc.drop_table("users", &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        assert_eq!(tc.tables.len(), 1);
        assert!(tc.get_table("users").is_none());
        assert!(tc.get_table("orders").is_some());

        cleanup("test_drop");
    }

    #[test]
    fn test_drop_table_persists() {
        cleanup("test_drop_persist");

        {
            let (mut tc, mut bp, mut ww) = setup("test_drop_persist");

            tc.create_table(
                Schema::new("users", vec![]).unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();
            tc.create_table(
                Schema::new("orders", vec![]).unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();
            tc.drop_table("users", &mut bp, &mut ww, TEST_TXN_ID)
                .unwrap();
            bp.flush_dirty().unwrap();
        }

        // Reload and verify drop persisted
        {
            let (tc, _, _) = setup("test_drop_persist");

            assert_eq!(tc.tables.len(), 1);
            assert!(tc.get_table("users").is_none());
            assert!(tc.get_table("orders").is_some());
        }

        cleanup("test_drop_persist");
    }

    #[test]
    fn test_drop_nonexistent_table() {
        cleanup("test_drop_none");

        let (mut tc, mut bp, mut ww) = setup("test_drop_none");

        let result = tc.drop_table("nonexistent", &mut bp, &mut ww, TEST_TXN_ID);
        assert!(result.is_err()); // Should return error

        cleanup("test_drop_none");
    }

    #[test]
    fn test_table_first_and_last_name_persists() {
        cleanup("test_table_first_and_last_name_persists");

        {
            let (mut tc, mut bp, mut ww) = setup("test_table_first_and_last_name_persists");

            tc.create_table(
                Schema::new("users", vec![]).unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();

            let table_meta = tc.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());
            bp.flush_dirty().unwrap();
        }

        // Reload and verify drop persisted
        {
            let (tc, _, _) = setup("test_table_first_and_last_name_persists");

            let table_meta = tc.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());
        }

        cleanup("test_table_first_and_last_name_persists");
    }

    #[test]
    fn test_update_table_last_page_persists() {
        cleanup("test_update_table_last_page");
        let new_last_page = 128;

        {
            let (mut tc, mut bp, mut ww) = setup("test_update_table_last_page");

            tc.create_table(
                Schema::new("users", vec![]).unwrap(),
                &mut bp,
                &mut ww,
                TEST_TXN_ID,
            )
            .unwrap();

            let table_meta = tc.tables.get("users").unwrap();
            assert_eq!(table_meta.first_page(), table_meta.last_page());

            // update last page
            tc.update_last_page("users", new_last_page, &mut bp, &mut ww, TEST_TXN_ID)
                .unwrap();
            assert_eq!(tc.get_last_page("users").unwrap(), new_last_page);
            bp.flush_dirty().unwrap();
        }

        // Reload and verify drop persisted
        {
            let (tc, _, _) = setup("test_update_table_last_page");

            assert_eq!(tc.get_last_page("users").unwrap(), new_last_page);
        }

        cleanup("test_update_table_last_page");
    }
}
