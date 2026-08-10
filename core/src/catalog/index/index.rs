use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    catalog::index::index_entry::IndexEntry,
    constants::{self, Lsn, SYSTEM_TXN_ID},
    storage::{buffer_pool::BufferPool, page::PAGE_SIZE},
    wal::{record_type::WalRecordType, writer::WalWriter},
};

pub struct IndexCatalog {
    indexes: HashMap<String, Vec<IndexEntry>>, // table -> Vec<Indexes>
}

impl IndexCatalog {
    pub fn new(buffer_pool: &mut BufferPool, wal_writer: &mut WalWriter) -> io::Result<Self> {
        // If this is a new database (only page 0 and 1 exists), allocate page 2 for index catalog
        if buffer_pool.total_num_of_db_pages() == 2 {
            // This is only on start up when there no Transactions yet
            // hence 0 (non-valid) txn_id
            buffer_pool.allocate_raw_page(wal_writer, SYSTEM_TXN_ID)?;
            wal_writer.append_commit_txn(SYSTEM_TXN_ID)?;
        }

        Self::load(buffer_pool)
    }

    /// Load Index Catalog from Buffer pool
    pub fn load(buffer_pool: &mut BufferPool) -> io::Result<Self> {
        let catalog_data = buffer_pool.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        // check if catalog is empty
        if catalog_data[constants::OFFSET_RAW_PAGE_START..]
            .iter()
            .all(|&b| b == 0)
        {
            // empty catalog - new db
            return Ok(IndexCatalog {
                indexes: HashMap::new(),
            });
        }

        // parse catalog data
        let mut offset = constants::OFFSET_RAW_PAGE_START;

        if catalog_data.len() < 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for number of index entry".to_string(),
            ));
        }

        // Reconstruct index catalog
        let num_index_entry = u32::from_le_bytes([
            catalog_data[offset],
            catalog_data[offset + 1],
            catalog_data[offset + 2],
            catalog_data[offset + 3],
        ]) as usize;
        offset += 4;

        let mut indexes: HashMap<String, Vec<IndexEntry>> = HashMap::new();

        for _ in 0..num_index_entry {
            let (index_entry, bytes_consumed) = IndexEntry::from_bytes(&catalog_data[offset..])?;
            offset += bytes_consumed;

            indexes
                .entry(index_entry.table_name().to_owned())
                .or_default()
                .push(index_entry);
        }

        Ok(IndexCatalog { indexes })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // index count
        bytes.extend_from_slice(&(self.total_count() as u32).to_le_bytes());

        for (_, indexes_vec) in self.indexes.iter() {
            for index in indexes_vec {
                let index_byte = index.to_bytes();

                bytes.extend_from_slice(&index_byte);
            }
        }

        bytes
    }

    pub fn add_index(
        &mut self,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        entry: IndexEntry,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let old_data = buffer_pool.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        self.indexes
            .entry(entry.table_name().to_string())
            .or_default()
            .push(entry);

        let catalog_bytes = self.to_bytes();

        // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
        // support with a page chain, similar to how table data pages are chained.
        if catalog_bytes.len() > PAGE_SIZE - constants::OFFSET_RAW_PAGE_START {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "catalog exceeds page size limit",
            ));
        }

        // build full new page so old_data and new_data are the same unit
        let mut new_page = old_data.clone();
        new_page[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()]
            .copy_from_slice(&catalog_bytes);
        new_page[constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()..].fill(0);

        let (lsn, wal_offset) = wal_writer.append_raw(
            WalRecordType::AddIndex,
            constants::INDEX_CATALOG_PAGE_ID,
            &new_page,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_page, lsn)?;
        Ok((lsn, wal_offset))
    }

    fn save(
        &mut self,
        buffer_pool: &mut BufferPool,
        new_data: &[u8; PAGE_SIZE],
        lsn: u64,
    ) -> io::Result<()> {
        buffer_pool.write_raw_page(constants::INDEX_CATALOG_PAGE_ID, new_data, lsn)
    }

    pub fn get_indexes_for_table(&self, table_name: &str) -> Option<&Vec<IndexEntry>> {
        self.indexes.get(table_name)
    }

    pub fn get_primary_index(&self, table_name: &str) -> Option<&IndexEntry> {
        if let Some(indexes) = self.indexes.get(table_name) {
            for index in indexes {
                if index.is_primary() {
                    return Some(index);
                }
            }
        }

        None
    }

    pub fn remove_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let old_data = buffer_pool.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        self.indexes
            .entry(table_name.to_string())
            .or_default()
            .retain(|i| i.index_name() != index_name);

        self.indexes.retain(|_, v| !v.is_empty());

        let catalog_bytes = self.to_bytes();

        // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
        // support with a page chain, similar to how table data pages are chained.
        if catalog_bytes.len() > PAGE_SIZE - constants::OFFSET_RAW_PAGE_START {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "catalog exceeds page size limit",
            ));
        }

        // build full new page so old_data and new_data are the same unit
        let mut new_page = old_data.clone();
        new_page[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()]
            .copy_from_slice(&catalog_bytes);
        new_page[constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()..].fill(0);

        let (lsn, wal_offset) = wal_writer.append_raw(
            WalRecordType::RemoveIndex,
            constants::INDEX_CATALOG_PAGE_ID,
            &new_page,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_page, lsn)?;
        Ok((lsn, wal_offset))
    }

    /// Updates the root page id for a named index and persists the change.
    ///
    /// Called whenever a BPlusTree's root changes (first insert, or a root
    /// split) so IndexCatalog stays the durable source of truth for where
    /// each index's root actually lives — otherwise a restart reloads the
    /// stale root and silently loses everything past the split.
    pub fn update_root_page_id(
        &mut self,
        table_name: &str,
        index_name: &str,
        new_root_page_id: u32,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let old_data = buffer_pool.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        if let Some(entries) = self.indexes.get_mut(table_name) {
            if let Some(entry) = entries.iter_mut().find(|e| e.index_name() == index_name) {
                entry.set_root_page_id(new_root_page_id);
            }
        }

        let catalog_bytes = self.to_bytes();

        // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
        // support with a page chain, similar to how table data pages are chained.
        if catalog_bytes.len() > PAGE_SIZE - constants::OFFSET_RAW_PAGE_START {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "catalog exceeds page size limit",
            ));
        }

        // build full new page so old_data and new_data are the same unit
        let mut new_page = old_data.clone();
        new_page[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()]
            .copy_from_slice(&catalog_bytes);
        new_page[constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()..].fill(0);

        let (lsn, wal_offset) = wal_writer.append_raw(
            WalRecordType::UpdateIndexRoot,
            constants::INDEX_CATALOG_PAGE_ID,
            &new_page,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_page, lsn)?;
        Ok((lsn, wal_offset))
    }

    pub fn total_count(&self) -> usize {
        self.indexes.values().map(|v| v.len()).sum()
    }

    pub fn remove_table_indexes(
        &mut self,
        table_name: &str,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let old_data = buffer_pool.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        self.indexes.remove(table_name);

        let catalog_bytes = self.to_bytes();

        // TODO: catalog is limited to a single 4KB page. Needs multi-page catalog
        // support with a page chain, similar to how table data pages are chained.
        if catalog_bytes.len() > PAGE_SIZE - constants::OFFSET_RAW_PAGE_START {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "catalog exceeds page size limit",
            ));
        }

        // build full new page so old_data and new_data are the same unit
        let mut new_page = old_data.clone();
        new_page[constants::OFFSET_RAW_PAGE_START
            ..constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()]
            .copy_from_slice(&catalog_bytes);
        new_page[constants::OFFSET_RAW_PAGE_START + catalog_bytes.len()..].fill(0);

        let (lsn, wal_offset) = wal_writer.append_raw(
            WalRecordType::RemoveIndex,
            constants::INDEX_CATALOG_PAGE_ID,
            &new_page,
            old_data,
            txn_id,
        )?;

        self.save(buffer_pool, &new_page, lsn)?;
        Ok((lsn, wal_offset))
    }

    pub fn all_indexes(&self) -> Vec<&IndexEntry> {
        self.indexes.values().flatten().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{index::index_entry::IndexColumnType, table::TableCatalog},
        storage::page::PageManager,
        test_helpers::*,
    };

    fn setup(basename: &str) -> (IndexCatalog, BufferPool, WalWriter) {
        let pm = PageManager::new(basename).unwrap();
        let mut wal_writer = WalWriter::new(basename).unwrap();
        let mut buffer_pool = BufferPool::new(pm, 5).unwrap();
        let _ = TableCatalog::new(&mut buffer_pool, &mut wal_writer).unwrap();
        let ic = IndexCatalog::new(&mut buffer_pool, &mut wal_writer).unwrap();
        (ic, buffer_pool, wal_writer)
    }

    const TEST_TXN_ID: u64 = 1;

    #[test]
    fn test_new_index_catalog() {
        cleanup("test_new_index_catalog");

        let (index_catalog, _, _) = setup("test_new_index_catalog");

        assert!(index_catalog.indexes.is_empty());
        assert_eq!(index_catalog.indexes.values().count(), 0);
        cleanup("test_new_index_catalog");
    }

    #[test]
    fn test_add_index() {
        cleanup("test_add_index");

        let (mut ic, mut bp, mut ww) = setup("test_add_index");

        let index = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );
        ic.add_index(&mut bp, &mut ww, index, TEST_TXN_ID).unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_some());
        assert_eq!(indexes.unwrap().len(), 1);
        cleanup("test_add_index");
    }

    #[test]
    fn test_add_index_multiple_tables() {
        cleanup("test_add_index_multiple_tables");

        let (mut ic, mut bp, mut ww) = setup("test_add_index_multiple_tables");

        let users_index = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );
        let orders_index = IndexEntry::new(
            "idx_orders_id",
            "orders",
            "id",
            IndexColumnType::Integer,
            true,
            98,
        );
        ic.add_index(&mut bp, &mut ww, users_index, TEST_TXN_ID)
            .unwrap();
        ic.add_index(&mut bp, &mut ww, orders_index, TEST_TXN_ID)
            .unwrap();

        let users_indexes = ic.get_indexes_for_table("users");
        let orders_indexes = ic.get_indexes_for_table("orders");
        assert!(users_indexes.is_some());
        assert_eq!(users_indexes.unwrap().len(), 1);
        assert!(orders_indexes.is_some());
        assert_eq!(orders_indexes.unwrap().len(), 1);
        cleanup("test_add_index_multiple_tables");
    }

    #[test]
    fn test_get_primary_index() {
        cleanup("test_get_primary_index");

        let (mut ic, mut bp, mut ww) = setup("test_get_primary_index");

        let index_1 = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );
        let index_2 = IndexEntry::new(
            "idx_users_name",
            "users",
            "name",
            IndexColumnType::Text,
            false,
            99,
        );

        ic.add_index(&mut bp, &mut ww, index_1, TEST_TXN_ID)
            .unwrap();
        ic.add_index(&mut bp, &mut ww, index_2, TEST_TXN_ID)
            .unwrap();

        let primary = ic.get_primary_index("users");
        assert!(primary.is_some());
        assert_eq!(primary.unwrap().index_name(), "idx_users_id");
        cleanup("test_get_primary_index");
    }

    #[test]
    fn test_get_primary_index_none() {
        cleanup("test_get_primary_index_none");

        let (mut ic, mut bp, mut ww) = setup("test_get_primary_index_none");

        let index = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            false,
            99,
        );

        ic.add_index(&mut bp, &mut ww, index, TEST_TXN_ID).unwrap();

        assert!(ic.get_primary_index("users").is_none());
        cleanup("test_get_primary_index_none");
    }

    #[test]
    fn test_remove_index() {
        cleanup("test_remove_index");

        let (mut ic, mut bp, mut ww) = setup("test_remove_index");

        let index = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );

        ic.add_index(&mut bp, &mut ww, index, TEST_TXN_ID).unwrap();
        ic.remove_index("users", "idx_users_id", &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_none());
        cleanup("test_remove_index");
    }

    #[test]
    fn test_catalog_persists_across_reopen() {
        cleanup("test_catalog_persist");

        {
            let (mut ic, mut bp, mut ww) = setup("test_catalog_persist");

            let index_1 = IndexEntry::new(
                "idx_users_id",
                "users",
                "id",
                IndexColumnType::Integer,
                true,
                99,
            );
            let index_2 = IndexEntry::new(
                "idx_users_email",
                "users",
                "email",
                IndexColumnType::Integer,
                false,
                98,
            );
            ic.add_index(&mut bp, &mut ww, index_1, TEST_TXN_ID)
                .unwrap();
            ic.add_index(&mut bp, &mut ww, index_2, TEST_TXN_ID)
                .unwrap();

            let indexes = ic.get_indexes_for_table("users");
            assert!(indexes.is_some());
            assert_eq!(indexes.unwrap().len(), 2);
            bp.flush_dirty().unwrap();
        }

        {
            let (ic, _, _) = setup("test_catalog_persist");

            let indexes = ic.get_indexes_for_table("users");
            assert!(indexes.is_some());
            assert_eq!(indexes.unwrap().len(), 2);
        }

        cleanup("test_catalog_persist");
    }

    #[test]
    fn test_remove_table_indexes() {
        cleanup("test_remove_table");
        let (mut ic, mut bp, mut ww) = setup("test_remove_table");

        let index_1 = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );
        let index_2 = IndexEntry::new(
            "idx_users_email",
            "users",
            "email",
            IndexColumnType::Text,
            false,
            98,
        );

        ic.add_index(&mut bp, &mut ww, index_1, TEST_TXN_ID)
            .unwrap();
        ic.add_index(&mut bp, &mut ww, index_2, TEST_TXN_ID)
            .unwrap();

        ic.remove_table_indexes("users", &mut bp, &mut ww, TEST_TXN_ID)
            .unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_none());

        cleanup("test_remove_table");
    }

    #[test]
    fn test_all_indexes() {
        cleanup("test_all_indexes");
        let (mut ic, mut bp, mut ww) = setup("test_all_indexes");

        let index_1 = IndexEntry::new(
            "idx_users_id",
            "users",
            "id",
            IndexColumnType::Integer,
            true,
            99,
        );
        let index_2 = IndexEntry::new(
            "idx_users_email",
            "users",
            "email",
            IndexColumnType::Text,
            false,
            98,
        );
        let index_3 = IndexEntry::new(
            "idx_orders_id",
            "orders",
            "id",
            IndexColumnType::Integer,
            false,
            97,
        );

        ic.add_index(&mut bp, &mut ww, index_1, TEST_TXN_ID)
            .unwrap();
        ic.add_index(&mut bp, &mut ww, index_2, TEST_TXN_ID)
            .unwrap();
        ic.add_index(&mut bp, &mut ww, index_3, TEST_TXN_ID)
            .unwrap();

        assert_eq!(ic.total_count(), ic.all_indexes().len());

        cleanup("test_all_indexes");
    }
}
