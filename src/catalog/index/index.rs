use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{catalog::index::index_entry::IndexEntry, constants, storage::page::PageManager};

pub struct IndexCatalog {
    indexes: HashMap<String, Vec<IndexEntry>>, // table -> Vec<Indexes>
}

impl IndexCatalog {
    pub fn new(page_manager: &mut PageManager) -> io::Result<Self> {
        // If this is a new database (only page 0 and 1 exists), allocate page 2 for index catalog
        if page_manager.num_pages() == 2 {
            page_manager.allocate_page()?;
        }

        let catalog_data = page_manager.read_page(constants::INDEX_CATALOG_PAGE_ID)?;

        // check if catalog is empty
        if catalog_data.iter().all(|&b| b == 0) {
            // empty catalog - new db
            return Ok(IndexCatalog {
                indexes: HashMap::new(),
            });
        }

        // parse catalog data
        let mut offset = 0;

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

        /*
        TODO-optimize;
        - do something about the double loop
        - maybe not re-allocate for each index
         */
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
        page_manager: &mut PageManager,
        entry: IndexEntry,
    ) -> io::Result<()> {
        self.indexes
            .entry(entry.table_name().to_string())
            .or_default()
            .push(entry);

        self.save(page_manager)?;
        Ok(())
    }

    fn save(&mut self, page_manager: &mut PageManager) -> io::Result<()> {
        // TODO-optimize: append instead of re-writing whole page
        let bytes = self.to_bytes();
        page_manager.write_page(constants::INDEX_CATALOG_PAGE_ID, &bytes)?;
        Ok(())
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
        page_manager: &mut PageManager,
    ) -> io::Result<()> {
        self.indexes
            .entry(table_name.to_string())
            .or_default()
            .retain(|i| i.index_name() != index_name);

        self.indexes.retain(|_, v| !v.is_empty());

        self.save(page_manager)?;
        Ok(())
    }

    pub fn total_count(&self) -> usize {
        self.indexes.values().map(|v| v.len()).sum()
    }

    pub fn remove_table_indexes(
        &mut self,
        table_name: &str,
        page_manager: &mut PageManager,
    ) -> io::Result<()> {
        self.indexes.remove(table_name);
        self.save(page_manager)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table::TableCatalog;
    use std::fs;

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    fn setup(basename: &str) -> (IndexCatalog, PageManager) {
        let mut pm = PageManager::new(&format!("{}.hdb", basename)).unwrap();
        let _ = TableCatalog::new(&mut pm).unwrap();
        let ic = IndexCatalog::new(&mut pm).unwrap();
        (ic, pm)
    }

    #[test]
    fn test_new_index_catalog() {
        cleanup("test_new_index_catalog");

        let (index_catalog, _) = setup("test_new_index_catalog");

        assert!(index_catalog.indexes.is_empty());
        assert_eq!(index_catalog.indexes.values().count(), 0);
        cleanup("test_new_index_catalog");
    }

    #[test]
    fn test_add_index() {
        cleanup("test_add_index");

        let (mut ic, mut pm) = setup("test_add_index");

        let index = IndexEntry::new("idx_users_id", "users", "id", true, 99);
        ic.add_index(&mut pm, index).unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_some());
        assert_eq!(indexes.unwrap().len(), 1);
        cleanup("test_add_index");
    }

    #[test]
    fn test_add_index_multiple_tables() {
        cleanup("test_add_index_multiple_tables");

        let (mut ic, mut pm) = setup("test_add_index_multiple_tables");

        let users_index = IndexEntry::new("idx_users_id", "users", "id", true, 99);
        let orders_index = IndexEntry::new("idx_orders_id", "orders", "id", true, 98);
        ic.add_index(&mut pm, users_index).unwrap();
        ic.add_index(&mut pm, orders_index).unwrap();

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

        let (mut ic, mut pm) = setup("test_get_primary_index");

        let index_1 = IndexEntry::new("idx_users_id", "users", "id", true, 99);
        let index_2 = IndexEntry::new("idx_users_name", "users", "name", false, 99);

        ic.add_index(&mut pm, index_1).unwrap();
        ic.add_index(&mut pm, index_2).unwrap();

        let primary = ic.get_primary_index("users");
        assert!(primary.is_some());
        assert_eq!(primary.unwrap().index_name(), "idx_users_id");
        cleanup("test_get_primary_index");
    }

    #[test]
    fn test_get_primary_index_none() {
        cleanup("test_get_primary_index_none");

        let (mut ic, mut pm): (IndexCatalog, PageManager) = setup("test_get_primary_index_none");

        let index = IndexEntry::new("idx_users_id", "users", "id", false, 99);

        ic.add_index(&mut pm, index).unwrap();

        assert!(ic.get_primary_index("users").is_none());
        cleanup("test_get_primary_index_none");
    }

    #[test]
    fn test_remove_index() {
        cleanup("test_remove_index");

        let (mut ic, mut pm) = setup("test_remove_index");

        let index = IndexEntry::new("idx_users_id", "users", "id", true, 99);

        ic.add_index(&mut pm, index).unwrap();
        ic.remove_index("users", "idx_users_id", &mut pm).unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_none());
        cleanup("test_remove_index");
    }

    #[test]
    fn test_catalog_persists_across_reopen() {
        cleanup("test_catalog_persist");

        {
            let (mut ic, mut pm) = setup("test_catalog_persist");

            let index_1 = IndexEntry::new("idx_users_id", "users", "id", true, 99);
            let index_2 = IndexEntry::new("idx_users_email", "users", "email", false, 98);
            ic.add_index(&mut pm, index_1).unwrap();
            ic.add_index(&mut pm, index_2).unwrap();

            let indexes = ic.get_indexes_for_table("users");
            assert!(indexes.is_some());
            assert_eq!(indexes.unwrap().len(), 2);
        }

        {
            let (ic, _) = setup("test_catalog_persist");

            let indexes = ic.get_indexes_for_table("users");
            assert!(indexes.is_some());
            assert_eq!(indexes.unwrap().len(), 2);
        }

        cleanup("test_catalog_persist");
    }

    #[test]
    fn test_remove_table_indexes() {
        cleanup("test_remove_table");
        let (mut ic, mut pm) = setup("test_remove_table");

        let index_1 = IndexEntry::new("idx_users_id", "users", "id", true, 99);
        let index_2 = IndexEntry::new("idx_users_email", "users", "email", false, 98);

        ic.add_index(&mut pm, index_1).unwrap();
        ic.add_index(&mut pm, index_2).unwrap();

        ic.remove_table_indexes("users", &mut pm).unwrap();

        let indexes = ic.get_indexes_for_table("users");
        assert!(indexes.is_none());

        cleanup("test_remove_table");
    }
}
