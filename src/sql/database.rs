use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    catalog::{
        index::{IndexCatalog, IndexEntry, index_entry::IndexColumnType},
        schema::Schema,
        table::{TableCatalog, TableMetadata},
    },
    constants::PageId,
    index::{btree::BPlusTree, key::IndexKey, node::leaf::RowLocation},
    storage::page::{PageManager, PageMetadata},
};

pub struct Database {
    page_manager: PageManager,
    table_catalog: TableCatalog,
    index_catalog: IndexCatalog,
    indexes: HashMap<String, BPlusTree>, // index name -> B+ tree
}

impl Database {
    pub fn new(mut page_manager: PageManager) -> io::Result<Self> {
        let table_catalog = TableCatalog::new(&mut page_manager)?;
        let index_catalog = IndexCatalog::new(&mut page_manager)?;
        let mut indexes = HashMap::new();

        // load b+ tree for all indexes
        for entry in index_catalog.all_indexes() {
            let btree = BPlusTree::load(entry.root_page_id(), entry.column_type().order());

            indexes.insert(entry.index_name().to_owned(), btree);
        }

        Ok(Database {
            page_manager,
            table_catalog,
            index_catalog,
            indexes,
        })
    }

    // page manager methods
    pub fn read_page(&self, page_id: u32) -> io::Result<[u8; 4096]> {
        self.page_manager.read_page(page_id)
    }

    pub fn write_page(&mut self, page_id: u32, data: &[u8]) -> io::Result<()> {
        self.page_manager.write_page(page_id, data)
    }

    pub fn read_page_metadata(&self, page_id: u32) -> io::Result<PageMetadata> {
        self.page_manager.read_page_metadata(page_id)
    }

    pub fn update_page_metadata(
        &mut self,
        page_id: u32,
        metadata: &PageMetadata,
    ) -> io::Result<()> {
        self.page_manager.update_page_metadata(page_id, metadata)
    }

    pub fn number_of_pages(&self) -> u32 {
        self.page_manager.num_pages()
    }

    pub fn allocate_page(&mut self) -> io::Result<PageId> {
        self.page_manager.allocate_page()
    }

    pub fn free_page(&mut self, page_id: PageId) -> io::Result<()> {
        self.page_manager.free_page(page_id)
    }

    pub fn get_first_free_page(&self) -> Option<PageId> {
        self.page_manager.first_free_page()
    }

    pub fn pm(&mut self) -> &mut PageManager {
        &mut self.page_manager
    }

    // table catalog
    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        self.table_catalog
            .create_table(schema.clone(), &mut self.page_manager)?;

        if let Some(pk_col) = schema.columns().iter().find(|c| c.is_primary_key()) {
            let column_type = IndexColumnType::try_from(*pk_col.data_type())?;
            let btree = BPlusTree::new(column_type.order(), &mut self.page_manager)?;
            let root_page_id = btree.root().ok_or_else(|| {
                return Error::new(ErrorKind::InvalidData, "b+ tree root page id not set");
            })?;

            let index_name = format!("idx-{}-{}", schema.table_name(), pk_col.name());
            let index_entry = IndexEntry::new(
                &index_name,
                schema.table_name(),
                pk_col.name(),
                column_type,
                true,
                root_page_id,
            );

            // register new index
            self.add_new_index(index_entry)?;
        }

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMetadata> {
        self.table_catalog.get_table(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.table_catalog.list_tables()
    }

    pub fn drop_table(&mut self, table_name: &str) -> io::Result<()> {
        self.table_catalog
            .drop_table(table_name, &mut self.page_manager)?;
        // drop indexes for the table
        self.index_catalog
            .remove_table_indexes(table_name, &mut self.page_manager)?;

        Ok(())
    }

    // Index catalog methods
    pub fn add_new_index(&mut self, entry: IndexEntry) -> io::Result<()> {
        let btree = BPlusTree::load(entry.root_page_id(), entry.column_type().order());
        let index_name = entry.index_name().to_owned();

        self.index_catalog
            .add_index(&mut self.page_manager, entry)?;
        self.indexes.insert(index_name, btree);

        Ok(())
    }

    pub fn get_indexes_for_table(&self, table_name: &str) -> Option<&Vec<IndexEntry>> {
        self.index_catalog.get_indexes_for_table(table_name)
    }

    pub fn get_primary_index(&self, table_name: &str) -> Option<&IndexEntry> {
        self.index_catalog.get_primary_index(table_name)
    }

    pub fn remove_index(&mut self, table_name: &str, index_name: &str) -> io::Result<()> {
        self.index_catalog
            .remove_index(table_name, index_name, &mut self.page_manager)
    }

    pub fn total_index_count(&self) -> usize {
        self.index_catalog.total_count()
    }

    // Indexes
    pub fn insert_into_index(
        &mut self,
        index_name: &str,
        key: IndexKey,
        row_location: RowLocation,
    ) -> io::Result<()> {
        let Database {
            indexes,
            page_manager,
            ..
        } = self;

        let btree = indexes.get_mut(index_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("index '{}' not found", index_name),
            )
        })?;

        btree.insert(key, row_location, page_manager)
    }

    pub fn indexes(&self) -> &HashMap<String, BPlusTree> {
        &self.indexes
    }

    pub fn search_index(
        &mut self,
        index_name: &str,
        key: &IndexKey,
    ) -> io::Result<Option<RowLocation>> {
        let Database {
            indexes,
            page_manager,
            ..
        } = self;
        let btree = indexes
            .get_mut(index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index not found"))?;
        btree.search(key, page_manager)
    }
}
