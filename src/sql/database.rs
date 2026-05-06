use std::io;

use crate::{
    catalog::{
        index::{IndexCatalog, IndexEntry},
        schema::Schema,
        table::{TableCatalog, TableMetadata},
    },
    storage::page::{PageId, PageManager, PageMetadata},
};

pub struct Database {
    page_manager: PageManager,
    table_catalog: TableCatalog,
    index_catalog: IndexCatalog,
}

impl Database {
    pub fn new(mut page_manager: PageManager) -> io::Result<Self> {
        let table_catalog = TableCatalog::new(&mut page_manager)?;
        let index_catalog = IndexCatalog::new(&mut page_manager)?;
        Ok(Database {
            page_manager,
            table_catalog,
            index_catalog,
        })
    }

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

    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        self.table_catalog
            .create_table(schema, &mut self.page_manager)?;
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

    pub fn add_new_index(&mut self, entry: IndexEntry) -> io::Result<()> {
        self.index_catalog.add_index(&mut self.page_manager, entry)
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
}
