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
    constants::{self, PageId},
    index::{btree::BPlusTree, key::IndexKey, node::leaf::RowLocation},
    sql::{executor::helpers, parser::BinaryOperator},
    storage::{
        buffer_pool::BufferPool,
        page::{PAGE_SIZE, PageManager, PageMetadata, PageType},
    },
    wal::{record_type::WalRecordType, writer::WalWriter},
};

pub struct Database {
    table_catalog: TableCatalog,
    index_catalog: IndexCatalog,
    indexes: HashMap<String, BPlusTree>, // index name -> B+ tree
    buffer_pool: BufferPool,
    wal_writer: WalWriter,
}

impl Database {
    pub fn new(db_name: &str) -> io::Result<Self> {
        let page_manager = PageManager::new(db_name)?;
        let mut buffer_pool = BufferPool::new(page_manager, constants::BUFFER_POOL_CAPACITY);

        let table_catalog = TableCatalog::new(&mut buffer_pool)?;
        let index_catalog = IndexCatalog::new(&mut buffer_pool)?;
        let mut indexes = HashMap::new();

        let wal_writer = WalWriter::new(db_name)?;

        // load b+ tree for all indexes
        for entry in index_catalog.all_indexes() {
            let btree = BPlusTree::load(entry.root_page_id(), entry.column_type().order());

            indexes.insert(entry.index_name().to_owned(), btree);
        }

        Ok(Database {
            table_catalog,
            index_catalog,
            indexes,
            buffer_pool,
            wal_writer,
        })
    }

    // Buffer pool methods
    pub fn read_page(&mut self, page_id: u32) -> io::Result<&[u8; PAGE_SIZE]> {
        self.buffer_pool.read_page(page_id)
    }

    pub fn get_page_mut(&mut self, page_id: u32) -> io::Result<&mut [u8; PAGE_SIZE]> {
        self.buffer_pool.get_page_mut(page_id)
    }

    pub fn mark_dirty(&mut self, page_id: PageId, lsn: u64) -> io::Result<()> {
        self.buffer_pool.mark_dirty(page_id, lsn)
    }

    pub fn read_page_metadata(
        &mut self,
        page_id: u32,
        page_type: PageType,
    ) -> io::Result<PageMetadata> {
        self.buffer_pool.read_page_metadata(page_id, page_type)
    }

    pub fn update_page_metadata(
        &mut self,
        page_id: u32,
        metadata: &PageMetadata,
    ) -> io::Result<()> {
        self.buffer_pool.update_page_metadata(page_id, metadata)
    }

    pub fn free_page(&mut self, page_id: PageId) -> io::Result<()> {
        self.buffer_pool.free_page(page_id, &mut self.wal_writer)
    }

    // table catalog
    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        self.table_catalog.create_table(
            schema.clone(),
            &mut self.buffer_pool,
            &mut self.wal_writer,
        )?;

        if let Some(pk_col) = schema.columns().iter().find(|c| c.is_primary_key()) {
            let column_type = IndexColumnType::try_from(*pk_col.data_type())?;
            let btree = BPlusTree::new(
                column_type.order(),
                &mut self.buffer_pool,
                &mut self.wal_writer,
            )?;
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

    // TODO: drop_table does not free B+ tree index pages — only the index
    // catalog entry is removed. Index pages are orphaned on disk until
    // the free list reclaims them.
    // Fix: implement BPlusTree::all_page_ids() that traverses the full
    // tree and returns every node's page ID, then call
    // buffer_pool.free_page() for each in drop_table.
    pub fn drop_table(&mut self, table_name: &str) -> io::Result<()> {
        let (first_page, _) = helpers::get_table_first_page_and_cols(self, table_name)?;

        // collect page chain
        let page_chain = helpers::collect_page_chain(&mut self.buffer_pool, first_page)?;

        // remove catalog entry
        self.table_catalog
            .drop_table(table_name, &mut self.buffer_pool, &mut self.wal_writer)?;

        // free pages
        for page_id in page_chain {
            self.buffer_pool.free_page(page_id, &mut self.wal_writer)?;
        }

        // drop indexes
        self.drop_table_indexes(table_name)?;

        Ok(())
    }

    // Index catalog methods
    pub fn add_new_index(&mut self, entry: IndexEntry) -> io::Result<()> {
        let btree = BPlusTree::load(entry.root_page_id(), entry.column_type().order());
        let index_name = entry.index_name().to_owned();

        self.index_catalog
            .add_index(&mut self.buffer_pool, &mut self.wal_writer, entry)?;
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
        self.index_catalog.remove_index(
            table_name,
            index_name,
            &mut self.buffer_pool,
            &mut self.wal_writer,
        )
    }

    pub fn total_index_count(&self) -> usize {
        self.index_catalog.total_count()
    }

    pub fn drop_table_indexes(&mut self, table_name: &str) -> io::Result<()> {
        self.index_catalog.remove_table_indexes(
            table_name,
            &mut self.buffer_pool,
            &mut self.wal_writer,
        )
    }

    pub fn get_table_last_page(&self, table_name: &str) -> Option<PageId> {
        self.table_catalog.get_last_page(table_name)
    }

    pub fn update_table_last_page(&mut self, table_name: &str, page_id: PageId) -> io::Result<()> {
        self.table_catalog.update_last_page(
            table_name,
            page_id,
            &mut self.buffer_pool,
            &mut self.wal_writer,
        )
    }

    // Indexes
    pub fn insert_into_index(
        &mut self,
        index_name: &str,
        key: IndexKey,
        row_location: RowLocation,
    ) -> io::Result<()> {
        let btree = self.indexes.get_mut(index_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("index '{}' not found", index_name),
            )
        })?;

        btree.insert(
            key,
            row_location,
            &mut self.buffer_pool,
            &mut self.wal_writer,
        )
    }

    pub fn indexes(&self) -> &HashMap<String, BPlusTree> {
        &self.indexes
    }

    pub fn search_index(
        &mut self,
        index_name: &str,
        key: &IndexKey,
    ) -> io::Result<Option<RowLocation>> {
        let btree = self
            .indexes
            .get_mut(index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index not found"))?;
        btree.search(key, &mut self.buffer_pool)
    }

    pub fn delete_from_index(&mut self, index_name: &str, key: &IndexKey) -> io::Result<()> {
        let btree = self
            .indexes
            .get_mut(index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index not found"))?;
        btree.delete(key, &mut self.buffer_pool, &mut self.wal_writer)
    }

    pub fn range_index_scan(
        &mut self,
        index_name: &str,
        start: Option<&IndexKey>,
        end: Option<&IndexKey>,
        op: &BinaryOperator,
    ) -> io::Result<Vec<RowLocation>> {
        let Database {
            indexes,
            buffer_pool,
            ..
        } = self;

        let btree = indexes
            .get_mut(index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index not found"))?;

        btree.range_scan(start, end, op, buffer_pool)
    }

    pub(crate) fn get_wal_and_buffer_pool(&mut self) -> (&mut WalWriter, &mut BufferPool) {
        let Database {
            buffer_pool,
            wal_writer,
            ..
        } = self;

        (wal_writer, buffer_pool)
    }

    pub fn total_num_of_db_pages(&self) -> u32 {
        self.buffer_pool.total_num_of_db_pages()
    }

    pub fn checkpoint(&mut self) -> io::Result<()> {
        self.buffer_pool.flush_dirty()?;
        self.wal_writer.checkpoint()?;
        Ok(())
    }

    pub fn wal_append_slotted(
        &mut self,
        record_type: WalRecordType,
        table_name: &str,
        page_id: PageId,
        slot: u16,
        new_data: &[u8],
        old_data: &[u8],
    ) -> io::Result<u64> {
        Ok(self.wal_writer.append_slotted(
            record_type,
            table_name,
            page_id,
            slot,
            new_data,
            old_data,
        )?)
    }

    pub fn wal_append_raw(
        &mut self,
        record_type: WalRecordType,
        page_id: PageId,
        new_data: &[u8],
        old_data: &[u8],
    ) -> io::Result<u64> {
        Ok(self
            .wal_writer
            .append_raw(record_type, page_id, new_data, old_data)?)
    }
}
