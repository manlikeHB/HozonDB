use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
    path::Path,
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
    transaction::Txn,
    wal::{reader::WalReader, record_type::WalRecordType, writer::WalWriter},
};

pub struct Database {
    table_catalog: TableCatalog,
    index_catalog: IndexCatalog,
    indexes: HashMap<String, BPlusTree>, // index name -> B+ tree
    buffer_pool: BufferPool,
    wal_writer: WalWriter,
    // represents the current transaction
    // HozonDB is single threaded so there could only be an instance of a Txn at a time
    txn: Option<Txn>,
    next_txn_id: u64,
}

impl Database {
    /// Creates or opens a database
    ///
    /// args
    /// db_name: name of database e.g mydb
    /// Uses BufferPool default capacity of 1024 frames (4MB).
    pub fn new(db_name: &str) -> io::Result<Self> {
        Self::with_capacity(db_name, constants::DEFAULT_BUFFER_POOL_CAPACITY)
    }

    /// Creates or opens a database with BufferPool frames capacity
    ///
    /// args
    /// db_name: name of database e.g mydb
    /// capacity: max number of frames in BufferPool.
    ///   Each frame holds one 4KB page. Minimum recommended: 64 frames (256KB).
    ///   Very small capacities cause frequent eviction and poor performance.
    ///
    /// Use Database::new(db_name) for the default capacity of 1024 frames (4MB).
    // TODO: buffer pool capacity is fixed at startup. Production databases
    // support online resizing — shrinking evicts frames, growing allocates more.
    pub fn with_capacity(db_name: &str, capacity: usize) -> io::Result<Self> {
        let page_manager = PageManager::new(db_name)?;
        let mut buffer_pool = BufferPool::new(page_manager, capacity);
        let mut txn_id = 0;

        // recover FIRST — before anything reads from buffer pool
        if Path::new(&format!("{}.wal", db_name)).exists() {
            txn_id = WalReader::new(db_name)?.recover(&mut buffer_pool)?;
        }

        let mut wal_writer = WalWriter::new(db_name)?;

        // now safe to load catalogs — pages reflect recovered state
        let table_catalog = TableCatalog::new(&mut buffer_pool, &mut wal_writer)?;
        let index_catalog = IndexCatalog::new(&mut buffer_pool, &mut wal_writer)?;
        let mut indexes = HashMap::new();

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
            txn: None,
            next_txn_id: txn_id + 1,
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

    pub fn free_page(&mut self, page_id: PageId) -> io::Result<()> {
        let txn_id = self.cur_txn_id()?;
        self.buffer_pool
            .free_page(page_id, &mut self.wal_writer, txn_id)
    }

    pub fn is_page_cached(&self, page_id: PageId) -> bool {
        self.buffer_pool.is_cached(page_id)
    }

    // table catalog
    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        let txn_id = self.cur_txn_id()?;
        self.table_catalog.create_table(
            schema.clone(),
            &mut self.buffer_pool,
            &mut self.wal_writer,
            txn_id,
        )?;

        if let Some(pk_col) = schema.columns().iter().find(|c| c.is_primary_key()) {
            let column_type = IndexColumnType::try_from(*pk_col.data_type())?;
            let btree = BPlusTree::new(
                column_type.order(),
                &mut self.buffer_pool,
                &mut self.wal_writer,
                txn_id,
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

    pub fn first_free_page(&self) -> Option<PageId> {
        self.buffer_pool.first_free_page()
    }

    pub fn get_table_first_page(&self, table_name: &str) -> io::Result<PageId> {
        match self.table_catalog.get_table(table_name) {
            Some(table_meta) => Ok(table_meta.first_page()),
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                ));
            }
        }
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
        let txn_id = self.cur_txn_id()?;
        self.table_catalog.drop_table(
            table_name,
            &mut self.buffer_pool,
            &mut self.wal_writer,
            txn_id,
        )?;

        // free pages
        let txn_id = self.cur_txn_id()?;
        for page_id in page_chain {
            self.buffer_pool
                .free_page(page_id, &mut self.wal_writer, txn_id)?;
        }

        // drop indexes
        self.drop_table_indexes(table_name)?;

        Ok(())
    }

    // Index catalog methods
    pub fn add_new_index(&mut self, entry: IndexEntry) -> io::Result<()> {
        let btree = BPlusTree::load(entry.root_page_id(), entry.column_type().order());
        let index_name = entry.index_name().to_owned();

        let txn_id = self.cur_txn_id()?;
        self.index_catalog
            .add_index(&mut self.buffer_pool, &mut self.wal_writer, entry, txn_id)?;
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
        let txn_id = self.cur_txn_id()?;

        self.index_catalog.remove_index(
            table_name,
            index_name,
            &mut self.buffer_pool,
            &mut self.wal_writer,
            txn_id,
        )
    }

    pub fn total_index_count(&self) -> usize {
        self.index_catalog.total_count()
    }

    pub fn drop_table_indexes(&mut self, table_name: &str) -> io::Result<()> {
        let txn_id = self.cur_txn_id()?;
        self.index_catalog.remove_table_indexes(
            table_name,
            &mut self.buffer_pool,
            &mut self.wal_writer,
            txn_id,
        )
    }

    pub fn get_table_last_page(&self, table_name: &str) -> Option<PageId> {
        self.table_catalog.get_last_page(table_name)
    }

    pub fn update_table_last_page(&mut self, table_name: &str, page_id: PageId) -> io::Result<()> {
        let txn_id = self.cur_txn_id()?;
        self.table_catalog.update_last_page(
            table_name,
            page_id,
            &mut self.buffer_pool,
            &mut self.wal_writer,
            txn_id,
        )
    }

    // Indexes
    pub fn insert_into_index(
        &mut self,
        index_name: &str,
        key: IndexKey,
        row_location: RowLocation,
    ) -> io::Result<()> {
        let txn_id = self.cur_txn_id()?;
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
            txn_id,
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
        let txn_id = self.cur_txn_id()?;

        let btree = self
            .indexes
            .get_mut(index_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "index not found"))?;

        btree.delete(key, &mut self.buffer_pool, &mut self.wal_writer, txn_id)
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
        let txn_id = self.cur_txn_id()?;
        Ok(self.wal_writer.append_slotted(
            record_type,
            table_name,
            page_id,
            slot,
            new_data,
            old_data,
            txn_id,
        )?)
    }

    pub fn wal_append_raw(
        &mut self,
        record_type: WalRecordType,
        page_id: PageId,
        new_data: &[u8],
        old_data: &[u8],
    ) -> io::Result<u64> {
        let txn_id = self.cur_txn_id()?;
        Ok(self
            .wal_writer
            .append_raw(record_type, page_id, new_data, old_data, txn_id)?)
    }

    // Transaction
    pub fn next_txn_id(&mut self) -> u64 {
        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;
        txn_id
    }

    pub fn cur_txn_id(&self) -> io::Result<u64> {
        match &self.txn {
            Some(txn) => Ok(txn.id()),
            None => Err(io::Error::new(
                ErrorKind::InvalidData,
                "No active transaction",
            )),
        }
    }

    /// Creates an implicit Txn
    /// Begins an implicit transaction if no active transaction

    pub fn begin_implicit_txn(&mut self) -> io::Result<()> {
        if self.txn.is_none() {
            self.begin_txn(true)?;
        }

        Ok(())
    }

    /// Creates an explicit Txn when `BEGIN` is called
    pub fn begin_explicit_txn(&mut self) -> io::Result<u64> {
        self.begin_txn(false)
    }

    fn begin_txn(&mut self, is_implicit: bool) -> io::Result<u64> {
        if let Some(txn) = &self.txn {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!(
                    "transaction {} is already active, commit or rollback before starting a new one",
                    txn.id()
                ),
            ));
        }

        let txn_id = self.next_txn_id();
        let txn = Txn::new(txn_id, is_implicit);
        self.txn = Some(txn);
        Ok(txn_id)
    }

    pub fn commit_txn(&mut self) -> io::Result<()> {
        if self.txn.is_none() {
            return Err(io::Error::new(
                ErrorKind::Other,
                "No active transaction to commit",
            ));
        }

        // flush WAL to disk
        self.wal_writer.sync()?;
        //remove current txn
        self.txn = None;
        Ok(())
    }

    pub fn is_txn_implicit(&self) -> io::Result<bool> {
        if let Some(txn) = &self.txn {
            Ok(txn.is_implicit())
        } else {
            Err(io::Error::new(ErrorKind::Other, "No active transaction"))
        }
    }
}
