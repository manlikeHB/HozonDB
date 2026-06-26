use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    constants::{self, Lsn, PageId, TxnId},
    index::{
        key::IndexKey,
        node::{
            InternalNode, LeafNode, Node,
            leaf::{LeafEntry, RowLocation},
        },
    },
    sql::parser::BinaryOperator,
    storage::buffer_pool::BufferPool,
    wal::{record_type::WalRecordType, writer::WalWriter},
};

pub struct BPlusTree {
    root: Option<PageId>,
    order: usize,
    cache: HashMap<PageId, Node>,
}

impl BPlusTree {
    /// This creates a fresh index
    /// allocates root page and writes empty leaf to disk
    pub fn new(
        order: usize,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: TxnId,
    ) -> io::Result<(Self, Vec<(Lsn, u64)>)> {
        let (root_page_id, lsn1, wal_offset1) =
            buffer_pool.allocate_raw_page(wal_writer, txn_id)?;
        let root_leaf = Node::Leaf(LeafNode::new());

        let mut b_plus_tree = BPlusTree {
            root: Some(root_page_id),
            order,
            cache: HashMap::new(),
        };

        let (lsn2, wal_offset2) = b_plus_tree.write_node(
            buffer_pool,
            wal_writer,
            root_page_id,
            root_leaf,
            WalRecordType::CreateBPlusTree,
            txn_id,
        )?;

        Ok((b_plus_tree, vec![(lsn1, wal_offset1), (lsn2, wal_offset2)]))
    }

    /// Opens existing tree with empty cache (lazy loading)
    ///
    /// On start up, an existing index B+ tree gets opened with empty cache
    /// the node are only read and cache lazily with 'load_node'
    pub fn load(root_page_id: PageId, order: usize) -> Self {
        BPlusTree {
            root: Some(root_page_id),
            order,
            cache: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        key: IndexKey,
        row_location: RowLocation,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<Vec<(Lsn, u64)>> {
        // collect lsns
        let mut lsns: Vec<(u64, u64)> = Vec::new();

        if let Some(page_id) = self.root {
            let (leaf_page_id, mut path) = self.find_leaf(&key, page_id, buffer_pool)?;

            // track current page of node
            // this get's updated with page ids in the path, traversing
            // the path all the way back to the root
            let mut cur_page = leaf_page_id;

            // current key that needs to be inserted
            // this gets updated with the split key from a leaf or internal node split
            let mut cur_key = key;

            // new page assigned to the new leaf or internal node
            let mut new_right_page: Option<PageId> = None;

            loop {
                if let Some(node) = self.cache.get_mut(&cur_page) {
                    match node {
                        Node::Leaf(leaf) => {
                            leaf.insert(LeafEntry::new(cur_key.clone(), row_location));

                            if leaf.is_full(self.order) {
                                // assign new page
                                let (new_page, lsn, wal_offset) =
                                    buffer_pool.allocate_raw_page(wal_writer, txn_id)?;
                                lsns.push((lsn, wal_offset));

                                // split leaf
                                let (k, new_leaf) = leaf.split(new_page);

                                // persist to buffer pool
                                // write left (modified by split)
                                let mut left_bytes = vec![];
                                left_bytes.push(constants::LEAF_NODE_TYPE);
                                leaf.write_to(&mut left_bytes);
                                let lsn = Self::update_node(
                                    buffer_pool,
                                    wal_writer,
                                    cur_page,
                                    &left_bytes,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                // write right (new node) and add to cache
                                let right_node = Node::Leaf(new_leaf);
                                let lsn = self.write_node(
                                    buffer_pool,
                                    wal_writer,
                                    new_page,
                                    right_node,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                cur_key = k;
                                new_right_page = Some(new_page);

                                cur_page = match path.pop() {
                                    Some(page_id) => page_id,
                                    None => {
                                        // split reached the root, create new root
                                        let lsn = self.create_new_root(
                                            cur_page,
                                            cur_key,
                                            new_right_page.expect("root creation reached without a prior split — this is a bug"),
                                            buffer_pool,
                                            wal_writer,
                                            txn_id
                                        )?;
                                        lsn.iter().for_each(|l| lsns.push(*l));
                                        break;
                                    }
                                }
                            } else {
                                // persist to buffer pool
                                let mut node_bytes = vec![];
                                node_bytes.push(constants::LEAF_NODE_TYPE);
                                leaf.write_to(&mut node_bytes);
                                let lsn = Self::update_node(
                                    buffer_pool,
                                    wal_writer,
                                    cur_page,
                                    &node_bytes,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                break;
                            }
                        }
                        Node::Internal(internal) => {
                            internal.insert_child(
                                cur_key.clone(),
                                new_right_page.expect(
                                    "internal node reached without a prior split — this is a bug",
                                ),
                            );

                            if internal.is_full(self.order) {
                                let (new_page, lsn, wal_offset) =
                                    buffer_pool.allocate_raw_page(wal_writer, txn_id)?;
                                lsns.push((lsn, wal_offset));

                                // split internal node
                                let (k, new_internal) = internal.split();

                                // persist to disk
                                // write left (modified by split)
                                let mut left_bytes = vec![];
                                left_bytes.push(constants::INTERNAL_NODE_TYPE);
                                internal.write_to(&mut left_bytes);
                                let lsn = Self::update_node(
                                    buffer_pool,
                                    wal_writer,
                                    cur_page,
                                    &left_bytes,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                // write right (new node) and add to cache
                                let right_node = Node::Internal(new_internal);
                                let lsn = self.write_node(
                                    buffer_pool,
                                    wal_writer,
                                    new_page,
                                    right_node,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                cur_key = k;
                                new_right_page = Some(new_page);

                                cur_page = match path.pop() {
                                    Some(page_id) => page_id,
                                    None => {
                                        // split reached the root, create new root
                                        let lsn = self.create_new_root(
                                            cur_page,
                                            cur_key,
                                            new_right_page.expect("root creation reached without a prior split — this is a bug"),
                                            buffer_pool,
                                            wal_writer,
                                            txn_id
                                        )?;
                                        lsn.iter().for_each(|l| lsns.push(*l));
                                        break;
                                    }
                                }
                            } else {
                                let mut node_bytes = vec![];
                                node_bytes.push(constants::INTERNAL_NODE_TYPE);
                                internal.write_to(&mut node_bytes);
                                let lsn = Self::update_node(
                                    buffer_pool,
                                    wal_writer,
                                    cur_page,
                                    &node_bytes,
                                    WalRecordType::IndexNode,
                                    txn_id,
                                )?;
                                lsns.push(lsn);

                                break;
                            }
                        }
                    }
                } else {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        format!("page {} not found", cur_page),
                    ));
                }
            }

            Ok(lsns)
        } else {
            // create new leaf node since root is None
            let mut leaf = LeafNode::new();
            leaf.insert(LeafEntry::new(key, row_location)); // insert new index
            let (new_page, lsn, wal_offset) = buffer_pool.allocate_raw_page(wal_writer, txn_id)?;
            lsns.push((lsn, wal_offset));

            // add the new leaf to nodes
            self.cache.insert(new_page, Node::Leaf(leaf));
            // set root to new leaf node
            self.root = Some(new_page);
            Ok(lsns)
        }
    }

    /// finds a leaf node containing the index key
    ///
    /// returns (leaf node page id, path to the leaf node)
    fn find_leaf(
        &mut self,
        key: &IndexKey,
        start: PageId,
        buffer_pool: &mut BufferPool,
    ) -> io::Result<(PageId, Vec<PageId>)> {
        let mut path = Vec::new();
        let mut cur = start;

        loop {
            match self.load_node(cur, buffer_pool)? {
                Node::Internal(internal) => {
                    path.push(cur);
                    let pos = internal
                        .keys()
                        .iter()
                        .position(|k| key < k)
                        .unwrap_or(internal.keys().len());
                    cur = internal.children()[pos];
                }
                Node::Leaf(_) => return Ok((cur, path)),
            }
        }
    }

    fn create_new_root(
        &mut self,
        left: PageId,
        key: IndexKey,
        right: PageId,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<Vec<(Lsn, u64)>> {
        let (new_root_page, lsn1, wal_offset1) =
            buffer_pool.allocate_raw_page(wal_writer, txn_id)?;
        let new_root = InternalNode::new(vec![key], vec![left, right]);

        let (lsn2, wal_offset2) = self.write_node(
            buffer_pool,
            wal_writer,
            new_root_page,
            Node::Internal(new_root),
            WalRecordType::IndexRoot,
            txn_id,
        )?;

        self.root = Some(new_root_page);
        Ok(vec![(lsn1, wal_offset1), (lsn2, wal_offset2)])
    }

    pub fn search(
        &mut self,
        key: &IndexKey,
        buffer_pool: &mut BufferPool,
    ) -> io::Result<Option<RowLocation>> {
        match self.root {
            Some(root_page_id) => {
                let (leaf_page_id, _) = self.find_leaf(key, root_page_id, buffer_pool)?;

                let node = self.cache.get(&leaf_page_id).ok_or_else(|| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!("Node for {} not found", leaf_page_id),
                    )
                })?;

                match node {
                    Node::Leaf(leaf) => {
                        if let Some(pos) =
                            leaf.entry().iter().position(|entry| entry.get_key() == key)
                        {
                            return Ok(Some(leaf.entry()[pos].get_row()));
                        } else {
                            return Ok(None);
                        }
                    }
                    Node::Internal(_) => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Expected a leaf node, found Internal node".to_string(),
                        ));
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// removes a key from leaf node if it exist
    pub fn delete(
        &mut self,
        key: &IndexKey,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        if let Some(root_page_id) = self.root {
            let (leaf_page_id, _) = self.find_leaf(key, root_page_id, buffer_pool)?;

            if let Some(node) = self.cache.get_mut(&leaf_page_id) {
                match node {
                    Node::Leaf(leaf) => match leaf.remove(key) {
                        true => {
                            // persist to disk
                            let mut buf = Vec::new();
                            buf.push(constants::LEAF_NODE_TYPE);
                            leaf.write_to(&mut buf);

                            // log + write page
                            let (lsn, wal_offset) = Self::update_node(
                                buffer_pool,
                                wal_writer,
                                leaf_page_id,
                                &buf,
                                WalRecordType::DeleteKey,
                                txn_id,
                            )?;

                            Ok((lsn, wal_offset))
                        }
                        false => Err(Error::new(ErrorKind::NotFound, "key doesn't exist")),
                    },
                    Node::Internal(_) => Err(Error::new(
                        ErrorKind::InvalidData,
                        "Expected a leaf node, found Internal node",
                    )),
                }
            } else {
                Err(Error::new(
                    ErrorKind::NotFound,
                    format!("No node with page id {}", leaf_page_id),
                ))
            }
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                "tree root page id not present",
            ))
        }
    }

    /// this checks cache for node first
    /// otherwise read from disk and cache it
    //
    // TODO: Buffer pool — the node cache grows unbounded within a session.
    // Lazy loading prevents loading nodes that are never accessed, but over
    // a long session with many queries touching different parts of the tree,
    // the cache accumulates all visited nodes with no eviction.
    // eviction policy needs to be implemented
    fn load_node(&mut self, page_id: PageId, buffer_pool: &mut BufferPool) -> io::Result<&Node> {
        if !self.cache.contains_key(&page_id) {
            // read node from buffer pool
            let node_bytes = buffer_pool.read_page(page_id)?;
            let node = Node::from_bytes(&node_bytes[constants::OFFSET_RAW_PAGE_START..])?;
            // insert into cache
            self.cache.insert(page_id, node);
        }

        self.cache.get(&page_id).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("Node with page id {} not found in cache", page_id),
            )
        })
    }

    fn write_node(
        &mut self,
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        page_id: PageId,
        node: Node,
        record_type: WalRecordType,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let node_bytes = node.to_bytes();
        let old_data = buffer_pool.read_page(page_id)?;

        let mut new_data = old_data.clone();
        new_data
            [constants::OFFSET_RAW_PAGE_START..constants::OFFSET_RAW_PAGE_START + node_bytes.len()]
            .copy_from_slice(&node_bytes);

        let (lsn, wal_offset) =
            wal_writer.append_raw(record_type, page_id, &new_data, old_data, txn_id)?;

        buffer_pool.write_raw_page(page_id, &new_data, lsn)?;

        self.cache.insert(page_id, node);
        Ok((lsn, wal_offset))
    }

    pub fn root(&self) -> Option<PageId> {
        self.root
    }

    pub fn range_scan(
        &mut self,
        start: Option<&IndexKey>, // None = from beginning
        end: Option<&IndexKey>,   // None = to end
        op: &BinaryOperator,      // to know if bounds are inclusive/exclusive
        buffer_pool: &mut BufferPool,
    ) -> io::Result<Vec<RowLocation>> {
        let mut row_locations = Vec::new();

        if let Some(root_page_id) = self.root() {
            // if start is None, then we need the first leaf in this tree
            let start_leaf = match start {
                Some(key) => {
                    let (leaf_page_id, _) = self.find_leaf(key, root_page_id, buffer_pool)?;
                    leaf_page_id
                }
                None => {
                    // find left most leaf node
                    let mut cur = root_page_id;

                    loop {
                        match self.load_node(cur, buffer_pool)? {
                            Node::Internal(internal) => {
                                cur = internal.children()[0];
                            }
                            Node::Leaf(_) => break,
                        }
                    }

                    cur
                }
            };

            let mut cur_leaf = start_leaf;
            let mut done = false;

            // transverse leaves to get row location of the rows that fit
            loop {
                match self.load_node(cur_leaf, buffer_pool)? {
                    Node::Leaf(leaf) => {
                        for entry in leaf.entry() {
                            let key = entry.get_key();

                            let in_range = match op {
                                BinaryOperator::LessThan => {
                                    key < end.ok_or_else(|| {
                                        Error::new(ErrorKind::InvalidInput, "expected end bound")
                                    })?
                                }
                                BinaryOperator::LessOrEqual => {
                                    key <= end.ok_or_else(|| {
                                        Error::new(ErrorKind::InvalidInput, "expected end bound")
                                    })?
                                }
                                BinaryOperator::GreaterThan => {
                                    key > start.ok_or_else(|| {
                                        Error::new(ErrorKind::InvalidInput, "expected start bound")
                                    })?
                                }
                                BinaryOperator::GreaterOrEqual => {
                                    key >= start.ok_or_else(|| {
                                        Error::new(ErrorKind::InvalidInput, "expected start bound")
                                    })?
                                }
                                _ => unreachable!("range_scan called with non-range operator"),
                            };

                            if in_range {
                                row_locations.push(entry.get_row());
                            } else if matches!(
                                op,
                                BinaryOperator::LessThan | BinaryOperator::LessOrEqual
                            ) {
                                // if the op is '<' or '<=', since the keys are sorted, that means every other key is '>' the end key
                                done = true;
                                break;
                            }
                        }

                        if done {
                            break;
                        }

                        if let Some(next_leaf_page_id) = leaf.next() {
                            cur_leaf = next_leaf_page_id;
                        } else {
                            break;
                        }
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "expected leaf node during range scan",
                        ));
                    }
                }
            }
        };

        Ok(row_locations)
    }

    fn update_node(
        buffer_pool: &mut BufferPool,
        wal_writer: &mut WalWriter,
        page_id: PageId,
        node_bytes: &[u8],
        record_type: WalRecordType,
        txn_id: u64,
    ) -> io::Result<(Lsn, u64)> {
        let old_data = buffer_pool.read_page(page_id)?;

        let mut new_data = old_data.clone();
        new_data
            [constants::OFFSET_RAW_PAGE_START..constants::OFFSET_RAW_PAGE_START + node_bytes.len()]
            .copy_from_slice(node_bytes);

        let (lsn, wal_offset) =
            wal_writer.append_raw(record_type, page_id, &new_data, old_data, txn_id)?;

        buffer_pool.write_raw_page(page_id, &new_data, lsn)?;
        Ok((lsn, wal_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::page::PageManager, test_helpers::*};

    fn get_rows(count: usize) -> Vec<RowLocation> {
        let mut rows = Vec::new();
        let page_id = 99;
        for i in 1..=count {
            let row = RowLocation::new((page_id + i) as u32, (page_id + i) as u16);
            rows.push(row);
        }
        rows
    }

    fn get_integer_keys(count: usize) -> Vec<IndexKey> {
        (1..=count).map(|i| IndexKey::Integer(i as i32)).collect()
    }

    fn setup_tree(name: &str, count: i32) -> (BPlusTree, BufferPool, WalWriter) {
        let pm = PageManager::new(name).unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new(name).unwrap();
        let (mut btree, _) = BPlusTree::new(4, &mut buffer_pool, &mut wal_writer, 1).unwrap();

        for i in 1..=count {
            btree
                .insert(
                    IndexKey::Integer(i),
                    RowLocation::new(i as u32, i as u16),
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        (btree, buffer_pool, wal_writer)
    }

    #[test]
    fn test_insert_before_split() {
        cleanup("test_insert_before_split");

        let pm = PageManager::new("test_insert_before_split").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_insert_before_split").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        btree
            .insert(
                keys[0].clone(),
                rows[0],
                &mut buffer_pool,
                &mut wal_writer,
                1,
            )
            .unwrap();
        btree
            .insert(
                keys[1].clone(),
                rows[1],
                &mut buffer_pool,
                &mut wal_writer,
                1,
            )
            .unwrap();

        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        let node = btree.cache.get(&btree.root.unwrap()).unwrap();
        match node {
            Node::Leaf(_) => (),
            Node::Internal(_) => panic!("expected a leaf node before split"),
        }

        cleanup("test_insert_before_split");
    }

    #[test]
    fn test_insert_till_leaf_split() {
        cleanup("test_insert_till_leaf_split");

        let pm = PageManager::new("test_insert_till_leaf_split").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_insert_till_leaf_split").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        for i in 0..btree.order {
            btree
                .insert(
                    keys[i].clone(),
                    rows[i],
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        assert_eq!(btree.cache.len(), 3);
        assert!(btree.root.is_some());

        let node = btree.cache.get(&btree.root.unwrap()).unwrap();
        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }

        cleanup("test_insert_till_leaf_split");
    }

    #[test]
    fn test_insert_till_internal_node_split() {
        cleanup("test_insert_till_internal_node_split");

        let pm = PageManager::new("test_insert_till_internal_node_split").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_insert_till_internal_node_split").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(9);

        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        for i in 0..btree.order {
            btree
                .insert(
                    keys[i].clone(),
                    rows[i],
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        assert_eq!(btree.cache.len(), 3);
        assert!(btree.root.is_some());

        let before_root = btree.root.unwrap();
        let node = btree.cache.get(&btree.root.unwrap()).unwrap();
        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }

        for i in 0..btree.order {
            btree
                .insert(
                    keys[i + btree.order].clone(),
                    rows[i],
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        assert!(btree.root.is_some());
        let after_root = btree.root.unwrap();
        assert_ne!(before_root, after_root);

        let node = btree.cache.get(&btree.root.unwrap()).unwrap();
        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }

        cleanup("test_insert_till_internal_node_split");
    }

    #[test]
    fn test_search_empty_tree() {
        cleanup("test_search_empty_tree");

        let pm = PageManager::new("test_search_empty_tree").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_search_empty_tree").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();

        let res = btree
            .search(&IndexKey::Integer(5), &mut buffer_pool)
            .unwrap();
        assert!(res.is_none());

        cleanup("test_search_empty_tree");
    }

    #[test]
    fn test_search_tree_with_leaf_nodes() {
        cleanup("test_search_tree_with_leaf_nodes");

        let pm = PageManager::new("test_search_tree_with_leaf_nodes").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_search_tree_with_leaf_nodes").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();

        let row_1 = RowLocation::new(100, 232);
        let key_1 = IndexKey::Integer(5);
        let row_2 = RowLocation::new(200, 400);
        let key_2 = IndexKey::Integer(15);

        btree
            .insert(key_1.clone(), row_1, &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();
        btree
            .insert(key_2.clone(), row_2, &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();

        let row_res = btree.search(&key_2, &mut buffer_pool).unwrap().unwrap();
        assert_eq!(row_res, row_2);

        let row_res = btree.search(&key_1, &mut buffer_pool).unwrap().unwrap();
        assert_eq!(row_res, row_1);

        cleanup("test_search_tree_with_leaf_nodes");
    }

    #[test]
    fn test_search_tree_with_internal_nodes() {
        cleanup("test_search_tree_with_internal_nodes");

        let pm = PageManager::new("test_search_tree_with_internal_nodes").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_search_tree_with_internal_nodes").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();

        let rows = get_rows(15);
        let keys = get_integer_keys(15);

        for i in 0..15 {
            btree
                .insert(
                    keys[i].clone(),
                    rows[i],
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        let row_1 = RowLocation::new(100, 232);
        let key_1 = IndexKey::Integer(30);
        let row_2 = RowLocation::new(200, 400);
        let key_2 = IndexKey::Integer(27);

        btree
            .insert(key_1.clone(), row_1, &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();
        btree
            .insert(key_2.clone(), row_2, &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();

        let row_res = btree.search(&key_2, &mut buffer_pool).unwrap().unwrap();
        assert_eq!(row_res, row_2);

        let row_res = btree.search(&key_1, &mut buffer_pool).unwrap().unwrap();
        assert_eq!(row_res, row_1);

        cleanup("test_search_tree_with_internal_nodes");
    }

    #[test]
    fn test_delete_key() {
        cleanup("test_delete_key");

        let (mut btree, mut buffer_pool, mut wal_writer) = setup_tree("test_delete_key", 15);

        btree
            .delete(&IndexKey::Integer(1), &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();
        assert!(
            btree
                .search(&IndexKey::Integer(1), &mut buffer_pool)
                .unwrap()
                .is_none()
        );

        assert!(
            btree
                .delete(&IndexKey::Integer(99), &mut buffer_pool, &mut wal_writer, 1)
                .is_err()
        );

        for i in 1..=15 {
            if i == 1 {
                continue;
            }
            assert!(
                btree
                    .search(&IndexKey::Integer(i), &mut buffer_pool)
                    .unwrap()
                    .is_some()
            );
        }

        cleanup("test_delete_key");
    }

    #[test]
    fn test_load_node_and_write_node() {
        cleanup("test_load_node");

        let pm = PageManager::new("test_load_node").unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new("test_load_node").unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();
        let node = Node::Leaf(LeafNode::new());
        let key = IndexKey::Integer(5);
        let row = RowLocation::new(99, 99);

        btree
            .insert(key, row, &mut buffer_pool, &mut wal_writer, 1)
            .unwrap();

        let cur_page = 1;

        let page_id = cur_page + 99;
        assert!(btree.load_node(page_id, &mut buffer_pool).is_err());

        btree
            .write_node(
                &mut buffer_pool,
                &mut wal_writer,
                cur_page,
                node.clone(),
                WalRecordType::IndexNode,
                1,
            )
            .unwrap();

        let res = btree.load_node(cur_page, &mut buffer_pool);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), &node);

        cleanup("test_load_node");
    }

    #[test]
    fn test_load_b_plus_tree_leaf_node() {
        cleanup("test_load_b_plus_tree");

        let order = 3;
        let key = IndexKey::Integer(4);
        let row = RowLocation::new(99, 99);
        let mut _root_page_id = 0;

        {
            let pm = PageManager::new("test_load_b_plus_tree").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer = WalWriter::new("test_load_b_plus_tree").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            btree
                .insert(key.clone(), row, &mut buffer_pool, &mut wal_writer, 1)
                .unwrap();
            assert_eq!(btree.cache.len(), 1);
            _root_page_id = btree.root.unwrap();
            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_load_b_plus_tree").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            let res = btree.search(&key, &mut buffer_pool).unwrap();
            assert!(res.is_some());
            assert_eq!(res.unwrap(), row);
        }

        cleanup("test_load_b_plus_tree");
    }

    #[test]
    fn test_load_b_plus_tree_internal_node_integer() {
        cleanup("test_load_b_plus_tree_internal_node_integer");

        let order = 3;
        let key_to_search = IndexKey::Integer(25);
        let expected_row = RowLocation::new(99, 99);
        let mut _root_page_id = 0;

        {
            let pm = PageManager::new("test_load_b_plus_tree_internal_node_integer").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer =
                WalWriter::new("test_load_b_plus_tree_internal_node_integer").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            let keys = get_integer_keys(15);
            let rows = get_rows(15);

            for (key, row) in keys.iter().zip(rows) {
                btree
                    .insert(key.clone(), row, &mut buffer_pool, &mut wal_writer, 1)
                    .unwrap();
            }

            btree
                .insert(
                    key_to_search.clone(),
                    expected_row,
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
            assert!(btree.cache.len() > 1);
            _root_page_id = btree.root.unwrap();
            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_load_b_plus_tree_internal_node_integer").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            let res = btree.search(&key_to_search, &mut buffer_pool).unwrap();
            assert!(res.is_some());
            assert_eq!(res.unwrap(), expected_row);
        }

        cleanup("test_load_b_plus_tree_internal_node_integer");
    }

    #[test]
    fn test_load_b_plus_tree_internal_node_text() {
        cleanup("test_load_b_plus_tree_internal_node_text");

        let order = 3;
        let key_to_search = IndexKey::Text(format!("test45@example.com"));
        let expected_row = RowLocation::new(99, 99);
        let mut _root_page_id = 0;

        {
            let pm = PageManager::new("test_load_b_plus_tree_internal_node_text").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer =
                WalWriter::new("test_load_b_plus_tree_internal_node_text").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            let rows = get_rows(15);
            for (i, row) in rows.iter().enumerate() {
                btree
                    .insert(
                        IndexKey::Text(format!("test{}@example.com", i)),
                        *row,
                        &mut buffer_pool,
                        &mut wal_writer,
                        1,
                    )
                    .unwrap();
            }

            btree
                .insert(
                    key_to_search.clone(),
                    expected_row,
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
            assert!(btree.cache.len() > 1);
            _root_page_id = btree.root.unwrap();
            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_load_b_plus_tree_internal_node_text").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            let res = btree.search(&key_to_search, &mut buffer_pool).unwrap();
            assert!(res.is_some());
            assert_eq!(res.unwrap(), expected_row);
        }

        cleanup("test_load_b_plus_tree_internal_node_text");
    }

    #[test]
    fn test_delete_persistence_round_trip() {
        cleanup("test_delete_persistence_round_trip");

        let order = 3;
        let key_1 = IndexKey::Text(format!("test5@example.com"));
        let key_2 = IndexKey::Text(format!("test35@example.com"));
        let row_1 = RowLocation::new(99, 234);
        let row_2 = RowLocation::new(54, 345);
        let mut _root_page_id = 0;

        {
            let pm = PageManager::new("test_delete_persistence_round_trip").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer = WalWriter::new("test_delete_persistence_round_trip").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            btree
                .insert(key_1.clone(), row_1, &mut buffer_pool, &mut wal_writer, 1)
                .unwrap();
            btree
                .insert(key_2.clone(), row_2, &mut buffer_pool, &mut wal_writer, 1)
                .unwrap();

            assert_eq!(
                btree
                    .search(&key_1.clone(), &mut buffer_pool)
                    .unwrap()
                    .unwrap(),
                row_1
            );
            assert_eq!(
                btree
                    .search(&key_2.clone(), &mut buffer_pool)
                    .unwrap()
                    .unwrap(),
                row_2
            );

            btree
                .delete(&key_1, &mut buffer_pool, &mut wal_writer, 1)
                .unwrap();

            assert!(btree.search(&key_1, &mut buffer_pool).unwrap().is_none());
            assert!(btree.search(&key_2, &mut buffer_pool).unwrap().is_some());

            _root_page_id = btree.root.unwrap();
            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_delete_persistence_round_trip").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);

            assert!(btree.search(&key_1, &mut buffer_pool).unwrap().is_none());
            assert_eq!(
                btree
                    .search(&key_2.clone(), &mut buffer_pool)
                    .unwrap()
                    .unwrap(),
                row_2
            );
        }

        cleanup("test_delete_persistence_round_trip");
    }

    #[test]
    fn test_create_new_root_persistence() {
        cleanup("test_create_new_root_persistence");

        let order = 3;
        let mut _root_page_id = 0;
        let keys = get_integer_keys(order + 1);
        let rows = get_rows(order + 1);

        {
            let pm = PageManager::new("test_create_new_root_persistence").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer = WalWriter::new("test_create_new_root_persistence").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            for (key, row) in keys.iter().zip(rows.iter()) {
                btree
                    .insert(key.clone(), *row, &mut buffer_pool, &mut wal_writer, 1)
                    .unwrap();
            }

            let root = btree.cache.get(&btree.root.unwrap()).unwrap();
            match root {
                Node::Internal(_) => (),
                Node::Leaf(_) => panic!("expected internal root after split"),
            }

            _root_page_id = btree.root.unwrap();
            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_create_new_root_persistence").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            let root = btree.load_node(_root_page_id, &mut buffer_pool).unwrap();
            match root {
                Node::Internal(internal) => {
                    assert!(
                        !internal.keys().is_empty(),
                        "root has no keys — was it persisted?"
                    );
                    assert!(internal.children().len() >= 2, "root has no children");
                }
                Node::Leaf(_) => panic!("expected internal root after reload"),
            }

            for (key, row) in keys.iter().zip(rows.iter()) {
                let res = btree.search(key, &mut buffer_pool).unwrap();
                assert!(res.is_some(), "key {:?} not found after reload", key);
                assert_eq!(res.unwrap(), *row);
            }
        }

        cleanup("test_create_new_root_persistence");
    }

    #[test]
    fn test_root_changes_multiple_times() {
        cleanup("test_root_changes_multiple_times");

        let order = 3;
        let count = 40;
        let keys = get_integer_keys(count);
        let rows = get_rows(count);
        let mut _root_page_id = 0;
        let mut root_changes = 0;
        let mut _prev_root: Option<PageId> = None;

        {
            let pm = PageManager::new("test_root_changes_multiple_times").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut wal_writer = WalWriter::new("test_root_changes_multiple_times").unwrap();
            let (mut btree, _) =
                BPlusTree::new(order, &mut buffer_pool, &mut wal_writer, 1).unwrap();

            for (key, row) in keys.iter().zip(rows.iter()) {
                let root_before = btree.root;
                btree
                    .insert(key.clone(), *row, &mut buffer_pool, &mut wal_writer, 1)
                    .unwrap();
                let root_after = btree.root;

                if root_before != root_after {
                    root_changes += 1;
                    _prev_root = root_before;
                }
            }

            assert!(
                root_changes >= 3,
                "expected at least 3 root changes, got {}",
                root_changes
            );
            _root_page_id = btree.root.unwrap();

            buffer_pool.flush_dirty().unwrap();
        }

        {
            let pm = PageManager::new("test_root_changes_multiple_times").unwrap();
            let mut buffer_pool = BufferPool::new(pm, 64);
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            let root = btree.load_node(_root_page_id, &mut buffer_pool).unwrap();
            match root {
                Node::Internal(internal) => {
                    assert!(!internal.keys().is_empty(), "root has no keys after reload");
                    assert!(
                        internal.children().len() >= 2,
                        "root has fewer than 2 children after reload"
                    );
                }
                Node::Leaf(_) => panic!("expected internal root after multiple splits"),
            }

            for (key, row) in keys.iter().zip(rows.iter()) {
                let res = btree.search(key, &mut buffer_pool).unwrap();
                assert!(res.is_some(), "key {:?} not found after reload", key);
                assert_eq!(res.unwrap(), *row, "wrong row for key {:?}", key);
            }
        }

        cleanup("test_root_changes_multiple_times");
    }

    #[test]
    fn test_range_scan_less_than() {
        let name = "test_range_scan_lt";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let end = IndexKey::Integer(5);
        let results = btree
            .range_scan(
                None,
                Some(&end),
                &BinaryOperator::LessThan,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 4);
        for (i, loc) in results.iter().enumerate() {
            assert_eq!(loc.page_id(), (i + 1) as u32);
        }

        cleanup(name);
    }

    #[test]
    fn test_range_scan_less_than_or_equal() {
        let name = "test_range_scan_lte";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let end = IndexKey::Integer(5);
        let results = btree
            .range_scan(
                None,
                Some(&end),
                &BinaryOperator::LessOrEqual,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 5);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_greater_than() {
        let name = "test_range_scan_gt";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let start = IndexKey::Integer(7);
        let results = btree
            .range_scan(
                Some(&start),
                None,
                &BinaryOperator::GreaterThan,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 3);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_greater_than_or_equal() {
        let name = "test_range_scan_gte";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let start = IndexKey::Integer(7);
        let results = btree
            .range_scan(
                Some(&start),
                None,
                &BinaryOperator::GreaterOrEqual,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 4);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_no_matches() {
        let name = "test_range_scan_no_match";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let end = IndexKey::Integer(0);
        let results = btree
            .range_scan(
                None,
                Some(&end),
                &BinaryOperator::LessThan,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 0);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_all_rows_less_than_or_equal() {
        let name = "test_range_scan_all_lte";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 10);

        let end = IndexKey::Integer(10);
        let results = btree
            .range_scan(
                None,
                Some(&end),
                &BinaryOperator::LessOrEqual,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 10);

        cleanup(name);
    }

    #[test]
    #[should_panic]
    fn test_range_scan_invalid_op() {
        let name = "test_range_scan_invalid_op";
        cleanup(name);
        let (mut btree, mut buffer_pool, _) = setup_tree(name, 5);
        cleanup(name);
        btree
            .range_scan(None, None, &BinaryOperator::Equals, &mut buffer_pool)
            .unwrap();
    }

    #[test]
    fn test_range_scan_missing_bound_returns_error() {
        let name = "test_range_scan_missing_bound";
        cleanup(name);

        let (mut btree, mut buffer_pool, _) = setup_tree(name, 5);

        let result = btree.range_scan(None, None, &BinaryOperator::LessThan, &mut buffer_pool);
        assert!(result.is_err());

        cleanup(name);
    }

    #[test]
    fn test_range_scan_spans_multiple_leaves() {
        let name = "test_range_scan_multi_leaf";
        cleanup(name);

        let pm = PageManager::new(name).unwrap();
        let mut buffer_pool = BufferPool::new(pm, 64);
        let mut wal_writer = WalWriter::new(name).unwrap();
        let (mut btree, _) = BPlusTree::new(3, &mut buffer_pool, &mut wal_writer, 1).unwrap();

        for i in 1..=15 {
            btree
                .insert(
                    IndexKey::Integer(i),
                    RowLocation::new(i as u32, i as u16),
                    &mut buffer_pool,
                    &mut wal_writer,
                    1,
                )
                .unwrap();
        }

        let end = IndexKey::Integer(10);
        let results = btree
            .range_scan(
                None,
                Some(&end),
                &BinaryOperator::LessOrEqual,
                &mut buffer_pool,
            )
            .unwrap();

        assert_eq!(results.len(), 10);

        cleanup(name);
    }
}
