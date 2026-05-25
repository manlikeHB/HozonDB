use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    constants::{self, PageId},
    index::{
        key::IndexKey,
        node::{
            InternalNode, LeafNode, Node,
            leaf::{LeafEntry, RowLocation},
        },
    },
    sql::parser::BinaryOperator,
    storage::page::PageManager,
};

pub struct BPlusTree {
    root: Option<PageId>,
    order: usize,
    cache: HashMap<PageId, Node>,
}

impl BPlusTree {
    /// This creates a fresh index
    /// allocates root page and writes empty leaf to disk
    pub fn new(order: usize, pm: &mut PageManager) -> io::Result<Self> {
        let root_page_id = pm.allocate_page()?;
        let root_leaf = Node::Leaf(LeafNode::new());

        let mut b_plus_tree = BPlusTree {
            root: Some(root_page_id),
            order,
            cache: HashMap::new(),
        };

        // write leaf to disk
        b_plus_tree.write_node(root_page_id, root_leaf, pm)?;

        Ok(b_plus_tree)
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
        pm: &mut PageManager,
    ) -> io::Result<()> {
        if let Some(page_id) = self.root {
            let (leaf_page_id, mut path) = self.find_leaf(&key, page_id, pm)?;

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
                                let new_page = pm.allocate_page()?;

                                // split leaf
                                let (k, new_leaf) = leaf.split(new_page);

                                // persist to disk
                                // write left (modified by split)
                                let mut left_bytes = vec![];
                                left_bytes.push(constants::LEAF_NODE_TYPE);
                                leaf.write_to(&mut left_bytes);
                                pm.write_page(cur_page, &left_bytes)?;

                                // write right (new node) and add to cache
                                let right_node = Node::Leaf(new_leaf);
                                let right_bytes = right_node.to_bytes();
                                self.cache.insert(new_page, right_node);
                                pm.write_page(new_page, &right_bytes)?;

                                cur_key = k;
                                new_right_page = Some(new_page);

                                cur_page = match path.pop() {
                                    Some(page_id) => page_id,
                                    None => {
                                        // split reached the root, create new root
                                        self.create_new_root(
                                            cur_page,
                                            cur_key,
                                            new_right_page.expect("root creation reached without a prior split — this is a bug"),
                                            pm
                                        )?;
                                        break;
                                    }
                                }
                            } else {
                                // persist to disk
                                let mut node_bytes = vec![];
                                node_bytes.push(constants::LEAF_NODE_TYPE);
                                leaf.write_to(&mut node_bytes);
                                pm.write_page(cur_page, &node_bytes)?;

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
                                let new_page = pm.allocate_page()?;

                                // split internal node
                                let (k, new_internal) = internal.split();

                                // persist to disk
                                // write left (modified by split)
                                let mut left_bytes = vec![];
                                left_bytes.push(constants::INTERNAL_NODE_TYPE);
                                internal.write_to(&mut left_bytes);
                                pm.write_page(cur_page, &left_bytes)?;

                                // write right (new node) and add to cache
                                let right_node = Node::Internal(new_internal);
                                let right_bytes = right_node.to_bytes();
                                self.cache.insert(new_page, right_node);
                                pm.write_page(new_page, &right_bytes)?;

                                cur_key = k;
                                new_right_page = Some(new_page);

                                cur_page = match path.pop() {
                                    Some(page_id) => page_id,
                                    None => {
                                        // split reached the root, create new root
                                        self.create_new_root(
                                            cur_page,
                                            cur_key,
                                            new_right_page.expect("root creation reached without a prior split — this is a bug"),
                                            pm
                                        )?;
                                        break;
                                    }
                                }
                            } else {
                                let mut node_bytes = vec![];
                                node_bytes.push(constants::INTERNAL_NODE_TYPE);
                                internal.write_to(&mut node_bytes);
                                pm.write_page(cur_page, &node_bytes)?;

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

            Ok(())
        } else {
            // create new leaf node since root is None
            let mut leaf = LeafNode::new();
            leaf.insert(LeafEntry::new(key, row_location)); // insert new index
            let new_page = pm.allocate_page()?;

            // add the new leaf to nodes
            self.cache.insert(new_page, Node::Leaf(leaf));
            // set root to new leaf node
            self.root = Some(new_page);
            Ok(())
        }
    }

    /// finds a leaf node containing the index key
    ///
    /// returns (leaf node page id, path to the leaf node)
    fn find_leaf(
        &mut self,
        key: &IndexKey,
        start: PageId,
        pm: &mut PageManager,
    ) -> io::Result<(PageId, Vec<PageId>)> {
        let mut path = Vec::new();
        let mut cur = start;

        loop {
            match self.load_node(cur, pm)? {
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
        pm: &mut PageManager,
    ) -> io::Result<()> {
        let new_root_page = pm.allocate_page()?;
        let new_root = InternalNode::new(vec![key], vec![left, right]);
        self.cache
            .insert(new_root_page, Node::Internal(new_root.clone()));
        self.write_node(new_root_page, Node::Internal(new_root), pm)?;
        self.root = Some(new_root_page);
        Ok(())
    }

    pub fn search(
        &mut self,
        key: &IndexKey,
        pm: &mut PageManager,
    ) -> io::Result<Option<RowLocation>> {
        match self.root {
            Some(root_page_id) => {
                let (leaf_page_id, _) = self.find_leaf(key, root_page_id, pm)?;

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
    pub fn delete(&mut self, key: &IndexKey, pm: &mut PageManager) -> io::Result<()> {
        if let Some(root_page_id) = self.root {
            let (leaf_page_id, _) = self.find_leaf(key, root_page_id, pm)?;

            if let Some(node) = self.cache.get_mut(&leaf_page_id) {
                match node {
                    Node::Leaf(leaf) => match leaf.remove(key) {
                        true => {
                            // persist to disk
                            let mut buf = Vec::new();
                            buf.push(constants::LEAF_NODE_TYPE);
                            leaf.write_to(&mut buf);
                            pm.write_page(leaf_page_id, &buf)?;

                            Ok(())
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

    // this checks cache for node first
    // otherwise read from disk and cache it
    //
    // TODO: Buffer pool — the node cache grows unbounded within a session.
    // Lazy loading prevents loading nodes that are never accessed, but over
    // a long session with many queries touching different parts of the tree,
    // the cache accumulates all visited nodes with no eviction.
    // eviction policy needs to be implemented
    fn load_node(&mut self, page_id: PageId, pm: &mut PageManager) -> io::Result<&Node> {
        if !self.cache.contains_key(&page_id) {
            // read node from disk
            let node_bytes = pm.read_page(page_id)?;
            let node = Node::from_bytes(&node_bytes)?;
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

    // TODO: write_node flushes to disk on every insert/delete.
    // For bulk operations this causes O(n) disk writes on B+ tree nodes.
    // A buffer pool that caches dirty nodes and flushes in batches would
    // dramatically reduce I/O for bulk write operations.
    fn write_node(&mut self, page_id: PageId, node: Node, pm: &mut PageManager) -> io::Result<()> {
        // serialize node
        let node_bytes = node.to_bytes();
        // write to disk
        pm.write_page(page_id, &node_bytes)?;
        // update cache
        self.cache.insert(page_id, node);
        Ok(())
    }

    pub fn root(&self) -> Option<PageId> {
        self.root
    }

    pub fn range_scan(
        &mut self,
        start: Option<&IndexKey>, // None = from beginning
        end: Option<&IndexKey>,   // None = to end
        op: &BinaryOperator,      // to know if bounds are inclusive/exclusive
        pm: &mut PageManager,
    ) -> io::Result<Vec<RowLocation>> {
        let mut row_locations = Vec::new();

        if let Some(root_page_id) = self.root() {
            // if start is None, then we need the first leaf in this tree
            let start_leaf = match start {
                Some(key) => {
                    let (leaf_page_id, _) = self.find_leaf(key, root_page_id, pm)?;
                    leaf_page_id
                }
                None => {
                    // find left most leaf node
                    let mut cur = root_page_id;

                    loop {
                        match self.load_node(cur, pm)? {
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
                match self.load_node(cur_leaf, pm)? {
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
}

// return Err(Error::new(ErrorKind::InvalidData, "expected a leaf node"))

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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

    fn cleanup(basename: &str) {
        let _ = fs::remove_file(format!("{}.hdb", basename));
        let _ = fs::remove_file(format!("{}.hdb.lock", basename));
    }

    fn setup_tree(name: &str, count: i32) -> (BPlusTree, PageManager) {
        let mut pm = PageManager::new(name).unwrap();
        let mut btree = BPlusTree::new(4, &mut pm).unwrap();

        for i in 1..=count {
            btree
                .insert(
                    IndexKey::Integer(i),
                    RowLocation::new(i as u32, i as u16),
                    &mut pm,
                )
                .unwrap();
        }

        (btree, pm)
    }

    #[test]
    fn test_insert_before_split() {
        cleanup("test_insert_before_split");

        let mut pm = PageManager::new("test_insert_before_split").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        // leaf root node added to cache when creating new BPlusTree
        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        btree.insert(keys[0].clone(), rows[0], &mut pm).unwrap();
        btree.insert(keys[1].clone(), rows[1], &mut pm).unwrap();

        // leaves should still be in the same page before split
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

        let mut pm = PageManager::new("test_insert_till_leaf_split").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        // leaf root node, adding to cache when creating new BPlusTree
        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        for i in 0..btree.order {
            btree.insert(keys[i].clone(), rows[i], &mut pm).unwrap();
        }

        // multiple nodes of both internal and leaf should be present
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

        let mut pm = PageManager::new("test_insert_till_internal_node_split").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(9);

        // leaf root node, adding to cache when creating new BPlusTree
        assert_eq!(btree.cache.len(), 1);
        assert!(btree.root.is_some());

        for i in 0..btree.order {
            btree.insert(keys[i].clone(), rows[i], &mut pm).unwrap();
        }

        // multiple nodes of both internal and leaf should be present
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
                .insert(keys[i + btree.order].clone(), rows[i], &mut pm)
                .unwrap();
        }

        // internal should split, creating new root
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
        let mut pm = PageManager::new("test_search_empty_tree").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();

        let res = btree.search(&IndexKey::Integer(5), &mut pm).unwrap();

        assert!(res.is_none());
        cleanup("test_search_empty_tree");
    }

    #[test]
    fn test_search_tree_with_leaf_nodes() {
        cleanup("test_search_tree_with_leaf_nodes");

        let mut pm = PageManager::new("test_search_tree_with_leaf_nodes").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();

        let row_1_page_id = 100;
        let row_1_slot = 232;
        let row_1 = RowLocation::new(row_1_page_id, row_1_slot);

        let key_1 = IndexKey::Integer(5);

        let row_2_page_id = 200;
        let row_2_slot = 400;
        let row_2 = RowLocation::new(row_2_page_id, row_2_slot);

        let key_2 = IndexKey::Integer(15);

        btree.insert(key_1.clone(), row_1, &mut pm).unwrap();
        btree.insert(key_2.clone(), row_2, &mut pm).unwrap();

        let row_res = btree.search(&key_2, &mut pm).unwrap().unwrap();

        assert_eq!(row_res, row_2);

        let row_res = btree.search(&key_1, &mut pm).unwrap().unwrap();

        assert_eq!(row_res, row_1);

        cleanup("test_search_tree_with_leaf_nodes");
    }

    #[test]
    fn test_search_tree_with_internal_nodes() {
        cleanup("test_search_tree_with_internal_nodes");

        let mut pm = PageManager::new("test_search_tree_with_internal_nodes").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();

        let rows = get_rows(15);
        let keys = get_integer_keys(15);

        for i in 0..15 {
            btree.insert(keys[i].clone(), rows[i], &mut pm).unwrap();
        }

        let row_1_page_id = 100;
        let row_1_slot = 232;
        let row_1 = RowLocation::new(row_1_page_id, row_1_slot);

        let key_1 = IndexKey::Integer(30);

        let row_2_page_id = 200;
        let row_2_slot = 400;
        let row_2 = RowLocation::new(row_2_page_id, row_2_slot);

        let key_2 = IndexKey::Integer(27);

        btree.insert(key_1.clone(), row_1, &mut pm).unwrap();
        btree.insert(key_2.clone(), row_2, &mut pm).unwrap();

        let row_res = btree.search(&key_2, &mut pm).unwrap().unwrap();

        assert_eq!(row_res, row_2);

        let row_res = btree.search(&key_1, &mut pm).unwrap().unwrap();

        assert_eq!(row_res, row_1);

        cleanup("test_search_tree_with_internal_nodes");
    }

    #[test]
    fn test_delete_key() {
        cleanup("test_delete_key");

        let mut pm = PageManager::new("test_delete_key").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();

        let rows = get_rows(15);
        let keys = get_integer_keys(15);

        for i in 0..15 {
            btree.insert(keys[i].clone(), rows[i], &mut pm).unwrap();
        }

        // delete existing key
        btree.delete(&IndexKey::Integer(1), &mut pm).unwrap();
        assert!(
            btree
                .search(&IndexKey::Integer(1), &mut pm)
                .unwrap()
                .is_none()
        );

        // delete non existing key
        assert!(btree.delete(&IndexKey::Integer(99), &mut pm).is_err());

        // check delete didn't corrupt tree
        for i in 1..=15 {
            if i == 1 {
                continue;
            }

            assert!(
                btree
                    .search(&IndexKey::Integer(i), &mut pm)
                    .unwrap()
                    .is_some()
            );
        }

        cleanup("test_delete_key");
    }

    #[test]
    fn test_load_node_and_write_node() {
        cleanup("test_load_node");

        let mut pm = PageManager::new("test_load_node").unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();
        let node = Node::Leaf(LeafNode::new());
        let key = IndexKey::Integer(5);
        let row = RowLocation::new(99, 99);

        // insert into btree so the next page (1) get assigned
        btree.insert(key, row, &mut pm).unwrap();

        // a valid page on disk
        let cur_page = 1;

        // try loading a non existing node - should return an err
        let page_id = cur_page + 99; // invalid page
        assert!(btree.load_node(page_id, &mut pm).is_err());

        // let's write to current node page
        btree.write_node(cur_page, node.clone(), &mut pm).unwrap();

        // load page
        let res = btree.load_node(cur_page, &mut pm);
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
            let mut pm = PageManager::new("test_load_b_plus_tree").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            // insert into btree so the next page (1) get assigned
            btree.insert(key.clone(), row, &mut pm).unwrap();
            assert_eq!(btree.cache.len(), 1);
            _root_page_id = btree.root.unwrap();
        }

        {
            let mut pm = PageManager::new("test_load_b_plus_tree").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0); // should be empty

            let res = btree.search(&key, &mut pm).unwrap();
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
            let mut pm = PageManager::new("test_load_b_plus_tree_internal_node_integer").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            let keys = get_integer_keys(15);
            let rows = get_rows(15);

            for (key, row) in keys.iter().zip(rows) {
                btree.insert(key.clone(), row, &mut pm).unwrap();
            }

            // insert key to search and it's row
            btree
                .insert(key_to_search.clone(), expected_row, &mut pm)
                .unwrap();

            // cache should be > 1, since there should be some split
            assert!(btree.cache.len() > 1);
            _root_page_id = btree.root.unwrap();
        }

        {
            let mut pm = PageManager::new("test_load_b_plus_tree_internal_node_integer").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0); // should be empty

            let res = btree.search(&key_to_search, &mut pm).unwrap();
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
            let mut pm = PageManager::new("test_load_b_plus_tree_internal_node_text").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            let rows = get_rows(15);

            for (i, row) in rows.iter().enumerate() {
                btree
                    .insert(
                        IndexKey::Text(format!("test{}@example.com", i)),
                        *row,
                        &mut pm,
                    )
                    .unwrap();
            }

            // insert key to search and it's row
            btree
                .insert(key_to_search.clone(), expected_row, &mut pm)
                .unwrap();

            // cache should be > 1, since there should be some split
            assert!(btree.cache.len() > 1);
            _root_page_id = btree.root.unwrap();
        }

        {
            let mut pm = PageManager::new("test_load_b_plus_tree_internal_node_text").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0); // should be empty

            let res = btree.search(&key_to_search, &mut pm).unwrap();
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
            let mut pm = PageManager::new("test_delete_persistence_round_trip").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            btree.insert(key_1.clone(), row_1, &mut pm).unwrap();
            btree.insert(key_2.clone(), row_2, &mut pm).unwrap();

            // verify both keys got inserted
            assert_eq!(
                btree.search(&key_1.clone(), &mut pm).unwrap().unwrap(),
                row_1
            );
            assert_eq!(
                btree.search(&key_2.clone(), &mut pm).unwrap().unwrap(),
                row_2
            );

            // delete key_1
            btree.delete(&key_1, &mut pm).unwrap();

            // verify it's deleted
            assert!(btree.search(&key_1, &mut pm).unwrap().is_none());
            assert!(btree.search(&key_2, &mut pm).unwrap().is_some());

            _root_page_id = btree.root.unwrap();
        }

        {
            let mut pm = PageManager::new("test_delete_persistence_round_trip").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);

            // verify key_1 is still deleted
            assert!(btree.search(&key_1, &mut pm).unwrap().is_none());

            // verify key 2 is unchanged
            assert_eq!(
                btree.search(&key_2.clone(), &mut pm).unwrap().unwrap(),
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

        // keys that will force a leaf split and root creation
        let keys = get_integer_keys(order + 1); // order + 1 triggers split
        let rows = get_rows(order + 1);

        {
            let mut pm = PageManager::new("test_create_new_root_persistence").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            for (key, row) in keys.iter().zip(rows.iter()) {
                btree.insert(key.clone(), *row, &mut pm).unwrap();
            }

            // root should now be an internal node after split
            let root = btree.cache.get(&btree.root.unwrap()).unwrap();
            match root {
                Node::Internal(_) => (),
                Node::Leaf(_) => panic!("expected internal root after split"),
            }

            _root_page_id = btree.root.unwrap();
        }

        {
            let mut pm = PageManager::new("test_create_new_root_persistence").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            // root page should deserialize as internal node
            let root = btree.load_node(_root_page_id, &mut pm).unwrap();
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

            // all keys should still be searchable
            for (key, row) in keys.iter().zip(rows.iter()) {
                let res = btree.search(key, &mut pm).unwrap();
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
        let count = 40; // enough to force multiple root changes
        let keys = get_integer_keys(count);
        let rows = get_rows(count);
        let mut _root_page_id = 0;
        let mut root_changes = 0;
        let mut _prev_root: Option<PageId> = None;

        {
            let mut pm = PageManager::new("test_root_changes_multiple_times").unwrap();
            let mut btree = BPlusTree::new(order, &mut pm).unwrap();

            for (key, row) in keys.iter().zip(rows.iter()) {
                let root_before = btree.root;
                btree.insert(key.clone(), *row, &mut pm).unwrap();
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
        }

        {
            let mut pm = PageManager::new("test_root_changes_multiple_times").unwrap();
            let mut btree = BPlusTree::load(_root_page_id, order);
            assert_eq!(btree.cache.len(), 0);

            // verify root is a valid internal node with keys and children
            let root = btree.load_node(_root_page_id, &mut pm).unwrap();
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

            // all keys must be findable after reload
            for (key, row) in keys.iter().zip(rows.iter()) {
                let res = btree.search(key, &mut pm).unwrap();
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

        let (mut btree, mut pm) = setup_tree(name, 10);

        let end = IndexKey::Integer(5);
        let results = btree
            .range_scan(None, Some(&end), &BinaryOperator::LessThan, &mut pm)
            .unwrap();

        // should return 1, 2, 3, 4
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

        let (mut btree, mut pm) = setup_tree(name, 10);

        let end = IndexKey::Integer(5);
        let results = btree
            .range_scan(None, Some(&end), &BinaryOperator::LessOrEqual, &mut pm)
            .unwrap();

        // should return 1, 2, 3, 4, 5
        assert_eq!(results.len(), 5);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_greater_than() {
        let name = "test_range_scan_gt";
        cleanup(name);

        let (mut btree, mut pm) = setup_tree(name, 10);

        let start = IndexKey::Integer(7);
        let results = btree
            .range_scan(Some(&start), None, &BinaryOperator::GreaterThan, &mut pm)
            .unwrap();

        // should return 8, 9, 10
        assert_eq!(results.len(), 3);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_greater_than_or_equal() {
        let name = "test_range_scan_gte";
        cleanup(name);

        let (mut btree, mut pm) = setup_tree(name, 10);

        let start = IndexKey::Integer(7);
        let results = btree
            .range_scan(Some(&start), None, &BinaryOperator::GreaterOrEqual, &mut pm)
            .unwrap();

        // should return 7, 8, 9, 10
        assert_eq!(results.len(), 4);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_no_matches() {
        let name = "test_range_scan_no_match";
        cleanup(name);

        let (mut btree, mut pm) = setup_tree(name, 10);

        let end = IndexKey::Integer(0); // nothing less than 1
        let results = btree
            .range_scan(None, Some(&end), &BinaryOperator::LessThan, &mut pm)
            .unwrap();

        assert_eq!(results.len(), 0);

        cleanup(name);
    }

    #[test]
    fn test_range_scan_all_rows_less_than_or_equal() {
        let name = "test_range_scan_all_lte";
        cleanup(name);

        let (mut btree, mut pm) = setup_tree(name, 10);

        let end = IndexKey::Integer(10);
        let results = btree
            .range_scan(None, Some(&end), &BinaryOperator::LessOrEqual, &mut pm)
            .unwrap();

        assert_eq!(results.len(), 10);

        cleanup(name);
    }

    #[test]
    #[should_panic]
    fn test_range_scan_invalid_op() {
        let name = "test_range_scan_invalid_op";
        cleanup(name);
        let (mut btree, mut pm) = setup_tree(name, 5);
        cleanup(name); // cleanup before panic
        btree
            .range_scan(None, None, &BinaryOperator::Equals, &mut pm)
            .unwrap();
    }

    #[test]
    fn test_range_scan_missing_bound_returns_error() {
        let name = "test_range_scan_missing_bound";
        cleanup(name);

        let (mut btree, mut pm) = setup_tree(name, 5);

        // LessThan without end bound
        let result = btree.range_scan(None, None, &BinaryOperator::LessThan, &mut pm);

        assert!(result.is_err());

        cleanup(name);
    }

    #[test]
    fn test_range_scan_spans_multiple_leaves() {
        let name = "test_range_scan_multi_leaf";
        cleanup(name);

        // order 3 with 15 keys guarantees multiple leaf nodes
        let mut pm = PageManager::new(name).unwrap();
        let mut btree = BPlusTree::new(3, &mut pm).unwrap();

        for i in 1..=15 {
            btree
                .insert(
                    IndexKey::Integer(i),
                    RowLocation::new(i as u32, i as u16),
                    &mut pm,
                )
                .unwrap();
        }

        let end = IndexKey::Integer(10);
        let results = btree
            .range_scan(None, Some(&end), &BinaryOperator::LessOrEqual, &mut pm)
            .unwrap();

        assert_eq!(results.len(), 10);

        cleanup(name);
    }
}
