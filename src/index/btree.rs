use std::{
    collections::HashMap,
    io::{self, Error, ErrorKind},
};

use crate::{
    constants::PageId,
    index::{
        key::IndexKey,
        node::{
            InternalNode, LeafNode, Node,
            leaf::{LeafEntry, RowLocation},
        },
    },
};

pub struct BPlusTree {
    root: Option<PageId>,
    order: usize,
    nodes: HashMap<PageId, Node>,
    next_page_id: u32,
}

impl BPlusTree {
    pub fn new() -> Self {
        BPlusTree {
            root: None,
            order: 3,
            nodes: HashMap::new(),
            next_page_id: 1,
        }
    }

    pub fn insert(&mut self, key: IndexKey, row_location: RowLocation) -> io::Result<()> {
        if let Some(page_id) = self.root {
            let (leaf_page_id, mut path) = self.find_leaf(&key, page_id)?;

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
                if let Some(node) = self.nodes.get_mut(&cur_page) {
                    match node {
                        Node::Leaf(leaf) => {
                            leaf.insert(LeafEntry::new(cur_key.clone(), row_location));

                            if leaf.is_full(self.order) {
                                // assign new page
                                let new_page = self.next_page_id;
                                self.next_page_id += 1;

                                // split leaf
                                let (k, new_leaf) = leaf.split(new_page);

                                // add leaf node to the node list
                                self.nodes.insert(new_page, Node::Leaf(new_leaf));

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
                                        );
                                        break;
                                    }
                                }
                            } else {
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
                                let new_page = self.next_page_id;
                                self.next_page_id += 1;

                                // split internal node
                                let (k, new_internal) = internal.split();

                                // add to node list
                                self.nodes.insert(new_page, Node::Internal(new_internal));

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
                                        );
                                        break;
                                    }
                                }
                            } else {
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
            let new_page = self.next_page_id;
            self.next_page_id += 1;

            // add the new leaf to nodes
            self.nodes.insert(new_page, Node::Leaf(leaf));
            // set root to new leaf node
            self.root = Some(new_page);
            Ok(())
        }
    }

    fn find_leaf(&self, key: &IndexKey, start: PageId) -> io::Result<(PageId, Vec<PageId>)> {
        let mut path = Vec::new();
        let mut cur = start;
        loop {
            match self.nodes.get(&cur) {
                Some(Node::Internal(internal)) => {
                    path.push(cur);
                    let pos = internal
                        .keys()
                        .iter()
                        .position(|k| key <= k)
                        .unwrap_or(internal.keys().len());
                    cur = internal.children()[pos];
                }
                Some(Node::Leaf(_)) => return Ok((cur, path)),
                None => {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        format!("page {} not found in node map", cur),
                    ));
                }
            }
        }
    }

    fn create_new_root(&mut self, left: PageId, key: IndexKey, right: PageId) {
        let new_root_page = self.next_page_id;
        self.next_page_id += 1;
        let new_root = InternalNode::new(vec![key], vec![left, right]);
        self.nodes.insert(new_root_page, Node::Internal(new_root));
        self.root = Some(new_root_page);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_rows(order: usize) -> Vec<RowLocation> {
        let mut rows = Vec::new();
        let page_id = 99;
        for i in 1..=order {
            let row = RowLocation::new((page_id + i) as u32, (page_id + i) as u16);
            rows.push(row);
        }

        rows
    }

    fn get_integer_keys(count: usize) -> Vec<IndexKey> {
        (1..=count).map(|i| IndexKey::Integer(i as i32)).collect()
    }

    #[test]
    fn test_insert_before_split() {
        let mut btree = BPlusTree::new();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        assert_eq!(btree.next_page_id, 1);
        assert_eq!(btree.nodes.len(), 0);
        assert!(btree.root.is_none());

        btree.insert(keys[0].clone(), rows[0]).unwrap();
        btree.insert(keys[1].clone(), rows[1]).unwrap();

        // leaves should still be in the same page before split
        assert_eq!(btree.next_page_id, 2);
        assert_eq!(btree.nodes.len(), 1);
        assert!(btree.root.is_some());

        let node = btree.nodes.get(&btree.root.unwrap()).unwrap();

        match node {
            Node::Leaf(_) => (),
            Node::Internal(_) => panic!("expected a leaf node before split"),
        }
    }

    #[test]
    fn test_insert_till_leaf_split() {
        let mut btree = BPlusTree::new();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(btree.order);

        assert_eq!(btree.next_page_id, 1);
        assert_eq!(btree.nodes.len(), 0);
        assert!(btree.root.is_none());

        for i in 0..btree.order {
            btree.insert(keys[i].clone(), rows[i]).unwrap();
        }

        // multiple nodes of both internal and leaf should be present
        assert_eq!(btree.next_page_id, 4);
        assert_eq!(btree.nodes.len(), 3);
        assert!(btree.root.is_some());

        let node = btree.nodes.get(&btree.root.unwrap()).unwrap();

        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }
    }

    #[test]
    fn test_insert_till_internal_node_split() {
        let mut btree = BPlusTree::new();
        let rows = get_rows(btree.order);
        let keys = get_integer_keys(9);

        assert_eq!(btree.next_page_id, 1);
        assert_eq!(btree.nodes.len(), 0);
        assert!(btree.root.is_none());

        for i in 0..btree.order {
            btree.insert(keys[i].clone(), rows[i]).unwrap();
        }

        // multiple nodes of both internal and leaf should be present
        assert_eq!(btree.next_page_id, 4);
        assert_eq!(btree.nodes.len(), 3);
        assert!(btree.root.is_some());

        let before_root = btree.root.unwrap();

        let node = btree.nodes.get(&btree.root.unwrap()).unwrap();

        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }

        for i in 0..btree.order {
            btree
                .insert(keys[i + btree.order].clone(), rows[i])
                .unwrap();
        }

        // internal should split, creating new root
        assert!(btree.root.is_some());

        let after_root = btree.root.unwrap();
        assert_ne!(before_root, after_root);

        let node = btree.nodes.get(&btree.root.unwrap()).unwrap();

        match node {
            Node::Leaf(_) => panic!("expected an internal node after leaf split"),
            Node::Internal(_) => (),
        }
    }
}
