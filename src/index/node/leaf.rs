use crate::{constants::PageId, index::key::IndexKey};

pub struct LeafNode {
    entry: Vec<LeafEntry>,
    next: Option<PageId>,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct LeafEntry {
    key: IndexKey,
    row: RowLocation,
}

impl LeafEntry {
    pub fn new(key: IndexKey, row: RowLocation) -> Self {
        LeafEntry { key, row }
    }

    pub fn get_key(&self) -> &IndexKey {
        &self.key
    }

    pub fn get_row(self) -> RowLocation {
        self.row
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub struct RowLocation {
    page_id: PageId,
    slot: u16,
}

impl RowLocation {
    pub fn new(page_id: PageId, slot: u16) -> Self {
        RowLocation { page_id, slot }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn slot(&self) -> u16 {
        self.slot
    }
}

impl Default for RowLocation {
    fn default() -> Self {
        RowLocation {
            page_id: Default::default(),
            slot: Default::default(),
        }
    }
}

impl LeafNode {
    pub fn new() -> Self {
        LeafNode {
            entry: vec![],
            next: None,
        }
    }

    pub fn insert(&mut self, new_entry: LeafEntry) {
        let pos = self
            .entry
            .iter()
            .position(|e| new_entry.key < e.key)
            .unwrap_or(self.entry.len());
        self.entry.insert(pos, new_entry);
    }

    pub fn split(&mut self, right_leaf_page_id: PageId) -> (IndexKey, LeafNode) {
        let mid = self.entry.len() / 2;

        let right_leaf = self.entry.split_off(mid);
        let split_key = right_leaf[0].key.clone();

        let old_next = self.next.take();

        self.next = Some(right_leaf_page_id);

        let new_leaf_node = LeafNode {
            entry: right_leaf,
            next: old_next,
        };

        (split_key, new_leaf_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_leaf_node_for_integer() {
        let mut leaf = LeafNode::new();

        // insert into empty leaf node (key = 5)
        let entry_key_5 = LeafEntry::new(IndexKey::Integer(5), RowLocation::default());
        leaf.insert(entry_key_5.clone());
        assert_eq!(leaf.entry.len(), 1);
        assert_eq!(leaf.entry[0], entry_key_5);

        // insert into empty leaf node (key = 3) // less than first key
        let entry_key_3 = LeafEntry::new(IndexKey::Integer(3), RowLocation::default());
        leaf.insert(entry_key_3.clone());
        assert_eq!(leaf.entry.len(), 2);
        assert_eq!(leaf.entry[0], entry_key_3);

        // insert into empty leaf node (key = 10) // key should be last in node
        let entry_key_10 = LeafEntry::new(IndexKey::Integer(10), RowLocation::default());
        leaf.insert(entry_key_10.clone());
        assert_eq!(leaf.entry.len(), 3);
        assert_eq!(leaf.entry[2], entry_key_10);
    }

    #[test]
    fn test_insert_leaf_node_for_text() {
        let mut leaf = LeafNode::new();

        // insert into empty leaf node (key = apple)
        let entry_key_apple =
            LeafEntry::new(IndexKey::Text("apple".to_string()), RowLocation::default());
        leaf.insert(entry_key_apple.clone());
        assert_eq!(leaf.entry.len(), 1);
        assert_eq!(leaf.entry[0], entry_key_apple);

        // insert into empty leaf node (key = aaron) // should come before first key
        let entry_key_aaron =
            LeafEntry::new(IndexKey::Text("aaron".to_string()), RowLocation::default());
        leaf.insert(entry_key_aaron.clone());
        assert_eq!(leaf.entry.len(), 2);
        assert_eq!(leaf.entry[0], entry_key_aaron);

        // insert into empty leaf node (key = mike) // key should be last in node
        let entry_key_mike =
            LeafEntry::new(IndexKey::Text("mike".to_string()), RowLocation::default());
        leaf.insert(entry_key_mike.clone());
        assert_eq!(leaf.entry.len(), 3);
        assert_eq!(leaf.entry[2], entry_key_mike);
    }

    #[test]
    fn test_split_leaf_node() {
        let mut leaf = LeafNode::new();
        for key in 1..=5 {
            leaf.insert(LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::default(),
            ));
        }

        assert!(leaf.entry.len() == 5);

        let right_leaf_page_id = 25_u32;
        let (split_key, right_leaf) = leaf.split(right_leaf_page_id);

        assert_eq!(leaf.entry.len(), 2);
        assert_eq!(leaf.next, Some(right_leaf_page_id));
        assert_eq!(right_leaf.entry.len(), 3);
        assert_eq!(split_key, right_leaf.entry[0].key);
        assert_eq!(right_leaf.next, None);
    }

    #[test]
    fn test_row_location_persist_across_ops() {
        let mut leaf = LeafNode::new();

        for key in 1..=5 {
            let entry = LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::new(key as u32, key as u16),
            );

            leaf.insert(entry);
        }

        for (pos, entry) in leaf.entry.iter().enumerate() {
            assert_eq!(entry.key, IndexKey::Integer(pos as i32 + 1));
            assert_eq!(entry.row, RowLocation::new(pos as u32 + 1, pos as u16 + 1));
        }

        let right_leaf_page_id = 98;
        let (_, new_leaf) = leaf.split(right_leaf_page_id);

        // leaf is split now containing 2 leaf entries
        for (pos, entry) in leaf.entry.iter().enumerate() {
            assert_eq!(entry.key, IndexKey::Integer(pos as i32 + 1));
            assert_eq!(entry.row, RowLocation::new(pos as u32 + 1, pos as u16 + 1));
        }

        // new leaf contains the remaining 3 leaf entries
        for (pos, entry) in new_leaf.entry.iter().enumerate() {
            assert_eq!(entry.key, IndexKey::Integer(pos as i32 + 3));
            assert_eq!(entry.row, RowLocation::new(pos as u32 + 3, pos as u16 + 3));
        }
    }

    #[test]
    fn test_leaf_entry_methods() {
        let key = IndexKey::Integer(5);
        let row = RowLocation::new(1, 16);
        let leaf_entry = LeafEntry::new(key.clone(), row);

        assert_eq!(leaf_entry.get_key(), &key);
        assert_eq!(leaf_entry.get_row(), row);
    }

    #[test]
    fn test_row_location_methods() {
        let page_id = 1;
        let slot = 23_u16;
        let row = RowLocation::new(page_id, slot);

        assert_eq!(row.page_id(), page_id);
        assert_eq!(row.slot(), slot);
    }
}
