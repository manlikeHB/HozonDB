use std::io::{self, Error, ErrorKind};

use crate::{constants::PageId, index::key::IndexKey};

#[derive(Debug, PartialEq, Eq)]
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

    pub fn get_row(&self) -> RowLocation {
        self.row
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(&self.key.to_bytes());
        bytes.extend_from_slice(&self.row.to_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;
        let (key, bytes_consumed) = IndexKey::from_bytes(&bytes)?;
        offset += bytes_consumed;

        let (row, bytes_consumed) = RowLocation::from_bytes(&bytes[offset..])?;
        offset += bytes_consumed;

        let leaf_entry = LeafEntry::new(key, row);
        Ok((leaf_entry, offset))
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

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // add page_id
        bytes.extend_from_slice(&self.page_id().to_le_bytes());
        // add slot
        bytes.extend_from_slice(&self.slot().to_le_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for row's page id",
            ));
        };

        let page_id = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        if bytes.len() < offset + 2 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for row's slot",
            ));
        };

        let slot = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        Ok((RowLocation::new(page_id, slot), offset))
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

    pub fn is_full(&self, order: usize) -> bool {
        self.entry.len() >= order
    }

    pub fn entry(&self) -> &Vec<LeafEntry> {
        &self.entry
    }

    pub fn next(&self) -> Option<PageId> {
        self.next
    }

    pub fn remove(&mut self, key: &IndexKey) -> bool {
        if let Some(pos) = self.entry.iter().position(|entry| &entry.key == key) {
            self.entry.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // add entry count
        bytes.extend_from_slice(&(self.entry().len() as u32).to_le_bytes());

        // add each entry
        for entry in self.entry() {
            bytes.extend_from_slice(&entry.to_bytes());
        }

        // encode if next is some (1) or none (0) - u8 (1 byte)
        // if some, add next
        match self.next {
            Some(page_id) => {
                bytes.push(1u8);
                bytes.extend_from_slice(&page_id.to_le_bytes());
            }
            None => {
                bytes.push(0u8);
            }
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;
        let mut entry = Vec::new();

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for leaf node entry count",
            ));
        };

        let count = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        for _ in 0..count {
            let (leaf_entry, bytes_consumed) = LeafEntry::from_bytes(&bytes[offset..])?;

            entry.push(leaf_entry);
            offset += bytes_consumed;
        }

        let next = match u8::from_le_bytes([bytes[offset]]) {
            0 => {
                offset += 1;
                None
            }
            1 => {
                offset += 1;

                if bytes.len() < offset + 4 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Not enough bytes for leaf node next page id",
                    ));
                };
                let next_page_id = u32::from_le_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]);

                Some(next_page_id)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Invalid value for lead node next page id option value",
                ));
            }
        };

        let leaf_node = LeafNode { entry, next };

        Ok((leaf_node, offset))
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

    #[test]
    fn test_leaf_node_is_full() {
        let order = 4;
        let mut leaf = LeafNode::new();

        assert!(!leaf.is_full(order));

        for key in 1..=5 {
            leaf.insert(LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::default(),
            ));
        }

        assert!(leaf.entry.len() == 5);
        assert!(leaf.is_full(order));
    }

    #[test]
    fn test_remove_key() {
        let mut leaf = LeafNode::new();

        for key in 1..=5 {
            leaf.insert(LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::default(),
            ));
        }

        assert!(leaf.entry().len() == 5);
        // remove existing key
        assert!(leaf.remove(&IndexKey::Integer(3)));
        assert!(leaf.entry().len() == 4);
        // remove non existing key
        assert!(!leaf.remove(&IndexKey::Integer(99)));
        assert!(leaf.entry().len() == 4);
    }

    #[test]
    fn test_row_location_serialization() {
        let row = RowLocation::new(234, 543);

        let bytes = row.to_bytes();
        let (row_location, _) = RowLocation::from_bytes(&bytes).unwrap();

        assert_eq!(row_location, row);
    }

    #[test]
    fn test_leaf_entry_serialization() {
        let key = IndexKey::Text("hello@example.com".to_string());
        let row = RowLocation::new(234, 543);

        let leaf_entry = LeafEntry::new(key, row);

        let bytes = leaf_entry.to_bytes();
        let (entry, _) = LeafEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry, leaf_entry);
    }

    #[test]
    fn test_leaf_node_serialization_next_some_variant() {
        let mut leaf_node = LeafNode::new();

        for key in 1..=5 {
            let entry = LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::new(key as u32, key as u16),
            );

            leaf_node.insert(entry);
        }

        // split leaf guarantying next is Some
        let right_leaf_page_id = 99;
        leaf_node.split(right_leaf_page_id);

        let bytes = leaf_node.to_bytes();
        let (node, _) = LeafNode::from_bytes(&bytes).unwrap();

        assert_eq!(leaf_node, node);
    }

    #[test]
    fn test_leaf_node_serialization_next_non_variant() {
        let key = IndexKey::Text("hello@example.com".to_string());
        let row = RowLocation::new(234, 543);

        let leaf_entry = LeafEntry::new(key, row);

        let mut leaf_node = LeafNode::new();

        // single insert, next is None
        leaf_node.insert(leaf_entry);

        let bytes = leaf_node.to_bytes();
        let (node, _) = LeafNode::from_bytes(&bytes).unwrap();

        assert_eq!(leaf_node, node);
    }
}
