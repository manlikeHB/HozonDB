use std::io::{self, Error, ErrorKind};

use crate::{constants::PageId, index::key::IndexKey};

#[derive(Debug, PartialEq, Eq)]
pub struct InternalNode {
    keys: Vec<IndexKey>,
    children: Vec<PageId>,
}

impl InternalNode {
    pub fn new(keys: Vec<IndexKey>, children: Vec<PageId>) -> Self {
        debug_assert_eq!(children.len(), keys.len() + 1, "children must be keys + 1");
        InternalNode { keys, children }
    }

    pub fn insert_child(&mut self, key: IndexKey, page_id: PageId) {
        let pos = self
            .keys
            .iter()
            .position(|index_key| key < *index_key)
            .unwrap_or(self.keys.len());

        self.keys.insert(pos, key);
        self.children.insert(pos + 1, page_id);
    }

    pub fn split(&mut self) -> (IndexKey, InternalNode) {
        let mid = self.keys.len() / 2;

        // split off after mid + 1, because mid key will be promoted
        let right_keys = self.keys.split_off(mid + 1);
        let right_children = self.children.split_off(mid + 1);

        let promoted_key = self.keys.remove(mid);

        (promoted_key, InternalNode::new(right_keys, right_children))
    }

    pub fn is_full(&self, order: usize) -> bool {
        self.keys.len() >= order
    }

    pub fn keys(&self) -> &Vec<IndexKey> {
        &self.keys
    }

    pub fn children(&self) -> &Vec<PageId> {
        &self.children
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // add key count
        bytes.extend_from_slice(&(self.keys().len() as u32).to_le_bytes());
        // add keys
        for key in self.keys() {
            bytes.extend_from_slice(&key.to_bytes());
        }

        // add children count
        bytes.extend_from_slice(&(self.children().len() as u32).to_le_bytes());
        // add children
        for child in self.children() {
            bytes.extend_from_slice(&child.to_le_bytes());
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<(Self, usize)> {
        let mut offset = 0;
        let mut keys = Vec::new();
        let mut children = Vec::new();

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for Internal node keys count",
            ));
        };

        let keys_count = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        for _ in 0..keys_count {
            let (key, bytes_consumed) = IndexKey::from_bytes(&bytes[offset..])?;
            keys.push(key);
            offset += bytes_consumed;
        }

        if bytes.len() < offset + 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough bytes for Internal node children count",
            ));
        };

        let children_count = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        for _ in 0..children_count {
            let child = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            children.push(child);
            offset += 4;
        }

        Ok((Self::new(keys, children), offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_child_integer() {
        let mut internal = InternalNode::new(vec![IndexKey::Integer(10)], vec![1, 2]);

        // insert 5
        let key = IndexKey::Integer(5);
        let page_id = 5;
        internal.insert_child(key.clone(), page_id);

        assert_eq!(internal.keys[0], key);
        assert_eq!(internal.children[1], page_id);
        assert_eq!(internal.children.len(), internal.keys.len() + 1);

        // insert 15
        let key = IndexKey::Integer(15);
        let page_id = 9;
        internal.insert_child(key.clone(), page_id);

        assert_eq!(internal.keys.last().unwrap().clone(), key);
        assert_eq!(internal.children[internal.keys.len()], page_id);
        assert_eq!(internal.children.len(), internal.keys.len() + 1);
    }

    #[test]
    fn test_insert_child_text() {
        let mut internal = InternalNode::new(vec![IndexKey::Text("alan".to_string())], vec![1, 2]);

        // insert aaron
        let key = IndexKey::Text("aaron".to_string());
        let page_id = 5;
        internal.insert_child(key.clone(), page_id);

        assert_eq!(internal.keys[0], key);
        assert_eq!(internal.children[1], page_id);
        assert_eq!(internal.children.len(), internal.keys.len() + 1);

        // insert mike
        let key = IndexKey::Text("mike".to_string());
        let page_id = 9;
        internal.insert_child(key.clone(), page_id);

        assert_eq!(internal.keys.last().unwrap().clone(), key);
        assert_eq!(internal.children[internal.keys.len()], page_id);
        assert_eq!(internal.children.len(), internal.keys.len() + 1);
    }

    #[test]
    fn test_split_internal_node() {
        let mut internal = InternalNode::new(
            vec![
                IndexKey::Integer(5),
                IndexKey::Integer(10),
                IndexKey::Integer(15),
            ],
            vec![1, 2, 3, 4],
        );

        let (split_key, right_node) = internal.split();

        assert_eq!(internal.keys.len(), 1);
        assert_eq!(internal.keys[0], IndexKey::Integer(5));
        assert_eq!(internal.children.len(), 2);
        assert_eq!(internal.children[0], 1);
        assert_eq!(internal.children[1], 2);
        assert_eq!(split_key, IndexKey::Integer(10));
        assert_eq!(right_node.keys.len(), 1);
        assert_eq!(right_node.keys[0], IndexKey::Integer(15));
        assert_eq!(right_node.children.len(), 2);
        assert_eq!(right_node.children[0], 3);
        assert_eq!(right_node.children[1], 4);
    }

    #[test]
    fn test_is_full_internal_node() {
        let internal = InternalNode::new(
            vec![
                IndexKey::Integer(5),
                IndexKey::Integer(10),
                IndexKey::Integer(15),
            ],
            vec![1, 2, 3, 4],
        );

        assert!(internal.is_full(2));
    }

    #[test]
    fn test_internal_node_serialization() {
        let internal = InternalNode::new(
            vec![
                IndexKey::Integer(5),
                IndexKey::Integer(10),
                IndexKey::Integer(15),
            ],
            vec![1, 2, 3, 4],
        );

        let bytes = internal.to_bytes();
        let (internal_node, _) = InternalNode::from_bytes(&bytes).unwrap();

        assert_eq!(internal_node, internal);
    }
}
