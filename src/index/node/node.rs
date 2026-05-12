use std::io::{self, Error, ErrorKind};

use crate::{
    constants,
    index::node::{LeafNode, internal::InternalNode},
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    fn to_u8(&self) -> u8 {
        match self {
            Node::Internal(_) => constants::INTERNAL_NODE_TYPE,
            Node::Leaf(_) => constants::LEAF_NODE_TYPE,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        // add node type
        buf.push(self.to_u8());

        match self {
            Node::Leaf(leaf) => {
                // add node
                leaf.write_to(buf);
            }
            Node::Internal(internal) => {
                // add node
                internal.write_to(buf);
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let mut offset = 0;

        if bytes.len() < offset + 1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough byte for node type",
            ));
        };

        let node_type = u8::from_le_bytes([bytes[offset]]);
        offset += 1;

        match node_type {
            constants::INTERNAL_NODE_TYPE => {
                let (node, _) = InternalNode::from_bytes(&bytes[offset..])?;
                Ok(Node::Internal(node))
            }
            constants::LEAF_NODE_TYPE => {
                let (node, _) = LeafNode::from_bytes(&bytes[offset..])?;
                Ok(Node::Leaf(node))
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown value for node type: {}", node_type),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::{
        key::IndexKey,
        node::leaf::{LeafEntry, RowLocation},
    };

    use super::*;

    #[test]
    fn test_node_serialization_for_internal_node() {
        let internal = InternalNode::new(
            vec![
                IndexKey::Integer(5),
                IndexKey::Integer(10),
                IndexKey::Integer(15),
            ],
            vec![1, 2, 3, 4],
        );

        let node = Node::Internal(internal);

        let bytes = node.to_bytes();
        let internal_node = Node::from_bytes(&bytes).unwrap();
        assert_eq!(node, internal_node);
    }

    #[test]
    fn test_node_serialization_for_leaf_node() {
        let mut leaf = LeafNode::new();

        for key in 1..=3 {
            let entry = LeafEntry::new(
                IndexKey::Integer(key),
                RowLocation::new(key as u32, key as u16),
            );

            leaf.insert(entry);
        }

        let node = Node::Leaf(leaf);

        let bytes = node.to_bytes();
        let leaf_node = Node::from_bytes(&bytes).unwrap();
        assert_eq!(node, leaf_node);
    }
}
