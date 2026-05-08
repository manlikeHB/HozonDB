use crate::index::node::{LeafNode, internal::InternalNode};

pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}
