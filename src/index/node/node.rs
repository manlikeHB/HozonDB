use crate::{
    constants::PageId,
    index::{key::IndexKey, node::LeafNode},
};

pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

pub struct InternalNode {
    keys: Vec<IndexKey>,
    children: Vec<PageId>,
}
