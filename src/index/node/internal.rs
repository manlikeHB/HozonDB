use crate::{constants::PageId, index::key::IndexKey};

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
}
