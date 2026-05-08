#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum IndexKey {
    Integer(i32),
    Text(String),
}
