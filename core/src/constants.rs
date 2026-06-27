pub const TABLE_CATALOG_PAGE_ID: u32 = 1;
pub const INDEX_CATALOG_PAGE_ID: u32 = 2;

// page
pub type PageId = u32;
pub const OFFSET_RAW_PAGE_START: usize = 8; // Metadata offsets raw page (non-slotted)

// btree
pub const BTREE_INTEGER_ORDER: usize = 371;
pub const BTREE_TEXT_ORDER: usize = 15;

// index
pub const MAX_TEXT_INDEX_KEY_BYTES: usize = 255;

// Node data type byte representation
pub const INTERNAL_NODE_TYPE: u8 = 1;
pub const LEAF_NODE_TYPE: u8 = 2;

pub const DEFAULT_BUFFER_POOL_CAPACITY: usize = 1024; // 4MB

// WAL
pub const WAL_RECORD_SLOTTED_TYPE: u8 = 1;
pub const WAL_RECORD_RAW_TYPE: u8 = 2;
pub const WAL_RECORD_CHECKPOINT_TYPE: u8 = 3;
pub const WAL_RECORD_LINK_PAGE_TYPE: u8 = 4;
pub const WAL_RECORD_ALLOCATE_PAGE_TYPE: u8 = 5;
pub const WAL_RECORD_ABORT_TYPE: u8 = 6;

pub type Lsn = u64;
pub type TxnId = u64;

// Sentinel value for Option::None
pub const NONE_U32: u32 = u32::MAX;
pub const NONE_U64: u64 = u64::MAX;
