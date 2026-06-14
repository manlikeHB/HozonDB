use std::collections::HashSet;

use crate::constants::PageId;

pub struct QueryMetrics {
    pub duration_ms: f64,
    pub disk_reads: usize,       // cache misses — PageManager::read_page called
    pub buffer_pool_hits: usize, // pages served from frames
    pub pages_dirtied: HashSet<PageId>, // frames marked dirty by this operation
    pub rows_scanned: usize,
    pub rows_modified: usize,
}

impl QueryMetrics {
    pub fn new() -> Self {
        QueryMetrics {
            duration_ms: 0.0,
            disk_reads: 0,
            buffer_pool_hits: 0,
            pages_dirtied: HashSet::new(),
            rows_scanned: 0,
            rows_modified: 0,
        }
    }
}
