pub struct QueryMetrics {
    pub duration_ms: f64,
    pub pages_read: usize,
    pub pages_written: usize,
    pub rows_scanned: usize,
    pub rows_modified: usize,
}

impl QueryMetrics {
    pub fn new() -> Self {
        QueryMetrics {
            duration_ms: 0.0,
            pages_read: 0,
            pages_written: 0,
            rows_scanned: 0,
            rows_modified: 0,
        }
    }
}
