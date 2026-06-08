use crate::{constants::PageId, storage::page::PAGE_SIZE};

#[derive(Debug, Clone)]
pub struct Frame {
    page_id: Option<PageId>,
    data: [u8; PAGE_SIZE],
    dirty: bool,
    pin_count: u32,
    last_lsn: u64,
    referenced: bool,
}

impl Frame {
    pub fn new() -> Self {
        Frame {
            page_id: None,
            data: [0u8; PAGE_SIZE],
            dirty: false,
            pin_count: 0,
            last_lsn: 0,
            referenced: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.page_id.is_none()
    }

    pub fn page_id(&self) -> Option<PageId> {
        self.page_id
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    pub fn load(&mut self, page_id: PageId, data: [u8; PAGE_SIZE]) {
        self.page_id = Some(page_id);
        self.data = data;
        self.dirty = false;
        self.pin_count = 0;
        self.last_lsn = 0;
        self.referenced = true;
    }

    pub fn mark_dirty(&mut self, lsn: u64) {
        self.dirty = true;
        self.last_lsn = lsn;
        self.referenced = true;
    }

    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1;
    }

    pub fn referenced(&self) -> bool {
        self.referenced
    }

    pub fn set_referenced(&mut self) {
        self.referenced = true;
    }

    pub fn clear_referenced(&mut self) {
        self.referenced = false;
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_frame() {
        let frame = Frame::new();

        assert!(frame.page_id.is_none());
        assert_eq!(frame.data, [0u8; PAGE_SIZE]);
        assert!(!frame.dirty);
        assert_eq!(frame.pin_count, 0);
        assert_eq!(frame.last_lsn, 0);
        assert!(frame.is_empty());
    }
}
