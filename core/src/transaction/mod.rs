pub struct Txn {
    id: u64,
    /// lsns of the WAL records introduced by this Txn
    lsns: Vec<u64>,
    /// Identifies if a Txn is explicitly created by calling `BEGIN` or it was implied
    ///
    /// true - txn was automatically created by the executor and will be automatically
    ///  be committed by the executor
    ///
    /// false - the txn was explicitly created by calling `BEGIN` and will be explicitly
    /// committed by calling `COMMIT`
    is_implicit: bool,
}

impl Txn {
    pub fn new(txn_id: u64, is_implicit: bool) -> Self {
        Self {
            id: txn_id,
            lsns: Vec::new(),
            is_implicit,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn lsns(&self) -> &[u64] {
        &self.lsns
    }

    pub fn is_implicit(&self) -> bool {
        self.is_implicit
    }

    pub fn add_lsn(&mut self, lsn: u64) {
        self.lsns.push(lsn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_new_implicit() {
        let txn = Txn::new(1, true);
        assert_eq!(txn.id(), 1);
        assert!(txn.is_implicit());
        assert!(txn.lsns().is_empty());
    }

    #[test]
    fn test_txn_new_explicit() {
        let txn = Txn::new(2, false);
        assert_eq!(txn.id(), 2);
        assert!(!txn.is_implicit());
    }

    #[test]
    fn test_add_lsn() {
        let mut txn = Txn::new(1, false);
        txn.add_lsn(10);
        txn.add_lsn(20);
        assert_eq!(txn.lsns(), &[10, 20]);
    }
}
