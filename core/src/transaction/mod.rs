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
    fn new(txn_id: u64, is_implicit: bool) -> Self {
        Self {
            id: txn_id,
            lsns: Vec::new(),
            is_implicit,
        }
    }

    /// Creates an implicit Txn
    pub fn implicit_txn(txn_id: u64) -> Self {
        Self::new(txn_id, true)
    }

    // Creates an explicit Txn when `BEGIN` is called
    pub fn explicit_txn(txn_id: u64) -> Self {
        Self::new(txn_id, false)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn lsns(&self) -> &Vec<u64> {
        &self.lsns
    }

    pub fn is_implicit(&self) -> bool {
        self.is_implicit
    }
}
