#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
pub(crate) struct BlobStoreId {
    pub file_index: usize,
    pub slice_index: Option<usize>,
}

impl Display for BlobStoreId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.slice_index {
            None => {
                write!(f, "blob_{self.file_index:06}")
            }
            Some(slice_index)=> {
                write!(f, "blob_{self.file_index:06}_{slice_index:06}")
            }
        }
    }
}

impl PartialOrd for BlobIdNested {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
