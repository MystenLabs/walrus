#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub(crate) struct BlobStoreId {
    pub file_index: usize,
    pub slice_index: Option<usize>,
}

impl BlobStoreId {
    #[allow(dead_code)]
    pub fn new_file(file_index: usize) -> Self {
        Self {
            file_index,
            slice_index: None,
        }
    }
    pub fn new_file_slice(file_index: usize, slice_index: usize) -> Self {
        Self {
            file_index,
            slice_index: Some(slice_index),
        }
    }
}

impl std::fmt::Display for BlobStoreId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.slice_index {
            None => {
                write!(f, "blob_{:06}", self.file_index)
            }
            Some(slice_index) => {
                write!(f, "blob_{:06}_{slice_index:06}", self.file_index)
            }
        }
    }
}
