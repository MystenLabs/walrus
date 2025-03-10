use std::fmt;

use base64::{display::Base64Display, engine::general_purpose::URL_SAFE_NO_PAD, Engine};

/// A unique identifier for a blob stored in the Walrus network.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct BlobId(pub [u8; BlobId::LENGTH]);

impl BlobId {
    /// The length of a blob ID in bytes is constant.
    pub const LENGTH: usize = 32;
}

impl AsRef<[u8]> for BlobId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Base64Display::new(self.as_ref(), &URL_SAFE_NO_PAD).fmt(f)
    }
}

/// Error returned when unable to parse a blob ID.
#[derive(Debug, PartialEq, Eq)]
pub struct BlobIdParseError;

impl fmt::Display for BlobIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unable to parse Walrus Blob ID")
    }
}
impl std::error::Error for BlobIdParseError {}

impl TryFrom<&[u8]> for BlobId {
    type Error = BlobIdParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes = <[u8; Self::LENGTH]>::try_from(value).map_err(|_| BlobIdParseError)?;
        Ok(Self(bytes))
    }
}

impl std::str::FromStr for BlobId {
    type Err = BlobIdParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut blob_id = Self([0; Self::LENGTH]);
        if let Ok(Self::LENGTH) = URL_SAFE_NO_PAD.decode_slice(input, &mut blob_id.0) {
            Ok(blob_id)
        } else {
            Err(BlobIdParseError)
        }
    }
}
