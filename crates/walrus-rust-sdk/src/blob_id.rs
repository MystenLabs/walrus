
pub struct BlobId(pub [u8; 32]);

impl AsRef<[u8]> for BlobId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Base64Display::new(self.as_ref(), &URL_SAFE_NO_PAD).fmt(f)
    }
}

impl Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({self})")
    }
}

/// Error returned when unable to parse a blob ID.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("failed to parse a blob ID")]
pub struct BlobIdParseError;

impl TryFrom<&[u8]> for BlobId {
    type Error = BlobIdParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes = <[u8; Self::LENGTH]>::try_from(value).map_err(|_| BlobIdParseError)?;
        Ok(Self(bytes))
    }
}

impl FromStr for BlobId {
    type Err = BlobIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut blob_id = Self([0; Self::LENGTH]);
        if let Ok(Self::LENGTH) = URL_SAFE_NO_PAD.decode_slice(s, &mut blob_id.0) {
            Ok(blob_id)
        } else {
            Err(BlobIdParseError)
        }
    }
}


