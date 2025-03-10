/// Error type for the Walrus SDK.
#[derive(Debug)]
pub enum Error {
    WalrusError(String),
    ClientError(String),
}
