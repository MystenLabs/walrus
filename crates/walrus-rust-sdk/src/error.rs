/// Error type for the Walrus SDK.
#[derive(Debug)]
pub enum Error {
    /// An error occurred in the Walrus network. Please try again.
    NetworkError(String),
    /// An error occurred locally. Please check the inputs to the erroneous method.
    ClientError(String),
}
