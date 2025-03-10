/// Error type for the Walrus SDK.
#[derive(Debug, Clone)]
pub enum Error {
    /// An error occurred in the Walrus network. Please try again.
    NetworkError(String),
    /// An error occurred locally. Please check the inputs to the erroneous method.
    ClientError(String),
    /// The operation failed due to lack of funds. Please add more coins to your account.
    InsufficientFundsError(Coin),
}

/// A type representing the two coins used by the Walrus network.
#[derive(Debug, Clone)]
pub enum Coin {
    Sui,
    Wal,
}
