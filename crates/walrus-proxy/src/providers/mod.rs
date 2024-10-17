/// walrus specific provider implementation for nodes in committee
pub mod walrus;

// Re-export for easier access
pub use crate::providers::walrus::provider::WalrusNodeProvider;
pub use crate::providers::walrus::query::{get_walrus_committee, NodeInfo};
