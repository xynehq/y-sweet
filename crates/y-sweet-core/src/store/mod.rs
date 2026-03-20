pub mod s3;

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Store bucket does not exist. {0}")]
    BucketDoesNotExist(String),
    #[error("Object does not exist. {0}")]
    DoesNotExist(String),
    #[error("Not authorized to access store. {0}")]
    NotAuthorized(String),
    #[error("Error connecting to store. {0}")]
    ConnectionError(String),
    #[error("Operation not supported. {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Returns the storage key for a document's main data blob.
pub fn doc_data_key(doc_id: &str) -> String {
    format!("{}/data.ysweet", doc_id)
}

/// Returns the storage key for a snapshot. Snapshot id is typically epoch milliseconds (sortable, unique).
pub fn snapshot_key(doc_id: &str, snapshot_id: &str) -> String {
    format!("{}/snapshots/{}/data.ysweet", doc_id, snapshot_id)
}

/// Returns the prefix to list snapshots for a document (e.g. "doc_id/snapshots/").
/// List results are keys under this prefix; each key is "doc_id/snapshots/{id}/data.ysweet".
pub fn snapshot_prefix(doc_id: &str) -> String {
    format!("{}/snapshots/", doc_id)
}

/// Valid snapshot ids: non-empty, reasonable length, only ASCII digits (e.g. epoch milliseconds).
/// Reject anything that could be used for path traversal or invalid keys.
pub fn is_valid_snapshot_timestamp(ts: &str) -> bool {
    !ts.is_empty()
        && ts.len() <= 20
        && ts.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait Store: 'static {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;
    /// List keys under the given prefix in **oldest-first order by creation/last-modified time**.
    /// Returns full keys (e.g. "doc_id/snapshots/ts1/data.ysweet"). Callers can delete the first N to retain only the newest.
    /// Stores that do not support listing return Err(StoreError::Unsupported).
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;
    /// List keys under the given prefix in **oldest-first order by creation/last-modified time**.
    /// Returns full keys (e.g. "doc_id/snapshots/ts1/data.ysweet"). Callers can delete the first N to retain only the newest.
    /// Stores that do not support listing return Err(StoreError::Unsupported).
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
