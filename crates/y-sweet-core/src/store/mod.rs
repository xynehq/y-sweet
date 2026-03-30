pub mod s3;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
    #[error("Versioning not supported by store. {0}")]
    VersioningNotSupported(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Information about a specific version of an object
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// The version ID (e.g., GCS generation or S3 version ID)
    pub version_id: String,
    /// When this version was created
    pub last_modified: DateTime<Utc>,
    /// Size of the version in bytes
    pub size: usize,
    /// Whether this is the latest/current version
    pub is_latest: bool,
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait Store: 'static {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;

    /// List all versions of a key. Default implementation returns empty (no versioning).
    async fn list_versions(&self, _key: &str) -> Result<Vec<VersionInfo>> {
        Ok(Vec::new())
    }

    /// Get a specific version by version_id. Default implementation returns None.
    async fn get_version(&self, _key: &str, _version_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn remove(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;

    /// List all versions of a key. Default implementation returns empty (no versioning).
    async fn list_versions(&self, _key: &str) -> Result<Vec<VersionInfo>> {
        Ok(Vec::new())
    }

    /// Get a specific version by version_id. Default implementation returns None.
    async fn get_version(&self, _key: &str, _version_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
