use async_trait::async_trait;
use std::{
    fs::{create_dir_all, read_dir, remove_file},
    path::PathBuf,
    time::SystemTime,
};
use y_sweet_core::store::{Result, Store, StoreError};

pub struct FileSystemStore {
    base_path: PathBuf,
}

impl FileSystemStore {
    pub fn new(base_path: PathBuf) -> std::result::Result<Self, std::io::Error> {
        create_dir_all(base_path.clone())?;
        Ok(Self { base_path })
    }
}

#[async_trait]
impl Store for FileSystemStore {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.base_path.join(key);
        let contents = std::fs::read(path);
        match contents {
            Ok(contents) => Ok(Some(contents)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::ConnectionError(e.to_string())),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let path = self.base_path.join(key);
        create_dir_all(path.parent().expect("Bad parent"))
            .map_err(|_| StoreError::NotAuthorized("Error creating directories".to_string()))?;
        std::fs::write(path, value)
            .map_err(|_| StoreError::NotAuthorized("Error writing file.".to_string()))?;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let path = self.base_path.join(key);
        remove_file(path)
            .map_err(|_| StoreError::NotAuthorized("Error removing file.".to_string()))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.base_path.join(key);
        Ok(path.exists())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let path = self.base_path.join(prefix);
        let read_dir = read_dir(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::DoesNotExist(path.to_string_lossy().to_string());
            }
            StoreError::ConnectionError(e.to_string())
        })?;
        let mut keys_with_modified: Vec<(String, SystemTime)> = Vec::new();
        for entry in read_dir {
            let entry = entry.map_err(|e| StoreError::ConnectionError(e.to_string()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.path().is_dir() {
                let key = format!("{}{}/data.ysweet", prefix, name);
                let file_path = self.base_path.join(&key);
                if file_path.exists() {
                    let modified = std::fs::metadata(&file_path)
                        .and_then(|m| m.modified())
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    keys_with_modified.push((key, modified));
                }
            }
        }
        keys_with_modified.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(keys_with_modified.into_iter().map(|(k, _)| k).collect())
    }
}
