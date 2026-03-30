use super::{Result, StoreError, VersionInfo};
use crate::store::Store;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method, Response, StatusCode, Url};
use rusty_s3::{Bucket, Credentials, S3Action};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;
use std::time::Duration;
use time::OffsetDateTime;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct S3Config {
    pub key: String,
    pub endpoint: String,
    pub secret: String,
    pub token: Option<String>,
    pub bucket: String,
    pub region: String,
    pub bucket_prefix: Option<String>,

    // Use old path-style URLs, needed to support some S3-compatible APIs (including some minio setups)
    pub path_style: bool,
}

const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(60 * 60);

pub struct S3Store {
    bucket: Bucket,
    _bucket_checked: OnceLock<()>,
    client: Client,
    credentials: Credentials,
    prefix: Option<String>,
}

/// Response from S3 ListObjectVersions API
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListVersionsResponse {
    #[serde(default)]
    version: Vec<ObjectVersion>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectVersion {
    key: String,
    version_id: String,
    is_latest: bool,
    last_modified: String,
    size: i64,
}

impl S3Store {
    pub fn new(config: S3Config) -> Self {
        let credentials = if let Some(token) = config.token {
            Credentials::new_with_token(config.key, config.secret, token)
        } else {
            Credentials::new(config.key, config.secret)
        };
        let endpoint: Url = config.endpoint.parse().expect("endpoint is a valid url");

        let path_style = if config.path_style {
            rusty_s3::UrlStyle::Path
        } else if endpoint.host_str() == Some("localhost") {
            // Since this was the old behavior before we added AWS_S3_USE_PATH_STYLE,
            // we continue to support it, but complain a bit.
            tracing::warn!("Inferring path-style URLs for localhost for backwards-compatibility. This behavior may change in the future. Set AWS_S3_USE_PATH_STYLE=true to ensure that path-style URLs are used.");
            rusty_s3::UrlStyle::Path
        } else {
            rusty_s3::UrlStyle::VirtualHost
        };

        let bucket = Bucket::new(endpoint, path_style, config.bucket, config.region)
            .expect("Url has a valid scheme and host");
        let client = Client::new();

        S3Store {
            bucket,
            _bucket_checked: OnceLock::new(),
            client,
            credentials,
            prefix: config.bucket_prefix,
        }
    }

    async fn store_request<'a, A: S3Action<'a>>(
        &self,
        method: Method,
        action: A,
        body: Option<Vec<u8>>,
    ) -> Result<Response> {
        let url = action.sign_with_time(PRESIGNED_URL_DURATION, &OffsetDateTime::now_utc());
        let mut request = self.client.request(method, url);

        request = if let Some(body) = body {
            request.body(body.to_vec())
        } else {
            request
        };

        let response = request.send().await;

        let response = match response {
            Ok(response) => response,
            Err(e) => return Err(StoreError::ConnectionError(e.to_string())),
        };

        match response.status() {
            StatusCode::OK => Ok(response),
            StatusCode::NOT_FOUND => Err(StoreError::DoesNotExist(
                "Received NOT_FOUND from S3-compatible API.".to_string(),
            )),
            StatusCode::FORBIDDEN => Err(StoreError::NotAuthorized(
                "Received FORBIDDEN from S3-compatible API.".to_string(),
            )),
            StatusCode::UNAUTHORIZED => Err(StoreError::NotAuthorized(
                "Received UNAUTHORIZED from S3-compatible API.".to_string(),
            )),
            _ => Err(StoreError::ConnectionError(format!(
                "Received {} from S3-compatible API.",
                response.status()
            ))),
        }
    }

    async fn read_response_bytes(response: Response) -> Result<Bytes> {
        match response.bytes().await {
            Ok(bytes) => Ok(bytes),
            Err(e) => Err(StoreError::ConnectionError(e.to_string())),
        }
    }

    pub async fn init(&self) -> Result<()> {
        if self._bucket_checked.get().is_some() {
            return Ok(());
        }

        let action = self.bucket.head_bucket(Some(&self.credentials));
        let result = self.store_request(Method::HEAD, action, None).await;

        match result {
            // Normally a 404 indicates that we are attempting to fetch an object that does
            // not exist, but we have only attempted to retrieve a bucket, so here it
            // indicates that the bucket does not exist.
            Err(StoreError::DoesNotExist(_)) => {
                return Err(StoreError::BucketDoesNotExist(
                    "Bucket does not exist.".to_string(),
                ))
            }
            Err(e) => return Err(e),
            Ok(response) => response,
        };

        self._bucket_checked.set(()).unwrap();
        Ok(())
    }

    fn prefixed_key(&self, key: &str) -> String {
        if let Some(path_prefix) = &self.prefix {
            format!("{}/{}", path_prefix, key)
        } else {
            key.to_string()
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let object_get = self
            .bucket
            .get_object(Some(&self.credentials), &prefixed_key);
        let response = self.store_request(Method::GET, object_get, None).await;

        match response {
            Ok(response) => {
                let result = Self::read_response_bytes(response).await?;
                Ok(Some(result.to_vec()))
            }
            Err(StoreError::DoesNotExist(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let action = self
            .bucket
            .put_object(Some(&self.credentials), &prefixed_key);
        self.store_request(Method::PUT, action, Some(value)).await?;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let action = self
            .bucket
            .delete_object(Some(&self.credentials), &prefixed_key);
        self.store_request(Method::DELETE, action, None).await?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);
        let action = self
            .bucket
            .head_object(Some(&self.credentials), &prefixed_key);
        let response = self.store_request(Method::HEAD, action, None).await;
        match response {
            Ok(_) => Ok(true),
            Err(StoreError::DoesNotExist(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // -------------------------------------------------------------------------
    // AWS Signature Version 4 – presigned GET URL
    //
    // rusty-s3 0.5 has no `ListObjectVersions` action and `Credentials` has no
    // standalone `sign` method. We implement SigV4 query-string auth ourselves
    // using the `sha2` + `hmac` crates that are already in the dependency tree.
    // The resulting presigned URL works with MinIO (local dev), AWS S3, and
    // GCS's S3-compatible API.
    // -------------------------------------------------------------------------
    fn sign_v4_presigned_get(&self, base_url: &Url, expires_secs: u64) -> Url {
        let now = Utc::now();
        let date = now.format("%Y%m%d").to_string();
        let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();

        let region = self.bucket.region();
        let access_key = self.credentials.key();
        let secret_key = self.credentials.secret();

        // Host header value (include non-default port, e.g. "minio:9000")
        let host = match (base_url.host_str(), base_url.port()) {
            (Some(h), Some(p)) => format!("{}:{}", h, p),
            (Some(h), None) => h.to_string(),
            _ => String::new(),
        };

        // Canonical URI: percent-encode each path segment but preserve '/'
        let raw_path = base_url.path();
        let canonical_uri: String = if raw_path.is_empty() {
            "/".to_string()
        } else {
            raw_path
                .split('/')
                .map(|seg| urlencoding::encode(seg).into_owned())
                .collect::<Vec<_>>()
                .join("/")
        };

        // Credential scope
        let credential_scope = format!("{}/{}/s3/aws4_request", date, region);

        // Collect existing query params from the URL (already decoded by url crate)
        let mut params: Vec<(String, String)> = base_url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        // Append required X-Amz-* signing parameters (unencoded; we encode below)
        params.push(("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()));
        params.push((
            "X-Amz-Credential".into(),
            format!("{}/{}", access_key, credential_scope),
        ));
        params.push(("X-Amz-Date".into(), datetime.clone()));
        params.push(("X-Amz-Expires".into(), expires_secs.to_string()));
        params.push(("X-Amz-SignedHeaders".into(), "host".into()));
        if let Some(token) = self.credentials.token() {
            params.push(("X-Amz-Security-Token".into(), token.to_string()));
        }

        // Sort by URI-encoded key (byte order), as required by SigV4
        params.sort_by(|a, b| {
            urlencoding::encode(&a.0).cmp(&urlencoding::encode(&b.0))
        });

        // Canonical query string: every key and value is RFC-3986 percent-encoded
        let canonical_query: String = params
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    urlencoding::encode(k),
                    urlencoding::encode(v)
                )
            })
            .collect::<Vec<_>>()
            .join("&");

        // Canonical headers (only "host" for presigned-URL style)
        let canonical_headers = format!("host:{}\n", host);
        let signed_headers = "host";

        // Canonical request
        let canonical_request = format!(
            "GET\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            canonical_uri, canonical_query, canonical_headers, signed_headers
        );

        // String to sign
        let cr_hash = HEXLOWER.encode(Sha256::digest(canonical_request.as_bytes()).as_ref());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            datetime, credential_scope, cr_hash
        );

        // Derive signing key: HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), "s3"), "aws4_request")
        let hmac_sha256 = |key: &[u8], data: &[u8]| -> Vec<u8> {
            let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        };

        let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = HEXLOWER.encode(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

        // Build final URL: canonical query + X-Amz-Signature appended at the end
        let final_query = format!("{}&X-Amz-Signature={}", canonical_query, signature);
        let mut signed_url = base_url.clone();
        signed_url.set_query(Some(&final_query));
        signed_url
    }

    // -------------------------------------------------------------------------
    // List all versions of a document key using S3 ListObjectVersions API.
    // rusty-s3 has no native action for this, so we build + sign the URL using
    // our SigV4 implementation above.
    // -------------------------------------------------------------------------
    async fn list_versions_impl(&self, key: &str) -> Result<Vec<VersionInfo>> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);

        // Build the ListObjectVersions URL: bucket base URL + ?versions&prefix=…
        let mut url = self.bucket.base_url().clone();
        url.query_pairs_mut()
            .append_pair("versions", "")
            .append_pair("prefix", &prefixed_key);

        let signed_url = self.sign_v4_presigned_get(&url, PRESIGNED_URL_DURATION.as_secs());

        let response = self
            .client
            .get(signed_url)
            .send()
            .await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(StoreError::ConnectionError(format!(
                "ListObjectVersions failed: HTTP {}",
                response.status()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| StoreError::ConnectionError(e.to_string()))?;

        // Parse the XML response (ListVersionsResult)
        let parsed: ListVersionsResponse = quick_xml::de::from_str(&body).map_err(|e| {
            StoreError::ConnectionError(format!("Failed to parse ListObjectVersions XML: {}", e))
        })?;

        let versions = parsed
            .version
            .into_iter()
            .filter(|v| v.key == prefixed_key)
            .map(|v| {
                let last_modified = DateTime::parse_from_rfc3339(&v.last_modified)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());

                VersionInfo {
                    version_id: v.version_id,
                    last_modified,
                    size: v.size as usize,
                    is_latest: v.is_latest,
                }
            })
            .collect();

        Ok(versions)
    }

    // -------------------------------------------------------------------------
    // Retrieve a specific version of an object.
    // Uses rusty-s3's GetObject action with `versionId` inserted into the
    // query-string BEFORE signing, so the signature covers the versionId param.
    // -------------------------------------------------------------------------
    async fn get_version_impl(&self, key: &str, version_id: &str) -> Result<Option<Vec<u8>>> {
        self.init().await?;
        let prefixed_key = self.prefixed_key(key);

        // Use rusty-s3's GetObject action and add versionId to the query before signing
        let mut action = self
            .bucket
            .get_object(Some(&self.credentials), &prefixed_key);
        action.query_mut().insert("versionId", version_id);

        let response = self.store_request(Method::GET, action, None).await;

        match response {
            Ok(response) => {
                let result = Self::read_response_bytes(response).await?;
                Ok(Some(result.to_vec()))
            }
            Err(StoreError::DoesNotExist(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Store for S3Store {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.set(key, value).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.remove(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }

    async fn list_versions(&self, key: &str) -> Result<Vec<VersionInfo>> {
        self.list_versions_impl(key).await
    }

    async fn get_version(&self, key: &str, version_id: &str) -> Result<Option<Vec<u8>>> {
        self.get_version_impl(key, version_id).await
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl Store for S3Store {
    async fn init(&self) -> Result<()> {
        self.init().await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.get(key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.set(key, value).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.remove(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.exists(key).await
    }

    async fn list_versions(&self, key: &str) -> Result<Vec<VersionInfo>> {
        self.list_versions_impl(key).await
    }

    async fn get_version(&self, key: &str, version_id: &str) -> Result<Option<Vec<u8>>> {
        self.get_version_impl(key, version_id).await
    }
}
