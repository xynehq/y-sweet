use anyhow::{anyhow, Result};
use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket},
        DefaultBodyLimit, Path, Query, Request, State, WebSocketUpgrade,
    },
    http::{
        header::{HeaderMap, HeaderName},
        StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_extra::typed_header::TypedHeader;
use dashmap::{mapref::one::MappedRef, DashMap};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{span, Instrument, Level};
use url::Url;
use y_sweet_core::{
    api_types::{
        validate_doc_name, AuthDocRequest, Authorization, ClientToken, DocCreationRequest,
        NewDocResponse,
    },
    auth::{Authenticator, ExpirationTimeEpochMillis, DEFAULT_EXPIRATION_SECONDS},
    doc_connection::DocConnection,
    doc_sync::{self, DocWithSyncKv},
    store::{self, Store, StoreError},
    sync::awareness::Awareness,
    sync_kv::SyncKv,
};

const PLANE_VERIFIED_USER_DATA_HEADER: &str = "x-verified-user-data";

// Every 20 seconds, we send a ping to the client.
const PING_EVERY: Duration = Duration::from_secs(20);
// If we haven't received a pong in the last 40 seconds, we close the connection.
// All modern browsers will respond to websocket pings with a pong message.
const PONG_TIMEOUT: Duration = Duration::from_secs(40);

fn current_time_epoch_millis() -> u64 {
    let now = std::time::SystemTime::now();
    let duration_since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    duration_since_epoch.as_millis() as u64
}

/// Retention: keep only the latest N snapshots (by count). Deletes by store creation order (oldest first).
/// list_prefix returns keys oldest-first; we delete the first (len - max_count) keys. No sorting in our code.
async fn apply_snapshot_retention_impl(
    store: &Arc<Box<dyn Store>>,
    doc_id: &str,
    retention_max: Option<usize>,
) {
    let max_count = match retention_max {
        Some(n) if n > 0 => n,
        _ => return,
    };
    let prefix = store::snapshot_prefix(doc_id);
    let keys = match store.list_prefix(&prefix).await {
        Ok(k) => k,
        Err(StoreError::Unsupported(_)) => return,
        Err(_) => return,
    };
    if keys.len() <= max_count {
        return;
    }
    let n_remove = keys.len() - max_count;
    for key in keys.into_iter().take(n_remove) {
        let _ = store.remove(&key).await;
    }
}

/// Context for on-persist snapshot creation (throttled). Passed into doc_persistence_worker when on_persist is enabled.
struct OnPersistSnapshotCtx {
    store: Arc<Box<dyn Store>>,
    doc_id: String,
    interval_secs: u64,
    retention_max: Option<usize>,
    last_snapshot: Arc<DashMap<String, Instant>>,
}

/// Context for on-GC snapshot (create snapshot when doc is evicted). Passed into doc_gc_worker when on_gc is enabled.
struct OnGcSnapshotCtx {
    store: Arc<Box<dyn Store>>,
    doc_id: String,
    retention_max: Option<usize>,
}

#[derive(Debug)]
pub struct AppError(StatusCode, anyhow::Error);
impl std::error::Error for AppError {}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, format!("Something went wrong: {}", self.1)).into_response()
    }
}
impl<E> From<(StatusCode, E)> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from((status_code, err): (StatusCode, E)) -> Self {
        Self(status_code, err.into())
    }
}
impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Status code: {} {}", self.0, self.1)?;
        Ok(())
    }
}

/// Snapshot feature configuration (ENV-driven).
#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    pub enabled: bool,
    pub manual: bool,
    pub before_rollback: bool,
    pub on_persist: bool,
    pub on_persist_interval_secs: u64,
    pub on_gc: bool,
    /// Max number of snapshots to keep per doc (oldest removed). Numbers-only retention; no time-based pruning.
    pub retention_max: Option<usize>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            manual: false,
            before_rollback: true,
            on_persist: false,
            on_persist_interval_secs: 3600,
            on_gc: false,
            retention_max: None,
        }
    }
}

pub struct Server {
    docs: Arc<DashMap<String, DocWithSyncKv>>,
    doc_worker_tracker: TaskTracker,
    store: Option<Arc<Box<dyn Store>>>,
    checkpoint_freq: Duration,
    authenticator: Option<Authenticator>,
    url_prefix: Option<Url>,
    cancellation_token: CancellationToken,
    /// Whether to garbage collect docs that are no longer in use.
    /// Disabled for single-doc mode, since we only have one doc.
    doc_gc: bool,
    max_body_size: Option<usize>,
    /// Whether to skip garbage collection in Yrs documents.
    skip_gc: bool,
    snapshot_config: SnapshotConfig,
    /// Last time we created an on-persist snapshot per doc_id (for throttling).
    last_snapshot_per_doc: Arc<DashMap<String, Instant>>,
}

impl Server {
    pub async fn new(
        store: Option<Box<dyn Store>>,
        checkpoint_freq: Duration,
        authenticator: Option<Authenticator>,
        url_prefix: Option<Url>,
        cancellation_token: CancellationToken,
        doc_gc: bool,
        max_body_size: Option<usize>,
        skip_gc: bool,
        snapshot_config: SnapshotConfig,
    ) -> Result<Self> {
        Ok(Self {
            docs: Arc::new(DashMap::new()),
            doc_worker_tracker: TaskTracker::new(),
            store: store.map(Arc::new),
            checkpoint_freq,
            authenticator,
            url_prefix,
            cancellation_token,
            doc_gc,
            max_body_size,
            skip_gc,
            snapshot_config: snapshot_config.clone(),
            last_snapshot_per_doc: Arc::new(DashMap::new()),
        })
    }

    pub fn snapshot_config(&self) -> &SnapshotConfig {
        &self.snapshot_config
    }

    pub async fn doc_exists(&self, doc_id: &str) -> bool {
        if self.docs.contains_key(doc_id) {
            return true;
        }
        if let Some(store) = &self.store {
            store
                .exists(&store::doc_data_key(doc_id))
                .await
                .unwrap_or_default()
        } else {
            false
        }
    }

    pub async fn create_doc(&self) -> Result<String> {
        let doc_id = nanoid::nanoid!();
        self.load_doc(&doc_id).await?;
        tracing::info!(doc_id=?doc_id, "Created doc");
        Ok(doc_id)
    }

    pub async fn load_doc(&self, doc_id: &str) -> Result<()> {
        let (send, recv) = channel(1024);

        let dwskv = DocWithSyncKv::new(
            doc_id,
            self.store.clone(),
            move || {
                send.try_send(()).unwrap();
            },
            self.skip_gc,
        )
        .await?;

        dwskv
            .sync_kv()
            .persist()
            .await
            .map_err(|e| anyhow!("Error persisting: {:?}", e))?;

        {
            let sync_kv = dwskv.sync_kv();
            let checkpoint_freq = self.checkpoint_freq;
            let doc_id = doc_id.to_string();
            let cancellation_token = self.cancellation_token.clone();

            let on_persist_ctx = (self.snapshot_config.enabled && self.snapshot_config.on_persist
                && self.store.is_some())
                .then(|| OnPersistSnapshotCtx {
                    store: self.store.as_ref().unwrap().clone(),
                    doc_id: doc_id.clone(),
                    interval_secs: self.snapshot_config.on_persist_interval_secs,
                    retention_max: self.snapshot_config.retention_max,
                    last_snapshot: self.last_snapshot_per_doc.clone(),
                });

            // Spawn a task to save the document to the store when it changes.
            self.doc_worker_tracker.spawn(
                Self::doc_persistence_worker(
                    recv,
                    sync_kv,
                    checkpoint_freq,
                    doc_id.clone(),
                    cancellation_token.clone(),
                    on_persist_ctx,
                )
                .instrument(span!(Level::INFO, "save_loop", doc_id=?doc_id)),
            );

            if self.doc_gc {
                let on_gc_ctx = (self.snapshot_config.enabled && self.snapshot_config.on_gc
                    && self.store.is_some())
                    .then(|| OnGcSnapshotCtx {
                        store: self.store.as_ref().unwrap().clone(),
                        doc_id: doc_id.clone(),
                        retention_max: self.snapshot_config.retention_max,
                    });
                self.doc_worker_tracker.spawn(
                    Self::doc_gc_worker(
                        self.docs.clone(),
                        doc_id.clone(),
                        checkpoint_freq,
                        cancellation_token,
                        on_gc_ctx,
                    )
                    .instrument(span!(Level::INFO, "gc_loop", doc_id=?doc_id)),
                );
            }
        }

        self.docs.insert(doc_id.to_string(), dwskv);
        Ok(())
    }

    async fn doc_gc_worker(
        docs: Arc<DashMap<String, DocWithSyncKv>>,
        doc_id: String,
        checkpoint_freq: Duration,
        cancellation_token: CancellationToken,
        on_gc_ctx: Option<OnGcSnapshotCtx>,
    ) {
        let mut checkpoints_without_refs = 0;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(checkpoint_freq) => {
                    if let Some(doc) = docs.get(&doc_id) {
                        let awareness = Arc::downgrade(&doc.awareness());
                        if awareness.strong_count() > 1 {
                            checkpoints_without_refs = 0;
                            tracing::debug!("doc is still alive - it has {} references", awareness.strong_count());
                        } else {
                            checkpoints_without_refs += 1;
                            tracing::info!("doc has only one reference, candidate for GC. checkpoints_without_refs: {}", checkpoints_without_refs);
                        }
                    } else {
                        break;
                    }

                    if checkpoints_without_refs >= 2 {
                        tracing::info!("GCing doc");
                        if let Some(doc) = docs.get(&doc_id) {
                            let _ = doc.sync_kv().persist().await;
                            if let Some(ctx) = &on_gc_ctx {
                                let data_key = store::doc_data_key(&ctx.doc_id);
                                if let Ok(Some(data)) = ctx.store.get(&data_key).await {
                                    let ts = current_time_epoch_millis().to_string();
                                    let snap_key = store::snapshot_key(&ctx.doc_id, &ts);
                                    let _ = ctx.store.set(&snap_key, data).await;
                                    apply_snapshot_retention_impl(&ctx.store, &ctx.doc_id, ctx.retention_max).await;
                                }
                            }
                            doc.sync_kv().shutdown();
                        }

                        docs.remove(&doc_id);
                        break;
                    }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            };
        }
        tracing::info!("Exiting gc_loop");
    }

    async fn doc_persistence_worker(
        mut recv: Receiver<()>,
        sync_kv: Arc<SyncKv>,
        checkpoint_freq: Duration,
        doc_id: String,
        cancellation_token: CancellationToken,
        on_persist_ctx: Option<OnPersistSnapshotCtx>,
    ) {
        let mut last_save = Instant::now();

        loop {
            let is_done = tokio::select! {
                v = recv.recv() => v.is_none(),
                _ = cancellation_token.cancelled() => true,
                _ = tokio::time::sleep(checkpoint_freq) => {
                    sync_kv.is_shutdown()
                }
            };

            tracing::info!("Received signal. done: {}", is_done);
            let now = Instant::now();
            if !is_done && now - last_save < checkpoint_freq {
                let sleep = tokio::time::sleep(checkpoint_freq - (now - last_save));
                tokio::pin!(sleep);
                tracing::info!("Throttling.");

                loop {
                    tokio::select! {
                        _ = &mut sleep => {
                            break;
                        }
                        v = recv.recv() => {
                            tracing::info!("Received dirty while throttling.");
                            if v.is_none() {
                                break;
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            tracing::info!("Received cancellation while throttling.");
                            break;
                        }

                    }
                    tracing::info!("Done throttling.");
                }
            }
            tracing::info!("Persisting.");
            let persist_ok = sync_kv.persist().await.is_ok();
            if !persist_ok {
                tracing::error!("Error persisting.");
            } else {
                tracing::info!("Done persisting.");
            }
            if persist_ok {
                if let Some(ctx) = &on_persist_ctx {
                    let should_snapshot = ctx
                        .last_snapshot
                        .get(&ctx.doc_id)
                        .map(|t| now.duration_since(*t).as_secs() >= ctx.interval_secs)
                        .unwrap_or(true);
                    if should_snapshot {
                        let data_key = store::doc_data_key(&ctx.doc_id);
                        if let Ok(Some(data)) = ctx.store.get(&data_key).await {
                            let ts = current_time_epoch_millis().to_string();
                            let snap_key = store::snapshot_key(&ctx.doc_id, &ts);
                            if ctx.store.set(&snap_key, data).await.is_ok() {
                                ctx.last_snapshot.insert(ctx.doc_id.clone(), now);
                                apply_snapshot_retention_impl(&ctx.store, &ctx.doc_id, ctx.retention_max).await;
                            }
                        }
                    }
                }
            }
            last_save = Instant::now();

            if is_done {
                break;
            }
        }
        tracing::info!("Terminating loop for {}", doc_id);
    }

    pub async fn get_or_create_doc(
        &self,
        doc_id: &str,
    ) -> Result<MappedRef<'_, String, DocWithSyncKv, DocWithSyncKv>> {
        if !self.docs.contains_key(doc_id) {
            tracing::info!(doc_id=?doc_id, "Loading doc");
            self.load_doc(doc_id).await?;
        }

        Ok(self
            .docs
            .get(doc_id)
            .ok_or_else(|| anyhow!("Failed to get-or-create doc"))?
            .map(|d| d))
    }

    /// List snapshot timestamps for a document (newest first). Returns empty if snapshots disabled or store does not support list.
    pub async fn list_snapshots(&self, doc_id: &str) -> Result<Vec<String>, AppError> {
        if !self.snapshot_config.enabled {
            return Err(AppError(
                StatusCode::NOT_FOUND,
                anyhow!("Snapshots are disabled"),
            ));
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow!("No store configured")))?;
        let prefix = store::snapshot_prefix(doc_id);
        let keys = match store.list_prefix(&prefix).await {
            Ok(k) => k,
            Err(StoreError::Unsupported(_)) => return Ok(Vec::new()),
            Err(e) => return Err(AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into())),
        };
        let mut ids: Vec<String> = keys
            .into_iter()
            .filter_map(|k| {
                let rest = k.strip_prefix(&prefix)?;
                rest.strip_suffix("/data.ysweet").map(String::from)
            })
            .collect();
        ids.reverse();
        Ok(ids)
    }

    /// Create a snapshot of the current document state.
    pub async fn create_snapshot(&self, doc_id: &str) -> Result<String, AppError> {
        if !self.snapshot_config.enabled || !self.snapshot_config.manual {
            return Err(AppError(
                StatusCode::FORBIDDEN,
                anyhow!("Snapshot creation is disabled"),
            ));
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow!("No store configured")))?;
        let data_key = store::doc_data_key(doc_id);
        let data = store
            .get(&data_key)
            .await
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?
            .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("Document has no data yet")))?;
        let ts = current_time_epoch_millis().to_string();
        let snap_key = store::snapshot_key(doc_id, &ts);
        store
            .set(&snap_key, data)
            .await
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;
        self.apply_snapshot_retention(doc_id).await?;
        Ok(ts)
    }

    /// Delete excess snapshots per retention_max (numbers-only). Ignores errors (e.g. store doesn't support list/remove).
    async fn apply_snapshot_retention(&self, doc_id: &str) -> Result<(), AppError> {
        if let Some(store) = &self.store {
            apply_snapshot_retention_impl(store, doc_id, self.snapshot_config.retention_max).await;
        }
        Ok(())
    }

    /// Return a snapshot as Yjs update bytes (read-only).
    pub async fn get_snapshot_as_update(
        &self,
        doc_id: &str,
        timestamp: &str,
    ) -> Result<Vec<u8>, AppError> {
        if !self.snapshot_config.enabled {
            return Err(AppError(
                StatusCode::NOT_FOUND,
                anyhow!("Snapshots are disabled"),
            ));
        }
        if !store::is_valid_snapshot_timestamp(timestamp) {
            return Err(AppError(
                StatusCode::BAD_REQUEST,
                anyhow!("Invalid snapshot timestamp"),
            ));
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow!("No store configured")))?;
        let key = store::snapshot_key(doc_id, timestamp);
        let bytes = store
            .get(&key)
            .await
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?
            .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("Snapshot not found")))?;
        doc_sync::snapshot_bytes_to_update(&bytes)
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e))
    }

    /// Rollback document to a snapshot. If before_rollback is true, current state is saved as a new snapshot first.
    pub async fn rollback_to_snapshot(
        &self,
        doc_id: &str,
        timestamp: &str,
    ) -> Result<(), AppError> {
        if !self.snapshot_config.enabled {
            return Err(AppError(
                StatusCode::FORBIDDEN,
                anyhow!("Snapshots are disabled"),
            ));
        }
        if !store::is_valid_snapshot_timestamp(timestamp) {
            return Err(AppError(
                StatusCode::BAD_REQUEST,
                anyhow!("Invalid snapshot timestamp"),
            ));
        }
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| AppError(StatusCode::BAD_REQUEST, anyhow!("No store configured")))?;
        let data_key = store::doc_data_key(doc_id);
        let snap_key = store::snapshot_key(doc_id, timestamp);
        let snapshot_data = store
            .get(&snap_key)
            .await
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?
            .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("Snapshot not found")))?;
        if self.snapshot_config.before_rollback {
            if let Some(current) = store.get(&data_key).await.map_err(|e| {
                AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into())
            })? {
                let backup_ts = current_time_epoch_millis().to_string();
                let backup_key = store::snapshot_key(doc_id, &backup_ts);
                store
                    .set(&backup_key, current)
                    .await
                    .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;
                self.apply_snapshot_retention(doc_id).await?;
            }
        }
        store
            .set(&data_key, snapshot_data)
            .await
            .map_err(|e| AppError(StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;
        if self.docs.contains_key(doc_id) {
            if let Some((_, doc)) = self.docs.remove(doc_id) {
                doc.sync_kv().shutdown();
            }
            self.load_doc(doc_id).await.map_err(|e| {
                AppError(StatusCode::INTERNAL_SERVER_ERROR, anyhow!("Reload after rollback: {}", e))
            })?;
        }
        Ok(())
    }

    pub fn check_auth(
        &self,
        auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    ) -> Result<(), AppError> {
        if let Some(auth) = &self.authenticator {
            if let Some(TypedHeader(headers::Authorization(bearer))) = auth_header {
                if let Ok(()) =
                    auth.verify_server_token(bearer.token(), current_time_epoch_millis())
                {
                    return Ok(());
                }
            }
            Err((StatusCode::UNAUTHORIZED, anyhow!("Unauthorized.")))?
        } else {
            Ok(())
        }
    }

    pub async fn redact_error_middleware(req: Request, next: Next) -> impl IntoResponse {
        let resp = next.run(req).await;
        if resp.status().is_server_error() || resp.status().is_client_error() {
            // If we should redact errors, copy over only the status code and
            // not the response body.
            return resp.status().into_response();
        }
        resp
    }

    pub fn routes(self: &Arc<Self>) -> Router {
        let mut router = Router::new()
            .route("/ready", get(ready))
            .route("/check_store", post(check_store))
            .route("/check_store", get(check_store_deprecated))
            .route("/doc/ws/:doc_id", get(handle_socket_upgrade_deprecated))
            .route("/doc/new", post(new_doc))
            .route("/doc/:doc_id/auth", post(auth_doc))
            .route("/doc/:doc_id/as-update", get(get_doc_as_update_deprecated))
            .route("/doc/:doc_id/update", post(update_doc_deprecated))
            .route("/d/:doc_id/as-update", get(get_doc_as_update))
            .route("/d/:doc_id/update", post(update_doc))
            .route(
                "/d/:doc_id/ws/:doc_id2",
                get(handle_socket_upgrade_full_path),
            );
        if self.snapshot_config.enabled {
            router = router
                .route("/d/:doc_id/snapshots", get(list_snapshots))
                .route("/d/:doc_id/snapshots", post(create_snapshot))
                .route(
                    "/d/:doc_id/snapshots/:timestamp/as-update",
                    get(get_snapshot_as_update),
                )
                .route(
                    "/d/:doc_id/snapshots/:timestamp/rollback",
                    post(rollback_snapshot),
                );
        }
        router.with_state(self.clone())
    }

    pub fn single_doc_routes(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/ws/:doc_id", get(handle_socket_upgrade_single))
            .route("/as-update", get(get_doc_as_update_single))
            .route("/update", post(update_doc_single))
            .with_state(self.clone())
    }

    async fn serve_internal(
        self: Arc<Self>,
        listener: TcpListener,
        redact_errors: bool,
        routes: Router,
    ) -> Result<()> {
        let token = self.cancellation_token.clone();

        let mut app = if let Some(max_body_size) = self.max_body_size {
            routes.layer(DefaultBodyLimit::max(max_body_size))
        } else {
            routes
        };

        app = if redact_errors {
            app
        } else {
            app.layer(middleware::from_fn(Self::redact_error_middleware))
        };

        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;

        self.doc_worker_tracker.close();
        self.doc_worker_tracker.wait().await;

        Ok(())
    }

    pub async fn serve(self, listener: TcpListener, redact_errors: bool) -> Result<()> {
        let s = Arc::new(self);
        let routes = s.routes();
        s.serve_internal(listener, redact_errors, routes).await
    }

    pub async fn serve_doc(self, listener: TcpListener, redact_errors: bool) -> Result<()> {
        let s = Arc::new(self);
        let routes = s.single_doc_routes();
        s.serve_internal(listener, redact_errors, routes).await
    }

    fn verify_doc_token(&self, token: Option<&str>, doc: &str) -> Result<Authorization, AppError> {
        if let Some(authenticator) = &self.authenticator {
            if let Some(token) = token {
                let authorization = authenticator
                    .verify_doc_token(token, doc, current_time_epoch_millis())
                    .map_err(|e| (StatusCode::UNAUTHORIZED, e))?;
                Ok(authorization)
            } else {
                Err((StatusCode::UNAUTHORIZED, anyhow!("No token provided.")))?
            }
        } else {
            Ok(Authorization::Full)
        }
    }

    fn get_single_doc_id(&self) -> Result<String, AppError> {
        self.docs
            .iter()
            .next()
            .map(|entry| entry.key().clone())
            .ok_or_else(|| AppError(StatusCode::NOT_FOUND, anyhow!("No document found")))
    }
}

#[derive(Deserialize)]
struct HandlerParams {
    token: Option<String>,
}

async fn get_doc_as_update(
    State(server_state): State<Arc<Server>>,
    Path(doc_id): Path<String>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Response, AppError> {
    // All authorization types allow reading the document.
    let token = get_token_from_header(auth_header);
    let _ = server_state.verify_doc_token(token.as_deref(), &doc_id)?;

    let dwskv = server_state
        .get_or_create_doc(&doc_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let update = dwskv.as_update();
    tracing::debug!("update: {:?}", update);
    Ok(update.into_response())
}

async fn get_doc_as_update_deprecated(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Response, AppError> {
    tracing::warn!("/doc/:doc_id/as-update is deprecated; call /doc/:doc_id/auth instead and then call as-update on the returned base URL.");
    get_doc_as_update(State(server_state), Path(doc_id), auth_header).await
}

async fn list_snapshots(
    State(server_state): State<Arc<Server>>,
    Path(doc_id): Path<String>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Json<serde_json::Value>, AppError> {
    if !validate_doc_name(doc_id.as_str()) {
        return Err(AppError(StatusCode::BAD_REQUEST, anyhow!("Invalid document name")));
    }
    let token = get_token_from_header(auth_header);
    let _ = server_state.verify_doc_token(token.as_deref(), &doc_id)?;
    let timestamps = server_state.list_snapshots(&doc_id).await?;
    Ok(Json(serde_json::json!({ "timestamps": timestamps })))
}

async fn create_snapshot(
    State(server_state): State<Arc<Server>>,
    Path(doc_id): Path<String>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Json<serde_json::Value>, AppError> {
    if !validate_doc_name(doc_id.as_str()) {
        return Err(AppError(StatusCode::BAD_REQUEST, anyhow!("Invalid document name")));
    }
    let token = get_token_from_header(auth_header);
    let auth = server_state.verify_doc_token(token.as_deref(), &doc_id)?;
    if !matches!(auth, Authorization::Full) {
        return Err(AppError(StatusCode::FORBIDDEN, anyhow!("Full auth required to create snapshot")));
    }
    let timestamp = server_state.create_snapshot(&doc_id).await?;
    Ok(Json(serde_json::json!({ "timestamp": timestamp })))
}

async fn get_snapshot_as_update(
    State(server_state): State<Arc<Server>>,
    Path((doc_id, timestamp)): Path<(String, String)>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Response, AppError> {
    if !validate_doc_name(doc_id.as_str()) {
        return Err(AppError(StatusCode::BAD_REQUEST, anyhow!("Invalid document name")));
    }
    let token = get_token_from_header(auth_header);
    let _ = server_state.verify_doc_token(token.as_deref(), &doc_id)?;
    let update = server_state.get_snapshot_as_update(&doc_id, &timestamp).await?;
    Ok(update.into_response())
}

async fn rollback_snapshot(
    State(server_state): State<Arc<Server>>,
    Path((doc_id, timestamp)): Path<(String, String)>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<StatusCode, AppError> {
    if !validate_doc_name(doc_id.as_str()) {
        return Err(AppError(StatusCode::BAD_REQUEST, anyhow!("Invalid document name")));
    }
    let token = get_token_from_header(auth_header);
    let auth = server_state.verify_doc_token(token.as_deref(), &doc_id)?;
    if !matches!(auth, Authorization::Full) {
        return Err(AppError(StatusCode::FORBIDDEN, anyhow!("Full auth required to rollback")));
    }
    server_state.rollback_to_snapshot(&doc_id, &timestamp).await?;
    Ok(StatusCode::OK)
}

async fn update_doc_deprecated(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    body: Bytes,
) -> Result<Response, AppError> {
    tracing::warn!("/doc/:doc_id/update is deprecated; call /doc/:doc_id/auth instead and then call update on the returned base URL.");
    update_doc(Path(doc_id), State(server_state), auth_header, body).await
}

async fn get_doc_as_update_single(
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Result<Response, AppError> {
    let doc_id = server_state.get_single_doc_id()?;
    get_doc_as_update(State(server_state), Path(doc_id), auth_header).await
}

async fn update_doc(
    Path(doc_id): Path<String>,
    State(server_state): State<Arc<Server>>,
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    body: Bytes,
) -> Result<Response, AppError> {
    let token = get_token_from_header(auth_header);
    let authorization = server_state.verify_doc_token(token.as_deref(), &doc_id)?;
    update_doc_inner(doc_id, server_state, authorization, body).await
}

async fn update_doc_inner(
    doc_id: String,
    server_state: Arc<Server>,
    authorization: Authorization,
    body: Bytes,
) -> Result<Response, AppError> {
    if !matches!(authorization, Authorization::Full) {
        return Err(AppError(StatusCode::FORBIDDEN, anyhow!("Unauthorized.")));
    }

    let dwskv = server_state
        .get_or_create_doc(&doc_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    if let Err(err) = dwskv.apply_update(&body) {
        tracing::error!(?err, "Failed to apply update");
        return Err(AppError(StatusCode::INTERNAL_SERVER_ERROR, err));
    }

    Ok(StatusCode::OK.into_response())
}

async fn update_doc_single(
    State(server_state): State<Arc<Server>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let doc_id = server_state.get_single_doc_id()?;
    // the doc server is meant to be run in Plane, so we expect verified plane
    // headers to be used for authorization.
    let authorization = get_authorization_from_plane_header(headers)?;
    update_doc_inner(doc_id, server_state, authorization, body).await
}

async fn handle_socket_upgrade(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<String>,
    authorization: Authorization,
    State(server_state): State<Arc<Server>>,
) -> Result<Response, AppError> {
    if !matches!(authorization, Authorization::Full) && !server_state.docs.contains_key(&doc_id) {
        return Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Doc {} not found", doc_id),
        ));
    }

    let dwskv = server_state
        .get_or_create_doc(&doc_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let awareness = dwskv.awareness();
    let cancellation_token = server_state.cancellation_token.clone();

    Ok(ws.on_upgrade(move |socket| {
        handle_socket(socket, awareness, authorization, cancellation_token)
    }))
}

async fn handle_socket_upgrade_deprecated(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<String>,
    Query(params): Query<HandlerParams>,
    State(server_state): State<Arc<Server>>,
) -> Result<Response, AppError> {
    tracing::warn!(
        "/doc/ws/:doc_id is deprecated; call /doc/:doc_id/auth instead and use the returned URL."
    );
    let authorization = server_state.verify_doc_token(params.token.as_deref(), &doc_id)?;
    handle_socket_upgrade(ws, Path(doc_id), authorization, State(server_state)).await
}

async fn handle_socket_upgrade_full_path(
    ws: WebSocketUpgrade,
    Path((doc_id, doc_id2)): Path<(String, String)>,
    Query(params): Query<HandlerParams>,
    State(server_state): State<Arc<Server>>,
) -> Result<Response, AppError> {
    if doc_id != doc_id2 {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("For Yjs compatibility, the doc_id appears twice in the URL. It must be the same in both places, but we got {} and {}.", doc_id, doc_id2),
        ));
    }
    let authorization = server_state.verify_doc_token(params.token.as_deref(), &doc_id)?;
    handle_socket_upgrade(ws, Path(doc_id), authorization, State(server_state)).await
}

async fn handle_socket_upgrade_single(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<String>,
    headers: HeaderMap,
    State(server_state): State<Arc<Server>>,
) -> Result<Response, AppError> {
    let single_doc_id = server_state.get_single_doc_id()?;
    if doc_id != single_doc_id {
        return Err(AppError(
            StatusCode::NOT_FOUND,
            anyhow!("Document not found"),
        ));
    }

    // the doc server is meant to be run in Plane, so we expect verified plane
    // headers to be used for authorization.
    let authorization = get_authorization_from_plane_header(headers)?;
    handle_socket_upgrade(ws, Path(single_doc_id), authorization, State(server_state)).await
}

async fn handle_socket(
    socket: WebSocket,
    awareness: Arc<RwLock<Awareness>>,
    authorization: Authorization,
    cancellation_token: CancellationToken,
) {
    let (mut sink, mut stream) = socket.split();
    let (send, mut recv) = channel(1024);

    let last_pong = Arc::new(RwLock::new(tokio::time::Instant::now()));
    let last_pong_clone = last_pong.clone();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(PING_EVERY);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                msg = recv.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    let _ = sink.send(Message::Binary(msg)).await;
                }
                _ = ticker.tick() => {
                    if last_pong_clone.read().expect("Failed to get read lock on last_pong").elapsed() > PONG_TIMEOUT {
                        tracing::info!("Pong timeout, closing connection");
                        break;
                    }
                    let _ = sink.send(Message::Ping(vec![])).await;
                }
            }
        }
    });

    let connection = DocConnection::new(awareness, authorization, move |bytes| {
        if let Err(e) = send.try_send(bytes.to_vec()) {
            tracing::warn!(?e, "Error sending message");
        }
    });

    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(msg) = msg else {
                    break;
                };
                let msg = match msg {
                    Ok(Message::Binary(bytes)) => bytes,
                    Ok(Message::Close(_)) => break,
                    Ok(Message::Pong(_)) => {
                        *last_pong.write().expect("Failed to get write lock on last_pong") = tokio::time::Instant::now();
                        continue;
                    }
                    Err(_e) => {
                        // The stream will complain about things like
                        // connections being lost without handshake.
                        continue;
                    }
                    msg => {
                        tracing::warn!(?msg, "Received non-binary message");
                        continue;
                    }
                };

                if let Err(e) = connection.send(&msg).await {
                    tracing::warn!(?e, "Error handling message");
                }
            }
            _ = cancellation_token.cancelled() => {
                tracing::debug!("Closing doc connection due to server cancel...");
                break;
            }
        }
    }
}

async fn check_store(
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    State(server_state): State<Arc<Server>>,
) -> Result<Json<Value>, AppError> {
    server_state.check_auth(auth_header)?;

    if server_state.store.is_none() {
        return Ok(Json(json!({"ok": false, "error": "No store set."})));
    };

    // The check_store endpoint for the native server is kind of moot, since
    // the server will not start if store is not ok.
    Ok(Json(json!({"ok": true})))
}

async fn check_store_deprecated(
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    State(server_state): State<Arc<Server>>,
) -> Result<Json<Value>, AppError> {
    tracing::warn!(
        "GET check_store is deprecated, use POST check_store with an empty body instead."
    );
    check_store(auth_header, State(server_state)).await
}

/// Always returns a 200 OK response, as long as we are listening.
async fn ready() -> Result<Json<Value>, AppError> {
    Ok(Json(json!({"ok": true})))
}

async fn new_doc(
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    State(server_state): State<Arc<Server>>,
    Json(body): Json<DocCreationRequest>,
) -> Result<Json<NewDocResponse>, AppError> {
    server_state.check_auth(auth_header)?;

    let doc_id = if let Some(doc_id) = body.doc_id {
        if !validate_doc_name(doc_id.as_str()) {
            Err((StatusCode::BAD_REQUEST, anyhow!("Invalid document name")))?
        }

        server_state
            .get_or_create_doc(doc_id.as_str())
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to create doc");
                (StatusCode::INTERNAL_SERVER_ERROR, e)
            })?;

        doc_id
    } else {
        server_state.create_doc().await.map_err(|d| {
            tracing::error!(?d, "Failed to create doc");
            (StatusCode::INTERNAL_SERVER_ERROR, d)
        })?
    };

    Ok(Json(NewDocResponse { doc_id }))
}

async fn auth_doc(
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
    TypedHeader(host): TypedHeader<headers::Host>,
    State(server_state): State<Arc<Server>>,
    Path(doc_id): Path<String>,
    body: Option<Json<AuthDocRequest>>,
) -> Result<Json<ClientToken>, AppError> {
    server_state.check_auth(auth_header)?;

    let Json(AuthDocRequest {
        authorization,
        valid_for_seconds,
        ..
    }) = body.unwrap_or_default();

    if !server_state.doc_exists(&doc_id).await {
        Err((StatusCode::NOT_FOUND, anyhow!("Doc {} not found", doc_id)))?;
    }

    let valid_for_seconds = valid_for_seconds.unwrap_or(DEFAULT_EXPIRATION_SECONDS);
    let expiration_time =
        ExpirationTimeEpochMillis(current_time_epoch_millis() + valid_for_seconds * 1000);

    let token = if let Some(auth) = &server_state.authenticator {
        let token = auth.gen_doc_token(&doc_id, authorization, expiration_time);
        Some(token)
    } else {
        None
    };

    let url = if let Some(url_prefix) = &server_state.url_prefix {
        let mut url = url_prefix.clone();
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme).unwrap();
        url = url.join(&format!("/d/{doc_id}/ws")).unwrap();
        url.to_string()
    } else {
        format!("ws://{host}/d/{doc_id}/ws")
    };

    let base_url = if let Some(url_prefix) = &server_state.url_prefix {
        let mut url_prefix = url_prefix.to_string();
        if !url_prefix.ends_with('/') {
            url_prefix = format!("{url_prefix}/");
        }

        format!("{url_prefix}d/{doc_id}")
    } else {
        format!("http://{host}/d/{doc_id}")
    };

    Ok(Json(ClientToken {
        url,
        base_url: Some(base_url),
        doc_id,
        token,
        authorization,
    }))
}

fn get_token_from_header(
    auth_header: Option<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>,
) -> Option<String> {
    if let Some(TypedHeader(headers::Authorization(bearer))) = auth_header {
        Some(bearer.token().to_string())
    } else {
        None
    }
}

#[derive(Deserialize)]
struct PlaneVerifiedUserData {
    authorization: Authorization,
}

fn get_authorization_from_plane_header(headers: HeaderMap) -> Result<Authorization, AppError> {
    if let Some(token) = headers.get(HeaderName::from_static(PLANE_VERIFIED_USER_DATA_HEADER)) {
        let token_str = token.to_str().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        let user_data: PlaneVerifiedUserData =
            serde_json::from_str(token_str).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        Ok(user_data.authorization)
    } else {
        Err((StatusCode::UNAUTHORIZED, anyhow!("No token provided.")))?
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use y_sweet_core::api_types::Authorization;

    #[tokio::test]
    async fn test_auth_doc() {
        let server_state = Server::new(
            None,
            Duration::from_secs(60),
            None,
            None,
            CancellationToken::new(),
            true,
            None,
            false,
            SnapshotConfig::default(),
        )
        .await
        .unwrap();

        let doc_id = server_state.create_doc().await.unwrap();

        let token = auth_doc(
            None,
            TypedHeader(headers::Host::from(http::uri::Authority::from_static(
                "localhost",
            ))),
            State(Arc::new(server_state)),
            Path(doc_id.clone()),
            Some(Json(AuthDocRequest {
                authorization: Authorization::Full,
                user_id: None,
                valid_for_seconds: None,
            })),
        )
        .await
        .unwrap();

        let expected_url = format!("ws://localhost/d/{doc_id}/ws");
        assert_eq!(token.url, expected_url);
        assert_eq!(token.doc_id, doc_id);
        assert!(token.token.is_none());
    }

    #[tokio::test]
    async fn test_auth_doc_with_prefix() {
        let prefix: Url = "https://foo.bar".parse().unwrap();
        let server_state = Server::new(
            None,
            Duration::from_secs(60),
            None,
            Some(prefix),
            CancellationToken::new(),
            true,
            None,
            false,
            SnapshotConfig::default(),
        )
        .await
        .unwrap();

        let doc_id = server_state.create_doc().await.unwrap();

        let token = auth_doc(
            None,
            TypedHeader(headers::Host::from(http::uri::Authority::from_static(
                "localhost",
            ))),
            State(Arc::new(server_state)),
            Path(doc_id.clone()),
            None,
        )
        .await
        .unwrap();

        let expected_url = format!("wss://foo.bar/d/{doc_id}/ws");
        assert_eq!(token.url, expected_url);
        assert_eq!(token.doc_id, doc_id);
        assert!(token.token.is_none());
    }
}
