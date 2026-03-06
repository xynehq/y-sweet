use crate::{
    config::Configuration, server_context::ServerContext, threadless::Threadless, DocIdPair,
};
use futures::StreamExt;
use js_sys::Uint8Array;
use std::sync::Arc;
use worker::{
    durable_object, Env, Request, Response, Result, RouteContext, Router, State, WebSocketPair,
};
#[allow(unused)]
use worker_sys::console_log;
use y_sweet_core::{
    api_types::Authorization,
    doc_connection::DocConnection,
    doc_sync::{self, DocWithSyncKv},
    store::{self, Store, StoreError},
};

#[durable_object]
pub struct YServe {
    env: Env,
    lazy_doc: Option<DocIdPair>,
    state: State,
}

impl YServe {
    /// We need to lazily create the doc because the constructor is non-async.
    pub async fn get_doc(&mut self, req: &Request, doc_id: &str) -> Result<&mut DocWithSyncKv> {
        if self.lazy_doc.is_none() {
            console_log!("Initializing lazy_doc.");

            let mut context = ServerContext::from_request(req, &self.env)
                .map_err(|_| "Couldn't get server context from request.")?;
            #[allow(clippy::arc_with_non_send_sync)] // Arc required for compatibility with core.
            let storage = Arc::new(self.state.storage());

            let store = Some(context.store());
            let storage = Threadless(storage);
            let config = Configuration::try_from(&self.env).map_err(|e| e.to_string())?;
            let timeout_interval_ms: i64 = config
                .timeout_interval
                .as_millis()
                .try_into()
                .expect("Should be able to convert timeout interval to i64");

            let doc = DocWithSyncKv::new(
                doc_id,
                store,
                move || {
                    let storage = storage.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        console_log!("Setting alarm.");
                        if let Err(e) = storage.0.set_alarm(timeout_interval_ms).await {
                            console_log!("Error setting alarm: {:?}", e);
                        }
                    });
                },
                false,
            )
            .await
            .map_err(|e| format!("Error creating doc: {:?}", e))?;

            let len = doc.sync_kv().len();
            console_log!("Persisting doc. Len = {}.", len);
            self.lazy_doc = Some(DocIdPair {
                doc,
                id: doc_id.to_owned(),
            });
            self.lazy_doc
                .as_mut()
                .ok_or("Couldn't get mutable reference to lazy_doc.")?
                .doc
                .sync_kv()
                .persist()
                .await
                .map_err(|_| "Couldn't persist doc.")?;
        }

        Ok(&mut self
            .lazy_doc
            .as_mut()
            .ok_or("Couldn't get doc as mutable.")?
            .doc)
    }
}

#[durable_object]
impl DurableObject for YServe {
    fn new(state: State, env: Env) -> Self {
        Self {
            env,
            state,
            lazy_doc: None,
        }
    }

    async fn fetch(&mut self, req: Request) -> Result<Response> {
        let env: Env = self.env.clone();
        let req = ServerContext::reconstruct_request(&req)?;

        Router::with_data(self)
            .post_async("/doc/:doc_id", handle_doc_create)
            .get_async("/doc/ws/:doc_id", websocket_connect)
            .get_async("/doc/:doc_id/as-update", as_update)
            .post_async("/doc/:doc_id/update", update_doc)
            .get_async("/doc/:doc_id/snapshots", do_list_snapshots)
            .post_async("/doc/:doc_id/snapshots", do_create_snapshot)
            .get_async("/doc/:doc_id/snapshots/:timestamp/as-update", do_get_snapshot_as_update)
            .post_async("/doc/:doc_id/snapshots/:timestamp/rollback", do_rollback_snapshot)
            .run(req, env)
            .await
    }

    async fn alarm(&mut self) -> Result<Response> {
        console_log!("Alarm!");
        let DocIdPair { id, doc } = self.lazy_doc.as_ref().ok_or("Couldn't get lazy doc.")?;
        doc.sync_kv()
            .persist()
            .await
            .map_err(|_| "Couldn't persist doc.")?;
        let len = doc.sync_kv().len();
        console_log!("Persisted. {} (len: {})", id, len);
        Response::ok("ok")
    }
}

async fn as_update(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let doc_id = ctx
        .param("doc_id")
        .ok_or("Couldn't parse doc_id")?
        .to_owned();
    let doc = ctx
        .data
        .get_doc(&req, &doc_id)
        .await
        .map_err(|_| "Couldn't get doc.")?;
    let update = doc.as_update();
    Response::from_bytes(update)
}

async fn update_doc(mut req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let doc_id = ctx
        .param("doc_id")
        .ok_or("Couldn't parse doc_id")?
        .to_owned();
    let doc = ctx
        .data
        .get_doc(&req, &doc_id)
        .await
        .map_err(|_| "Couldn't get doc.")?;
    let bytes = req.bytes().await.map_err(|_| "Couldn't get bytes.")?;
    doc.apply_update(&bytes)
        .map_err(|_| "Couldn't apply update.")?;
    Response::ok("ok")
}

async fn handle_doc_create(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let doc_id = ctx
        .param("doc_id")
        .ok_or("Couldn't parse doc_id")?
        .to_owned();
    ctx.data
        .get_doc(&req, &doc_id)
        .await
        .map_err(|_| "Couldn't get doc.")?;

    Response::ok("ok")
}

fn context_from_request(req: &Request, env: &Env) -> std::result::Result<ServerContext, worker::Error> {
    ServerContext::from_request(req, env).map_err(|e| worker::Error::RustError(e.to_string()))
}

async fn do_list_snapshots(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let mut context = context_from_request(&req, &ctx.data.env)?;
    if !context.config.snapshots_enabled {
        return Response::error("Snapshots disabled", 404)?;
    }
    let store = context.store();
    let doc_id = ctx
        .param("doc_id")
        .ok_or_else(|| worker::Error::RustError("doc_id".into()))?
        .to_string();
    let prefix = store::snapshot_prefix(&doc_id);
    let keys = match store.list_prefix(&prefix).await {
        Ok(k) => k,
        Err(StoreError::Unsupported(_)) => vec![],
        Err(_) => return Response::error("List failed", 500)?,
    };
    let mut ids: Vec<String> = keys
        .iter()
        .filter_map(|k| {
            let rest = k.strip_prefix(&prefix)?;
            rest.strip_suffix("/data.ysweet").map(String::from)
        })
        .collect();
    ids.reverse();
    Response::from_json(&serde_json::json!({ "timestamps": ids }))
}

async fn do_create_snapshot(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let mut context = context_from_request(&req, &ctx.data.env)?;
    if !context.config.snapshots_enabled {
        return Response::error("Snapshots disabled", 403)?;
    }
    let store = context.store();
    let doc_id = ctx
        .param("doc_id")
        .ok_or_else(|| worker::Error::RustError("doc_id".into()))?
        .to_string();
    let _doc = ctx
        .data
        .get_doc(&req, &doc_id)
        .await
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    let _ = _doc.sync_kv().persist().await;
    let data_key = store::doc_data_key(&doc_id);
    let data = match store.get(&data_key).await {
        Ok(Some(d)) => d,
        _ => return Response::error("No document data", 404)?,
    };
    let ts = worker::Date::now().as_millis().to_string();
    let snap_key = store::snapshot_key(&doc_id, &ts);
    store
        .set(&snap_key, data)
        .await
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    Response::from_json(&serde_json::json!({ "timestamp": ts }))
}

async fn do_get_snapshot_as_update(
    req: Request,
    ctx: RouteContext<&mut YServe>,
) -> Result<Response> {
    let mut context = context_from_request(&req, &ctx.data.env)?;
    if !context.config.snapshots_enabled {
        return Response::error("Snapshots disabled", 404)?;
    }
    let store = context.store();
    let doc_id = ctx
        .param("doc_id")
        .ok_or_else(|| worker::Error::RustError("doc_id".into()))?
        .to_string();
    let timestamp = ctx
        .param("timestamp")
        .ok_or_else(|| worker::Error::RustError("timestamp".into()))?
        .to_string();
    if !store::is_valid_snapshot_timestamp(&timestamp) {
        return Response::error("Invalid snapshot timestamp", 400)?;
    }
    let key = store::snapshot_key(&doc_id, &timestamp);
    let bytes = match store.get(&key).await {
        Ok(Some(b)) => b,
        _ => return Response::error("Snapshot not found", 404)?,
    };
    let update = doc_sync::snapshot_bytes_to_update(&bytes)
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    Response::from_bytes(update)
}

async fn do_rollback_snapshot(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let mut context = context_from_request(&req, &ctx.data.env)?;
    if !context.config.snapshots_enabled {
        return Response::error("Snapshots disabled", 403)?;
    }
    let store = context.store();
    let doc_id = ctx
        .param("doc_id")
        .ok_or_else(|| worker::Error::RustError("doc_id".into()))?
        .to_string();
    let timestamp = ctx
        .param("timestamp")
        .ok_or_else(|| worker::Error::RustError("timestamp".into()))?
        .to_string();
    if !store::is_valid_snapshot_timestamp(&timestamp) {
        return Response::error("Invalid snapshot timestamp", 400)?;
    }
    let data_key = store::doc_data_key(&doc_id);
    let snap_key = store::snapshot_key(&doc_id, &timestamp);
    let snapshot_data = match store.get(&snap_key).await {
        Ok(Some(d)) => d,
        _ => return Response::error("Snapshot not found", 404)?,
    };
    if let Ok(Some(current)) = store.get(&data_key).await {
        let backup_ts = worker::Date::now().as_millis().to_string();
        let backup_key = store::snapshot_key(&doc_id, &backup_ts);
        let _ = store.set(&backup_key, current).await;
    }
    store
        .set(&data_key, snapshot_data)
        .await
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    ctx.data.lazy_doc = None;
    Response::ok("ok")
}

async fn websocket_connect(req: Request, ctx: RouteContext<&mut YServe>) -> Result<Response> {
    let WebSocketPair { client, server } = WebSocketPair::new()?;
    server.accept()?;

    let doc_id = ctx
        .param("doc_id")
        .ok_or("Couldn't parse doc_id")?
        .to_owned();
    let awareness = ctx
        .data
        .get_doc(&req, &doc_id)
        .await
        .map_err(|_| "Couldn't get doc.")?
        .awareness();

    let connection = {
        let server = server.clone();
        DocConnection::new(awareness, Authorization::Full, move |bytes| {
            let uint8_array = Uint8Array::from(bytes);
            let result = server
                .as_ref()
                .send_with_array_buffer(&uint8_array.buffer());

            if let Err(result) = result {
                console_log!("Error sending bytes: {:?}", result);
            }
        })
    };

    wasm_bindgen_futures::spawn_local(async move {
        let mut events = server.events().unwrap();

        while let Some(event) = events.next().await {
            match event.unwrap() {
                worker::WebsocketEvent::Message(message) => {
                    if let Some(bytes) = message.bytes() {
                        let result = connection.send(&bytes).await;
                        if let Err(result) = result {
                            console_log!("Error sending bytes: {:?}", result);
                        }
                    } else {
                        server
                            .send_with_str("Received unexpected text message.")
                            .unwrap()
                    }
                }
                worker::WebsocketEvent::Close(_) => {
                    let _ = server.close::<&str>(None, None);
                    break;
                }
            }
        }
    });

    let resp = Response::from_websocket(client)?;
    Ok(resp)
}
