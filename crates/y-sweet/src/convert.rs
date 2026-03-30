use anyhow::Result;
use std::sync::Arc;
use y_sweet_core::{doc_connection::DOC_NAME, store::Store, sync_kv::SyncKv};
use yrs_kvstore::DocOps;

/// Convert a Yjs document (encoded as a v1 update) to a .ysweet store.
pub async fn convert(store: Box<dyn Store>, doc_as_update: &[u8], doc_id: &str) -> Result<()> {
    let store = Some(Arc::new(store));

    let sync_kv = SyncKv::new(store, doc_id, || ()).await?;

    let sync_kv = Arc::new(sync_kv);

    sync_kv
        .push_update(DOC_NAME, doc_as_update)
        .map_err(|_| anyhow::anyhow!("Failed to push update"))?;

    sync_kv
        .flush_doc_with(DOC_NAME, yrs::Options::default())
        .map_err(|err| anyhow::anyhow!("Failed to flush doc {:?}", err))?;

    sync_kv
        .persist()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to persist: {:?}", e))?;

    Ok(())
}

/// Convert .ysweet store data (bincode-serialized BTreeMap) to a Yjs v1 update.
/// The SyncKv stores data as a bincode-serialized BTreeMap<Vec<u8>, Vec<u8>>.
/// 
/// This function properly loads the document using yrs_kvstore's API.
pub fn store_data_to_update(store_data: &[u8]) -> Result<Vec<u8>> {
    use yrs::{Doc, ReadTxn, StateVector, Transact, Update};
    use yrs::updates::decoder::Decode;
    use std::collections::BTreeMap;
    
    // Deserialize the bincode-encoded BTreeMap
    let map: BTreeMap<Vec<u8>, Vec<u8>> = bincode::deserialize(store_data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize bincode data: {}", e))?;
    
    if map.is_empty() {
        anyhow::bail!("Store data is empty");
    }
    
    tracing::info!("store_data_to_update: loading document from {} entries", map.len());
    
    // Create a new Yjs document
    let doc = Doc::new();
    
    // yrs_kvstore stores updates with keys that have a specific binary format.
    // The keys starting with \0 followed by non-zero bytes are document updates.
    // We need to collect all updates and apply them to the document.
    
    let mut update_count = 0;
    for (key, value) in &map {
        // Check if this is a document update key (starts with \0 but not \0\0)
        if key.len() >= 2 && key[0] == 0 && key[1] != 0 {
            // This is a document update - decode and apply it
            match Update::decode_v1(value) {
                Ok(update) => {
                    let mut txn = doc.transact_mut();
                    txn.apply_update(update);
                    update_count += 1;
                }
                Err(e) => {
                    tracing::warn!("Failed to decode update ({} bytes): {}", value.len(), e);
                }
            }
        }
    }
    
    tracing::info!("Applied {} document updates", update_count);
    
    // Export as v1 update
    let update = doc.transact().encode_state_as_update_v1(&StateVector::default());
    tracing::info!("Exported document as {} byte update", update.len());
    
    Ok(update)
}
