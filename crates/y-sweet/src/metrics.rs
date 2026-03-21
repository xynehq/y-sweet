use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Global metrics for the y-sweet server.
///
/// - Gauges: real-time current state (active_connections). active_documents
///   is computed live from the DashMap when a snapshot is taken.
/// - Interval counters: event counts in the last 60s, reset each period.
/// - Latency trackers: processing time for sync updates, HTTP updates,
///   and persistence operations (avg/max over the interval).
#[derive(Debug, Default)]
pub struct Metrics {
    // ── Gauge ──
    pub active_connections: AtomicU64,

    // ── Interval counters (reset every 60s) ──
    pub websocket_failures: AtomicU64,
    pub pong_timeouts: AtomicU64,
    pub documents_loaded: AtomicU64,
    pub documents_gc: AtomicU64,
    pub sync_updates: AtomicU64,
    pub http_updates: AtomicU64,
    pub persistence_ops: AtomicU64,
    pub persistence_errors: AtomicU64,

    // ── Latency: sync update processing (microseconds) ──
    pub sync_latency_total_us: AtomicU64,
    pub sync_latency_count: AtomicU64,
    pub sync_latency_max_us: AtomicU64,

    // ── Latency: HTTP update processing (microseconds) ──
    pub http_latency_total_us: AtomicU64,
    pub http_latency_count: AtomicU64,
    pub http_latency_max_us: AtomicU64,

    // ── Latency: persistence (microseconds) ──
    pub persistence_latency_total_us: AtomicU64,
    pub persistence_latency_count: AtomicU64,
    pub persistence_latency_max_us: AtomicU64,
}

/// A point-in-time snapshot of all metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    // Gauges (live)
    pub active_connections: u64,
    pub active_documents: u64,

    // Interval counters (last 60s)
    pub websocket_failures: u64,
    pub pong_timeouts: u64,
    pub documents_loaded: u64,
    pub documents_gc: u64,
    pub sync_updates: u64,
    pub http_updates: u64,
    pub persistence_ops: u64,
    pub persistence_errors: u64,

    // Latency (last 60s)
    pub sync_latency_avg_ms: f64,
    pub sync_latency_max_ms: f64,
    pub http_latency_avg_ms: f64,
    pub http_latency_max_ms: f64,
    pub persistence_latency_avg_ms: f64,
    pub persistence_latency_max_ms: f64,
}

fn update_max(atomic: &AtomicU64, value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while value > current {
        match atomic.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn avg_us_to_ms(total_us: u64, count: u64) -> f64 {
    if count > 0 {
        (total_us as f64 / count as f64) / 1000.0
    } else {
        0.0
    }
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    // ── Connection gauge ──

    pub fn inc_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    // ── Interval counter helpers ──

    pub fn inc_websocket_failures(&self) {
        self.websocket_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_pong_timeouts(&self) {
        self.pong_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_documents_loaded(&self) {
        self.documents_loaded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_documents_gc(&self) {
        self.documents_gc.fetch_add(1, Ordering::Relaxed);
    }

    // ── Sync update latency ──

    pub fn record_sync_update(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.sync_updates.fetch_add(1, Ordering::Relaxed);
        self.sync_latency_total_us.fetch_add(us, Ordering::Relaxed);
        self.sync_latency_count.fetch_add(1, Ordering::Relaxed);
        update_max(&self.sync_latency_max_us, us);
    }

    // ── HTTP update latency ──

    pub fn record_http_update(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.http_updates.fetch_add(1, Ordering::Relaxed);
        self.http_latency_total_us.fetch_add(us, Ordering::Relaxed);
        self.http_latency_count.fetch_add(1, Ordering::Relaxed);
        update_max(&self.http_latency_max_us, us);
    }

    // ── Persistence latency ──

    pub fn record_persistence_op(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.persistence_ops.fetch_add(1, Ordering::Relaxed);
        self.persistence_latency_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.persistence_latency_count
            .fetch_add(1, Ordering::Relaxed);
        update_max(&self.persistence_latency_max_us, us);
    }

    pub fn inc_persistence_errors(&self) {
        self.persistence_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot and reset interval counters. `active_documents` is passed in
    /// from the caller (computed from DashMap::len()).
    pub fn snapshot_and_reset(&self, active_documents: u64) -> MetricsSnapshot {
        let sync_total = self.sync_latency_total_us.swap(0, Ordering::Relaxed);
        let sync_count = self.sync_latency_count.swap(0, Ordering::Relaxed);
        let sync_max = self.sync_latency_max_us.swap(0, Ordering::Relaxed);

        let http_total = self.http_latency_total_us.swap(0, Ordering::Relaxed);
        let http_count = self.http_latency_count.swap(0, Ordering::Relaxed);
        let http_max = self.http_latency_max_us.swap(0, Ordering::Relaxed);

        let persist_total = self.persistence_latency_total_us.swap(0, Ordering::Relaxed);
        let persist_count = self.persistence_latency_count.swap(0, Ordering::Relaxed);
        let persist_max = self.persistence_latency_max_us.swap(0, Ordering::Relaxed);

        MetricsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            active_documents,

            websocket_failures: self.websocket_failures.swap(0, Ordering::Relaxed),
            pong_timeouts: self.pong_timeouts.swap(0, Ordering::Relaxed),
            documents_loaded: self.documents_loaded.swap(0, Ordering::Relaxed),
            documents_gc: self.documents_gc.swap(0, Ordering::Relaxed),
            sync_updates: self.sync_updates.swap(0, Ordering::Relaxed),
            http_updates: self.http_updates.swap(0, Ordering::Relaxed),
            persistence_ops: self.persistence_ops.swap(0, Ordering::Relaxed),
            persistence_errors: self.persistence_errors.swap(0, Ordering::Relaxed),

            sync_latency_avg_ms: avg_us_to_ms(sync_total, sync_count),
            sync_latency_max_ms: sync_max as f64 / 1000.0,
            http_latency_avg_ms: avg_us_to_ms(http_total, http_count),
            http_latency_max_ms: http_max as f64 / 1000.0,
            persistence_latency_avg_ms: avg_us_to_ms(persist_total, persist_count),
            persistence_latency_max_ms: persist_max as f64 / 1000.0,
        }
    }

    /// Read-only snapshot (no reset). Used by `/metrics` endpoint.
    pub fn snapshot(&self, active_documents: u64) -> MetricsSnapshot {
        let sync_total = self.sync_latency_total_us.load(Ordering::Relaxed);
        let sync_count = self.sync_latency_count.load(Ordering::Relaxed);
        let sync_max = self.sync_latency_max_us.load(Ordering::Relaxed);

        let http_total = self.http_latency_total_us.load(Ordering::Relaxed);
        let http_count = self.http_latency_count.load(Ordering::Relaxed);
        let http_max = self.http_latency_max_us.load(Ordering::Relaxed);

        let persist_total = self.persistence_latency_total_us.load(Ordering::Relaxed);
        let persist_count = self.persistence_latency_count.load(Ordering::Relaxed);
        let persist_max = self.persistence_latency_max_us.load(Ordering::Relaxed);

        MetricsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            active_documents,

            websocket_failures: self.websocket_failures.load(Ordering::Relaxed),
            pong_timeouts: self.pong_timeouts.load(Ordering::Relaxed),
            documents_loaded: self.documents_loaded.load(Ordering::Relaxed),
            documents_gc: self.documents_gc.load(Ordering::Relaxed),
            sync_updates: self.sync_updates.load(Ordering::Relaxed),
            http_updates: self.http_updates.load(Ordering::Relaxed),
            persistence_ops: self.persistence_ops.load(Ordering::Relaxed),
            persistence_errors: self.persistence_errors.load(Ordering::Relaxed),

            sync_latency_avg_ms: avg_us_to_ms(sync_total, sync_count),
            sync_latency_max_ms: sync_max as f64 / 1000.0,
            http_latency_avg_ms: avg_us_to_ms(http_total, http_count),
            http_latency_max_ms: http_max as f64 / 1000.0,
            persistence_latency_avg_ms: avg_us_to_ms(persist_total, persist_count),
            persistence_latency_max_ms: persist_max as f64 / 1000.0,
        }
    }
}

impl MetricsSnapshot {
    pub fn to_prometheus(&self) -> String {
        format!(
            "\
# HELP y_sweet_active_connections Current number of active WebSocket connections.
# TYPE y_sweet_active_connections gauge
y_sweet_active_connections {}

# HELP y_sweet_active_documents Current number of documents loaded in memory.
# TYPE y_sweet_active_documents gauge
y_sweet_active_documents {}

# HELP y_sweet_websocket_failures WebSocket stream errors in the last interval.
# TYPE y_sweet_websocket_failures gauge
y_sweet_websocket_failures {}

# HELP y_sweet_pong_timeouts Pong timeout disconnections in the last interval.
# TYPE y_sweet_pong_timeouts gauge
y_sweet_pong_timeouts {}

# HELP y_sweet_documents_loaded Documents loaded from store in the last interval.
# TYPE y_sweet_documents_loaded gauge
y_sweet_documents_loaded {}

# HELP y_sweet_documents_gc Documents garbage collected in the last interval.
# TYPE y_sweet_documents_gc gauge
y_sweet_documents_gc {}

# HELP y_sweet_sync_updates Sync updates received via WebSocket in the last interval.
# TYPE y_sweet_sync_updates gauge
y_sweet_sync_updates {}

# HELP y_sweet_http_updates Updates received via HTTP in the last interval.
# TYPE y_sweet_http_updates gauge
y_sweet_http_updates {}

# HELP y_sweet_persistence_ops Successful persistence operations in the last interval.
# TYPE y_sweet_persistence_ops gauge
y_sweet_persistence_ops {}

# HELP y_sweet_persistence_errors Failed persistence operations in the last interval.
# TYPE y_sweet_persistence_errors gauge
y_sweet_persistence_errors {}

# HELP y_sweet_sync_latency_avg_ms Average sync update processing latency (ms) in the last interval.
# TYPE y_sweet_sync_latency_avg_ms gauge
y_sweet_sync_latency_avg_ms {:.3}

# HELP y_sweet_sync_latency_max_ms Max sync update processing latency (ms) in the last interval.
# TYPE y_sweet_sync_latency_max_ms gauge
y_sweet_sync_latency_max_ms {:.3}

# HELP y_sweet_http_latency_avg_ms Average HTTP update processing latency (ms) in the last interval.
# TYPE y_sweet_http_latency_avg_ms gauge
y_sweet_http_latency_avg_ms {:.3}

# HELP y_sweet_http_latency_max_ms Max HTTP update processing latency (ms) in the last interval.
# TYPE y_sweet_http_latency_max_ms gauge
y_sweet_http_latency_max_ms {:.3}

# HELP y_sweet_persistence_latency_avg_ms Average persistence latency (ms) in the last interval.
# TYPE y_sweet_persistence_latency_avg_ms gauge
y_sweet_persistence_latency_avg_ms {:.3}

# HELP y_sweet_persistence_latency_max_ms Max persistence latency (ms) in the last interval.
# TYPE y_sweet_persistence_latency_max_ms gauge
y_sweet_persistence_latency_max_ms {:.3}
",
            self.active_connections,
            self.active_documents,
            self.websocket_failures,
            self.pong_timeouts,
            self.documents_loaded,
            self.documents_gc,
            self.sync_updates,
            self.http_updates,
            self.persistence_ops,
            self.persistence_errors,
            self.sync_latency_avg_ms,
            self.sync_latency_max_ms,
            self.http_latency_avg_ms,
            self.http_latency_max_ms,
            self.persistence_latency_avg_ms,
            self.persistence_latency_max_ms,
        )
    }

    pub fn log_summary(&self) {
        tracing::info!(
            active_connections = self.active_connections,
            active_documents = self.active_documents,
            websocket_failures = self.websocket_failures,
            pong_timeouts = self.pong_timeouts,
            docs_loaded = self.documents_loaded,
            docs_gc = self.documents_gc,
            sync_updates = self.sync_updates,
            http_updates = self.http_updates,
            persistence_ops = self.persistence_ops,
            persistence_errors = self.persistence_errors,
            sync_latency_avg_ms = format!("{:.3}", self.sync_latency_avg_ms),
            sync_latency_max_ms = format!("{:.3}", self.sync_latency_max_ms),
            http_latency_avg_ms = format!("{:.3}", self.http_latency_avg_ms),
            http_latency_max_ms = format!("{:.3}", self.http_latency_max_ms),
            persistence_latency_avg_ms = format!("{:.3}", self.persistence_latency_avg_ms),
            persistence_latency_max_ms = format!("{:.3}", self.persistence_latency_max_ms),
            "metrics (last 60s)"
        );
    }
}

/// Spawns a background task that logs metrics every 60 seconds.
/// Interval counters are reset after each log.
pub fn spawn_metrics_logger(
    metrics: Arc<Metrics>,
    docs: Arc<dashmap::DashMap<String, y_sweet_core::doc_sync::DocWithSyncKv>>,
    cancellation_token: CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let snap = metrics.snapshot_and_reset(docs.len() as u64);
                    snap.log_summary();
                }
                _ = cancellation_token.cancelled() => {
                    tracing::info!("Metrics logger shutting down.");
                    let snap = metrics.snapshot_and_reset(docs.len() as u64);
                    snap.log_summary();
                    break;
                }
            }
        }
    });
}
