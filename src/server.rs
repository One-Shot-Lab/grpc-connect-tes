use axum::{extract::State, http::StatusCode, routing::get, Router};
use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::Arc;
use tokio::net::TcpListener;

async fn metrics_handler(State(registry): State<Arc<Registry>>) -> Result<String, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    String::from_utf8(buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn health_handler() -> &'static str {
    "OK"
}

pub async fn start_metrics_server(registry: Arc<Registry>, port: u16) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(registry);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    println!("Prometheus metrics server listening on port {}", port);
    axum::serve(listener, app).await.unwrap();
}