pub mod request;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Semaphore};

use crate::Database;
use crate::db::query::{Query, QueryErr};
use crate::*;
use request::Request;

async fn request_handler(
    State((db, signal)): State<(Database, Arc<Semaphore>)>,
    Path(endpoint): Path<String>,
    Json(request): Json<Request>,
) -> Result<StatusCode, QueryErr> {
    info!("Got query on endpoint: {}", endpoint);
    let query = Query::try_from(request)?;

    db.process_query(&query, &endpoint).await?;

    signal.add_permits(1);

    Ok(StatusCode::OK)
}

pub struct WebListener {
    iface: String,
    port: u16,
    router: Router,
}

impl WebListener {
    pub fn init(database: Database, signal: Arc<Semaphore>) -> Self {
        let router = Router::new()
            .route("/webhook/{endpoint}", post(request_handler))
            .with_state((database, signal));

        Self {
            iface: "0.0.0.0".to_string(),
            port: 31337,
            router,
        }
    }

    pub async fn run(self) -> std::io::Result<()> {
        let addr = format! {"{}:{}", self.iface, self.port};
        let listener = TcpListener::bind(&addr).await?;

        axum::serve(listener, self.router)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
    }
}
