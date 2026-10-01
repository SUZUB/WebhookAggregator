use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::Database;
use crate::*;

const MAX_ATTEMPTS: u32 = 5;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const BASE_DELAY: Duration = Duration::from_millis(500);
const MAX_DELAY: Duration = Duration::from_secs(30);

enum Attempt {
    Delivered,
    Retry(String),
    GiveUp(String),
}

#[derive(Clone)]
pub struct Dispatcher {
    database: Database,
    http: reqwest::Client,
    capacity_map: Arc<Mutex<HashMap<u32, Arc<Semaphore>>>>,
    pub pending_requests: Arc<Semaphore>,
}

impl Dispatcher {
    pub fn init(database: Database) -> Self {
        Self {
            database,
            http: reqwest::Client::new(),
            capacity_map: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Semaphore::new(0)),
        }
    }

    async fn service_semaphore(&self, id: u32) -> Result<Arc<Semaphore>, mysql_async::Error> {
        if let Some(sem) = self.capacity_map.lock().unwrap().get(&id) {
            return Ok(sem.clone());
        }

        let capacity = self.database.get_service_capacity(id).await?;
        info!("Loaded capacity {} for service {}", capacity, id);
        Ok(self
            .capacity_map
            .lock()
            .unwrap()
            .entry(id)
            .or_insert_with(|| Arc::new(Semaphore::new(capacity)))
            .clone())
    }

    async fn try_send(&self, url: &str, payload: &str) -> Attempt {
        let req = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload.to_owned())
            .send();

        match timeout(ATTEMPT_TIMEOUT, req).await {
            Err(_) => Attempt::Retry("timed out".into()),
            Ok(Err(e)) if e.is_builder() => Attempt::GiveUp(e.to_string()),
            Ok(Err(e)) => Attempt::Retry(e.to_string()),
            Ok(Ok(resp)) => {
                let s = resp.status();
                if s.is_success() {
                    Attempt::Delivered
                } else if s.is_server_error() || s == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    Attempt::Retry(s.to_string())
                } else {
                    Attempt::GiveUp(s.to_string())
                }
            }
        }
    }

    async fn handler(self) -> Result<(), mysql_async::Error> {
        let Some((client_id, event_id, service_id, target_url, payload)) =
            self.database.next_pending_request().await?
        else {
            debug!("Woken up but no pending query was available");
            return Ok(());
        };

        let sem = self.service_semaphore(service_id).await?;

        info!("Dispatched request to {}", target_url);

        let mut delay = BASE_DELAY;
        let mut status = "FAILED";

        for attempt in 1..=MAX_ATTEMPTS {
            let outcome = {
                let _permit = sem.acquire().await.expect("impossible");
                self.try_send(&target_url, &payload).await
            };

            match outcome {
                Attempt::Delivered => {
                    info!("Delivered to {} on attempt {}", target_url, attempt);
                    status = "SUCCESS";
                    break;
                }
                Attempt::GiveUp(why) => {
                    warn!("Delivery to {} rejected: {}", target_url, why);
                    break;
                }
                Attempt::Retry(why) if attempt < MAX_ATTEMPTS => {
                    warn!(
                        "Attempt {}/{} to {} failed: {}, retrying in {:?}",
                        attempt, MAX_ATTEMPTS, target_url, why, delay
                    );
                    sleep(delay).await;
                    delay = (delay * 2).min(MAX_DELAY);
                }
                Attempt::Retry(why) => {
                    warn!(
                        "Giving up on {} after {} attempts: {}",
                        target_url, MAX_ATTEMPTS, why
                    );
                }
            }
        }

        self.database
            .set_query_status(client_id, &event_id, service_id, status)
            .await
    }

    pub async fn run(&self, stop: CancellationToken) -> Result<(), mysql_async::Error> {
        let mut tasks = JoinSet::new();
        info!("Dispatcher started");

        loop {
            tokio::select! {
                permit = self.pending_requests.acquire() => {
                    permit.expect("impossible").forget();
                    tasks.spawn(self.clone().handler());

                    while let Some(res) = tasks.try_join_next() {
                        report(res);
                    }
                }
                _ = stop.cancelled() => break,
            }
        }

        while let Ok(permit) = self.pending_requests.try_acquire() {
            permit.forget();
            tasks.spawn(self.clone().handler());
        }

        info!("Dispatcher stopping, draining {} tasks", tasks.len());
        while let Some(res) = tasks.join_next().await {
            report(res);
        }
        info!("Dispatcher stopped");
        Ok(())
    }
}

fn report(res: Result<Result<(), mysql_async::Error>, tokio::task::JoinError>) {
    match res {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!("Dispatch database error: {}", e),
        Err(e) => error!("Dispatch task failed: {}", e),
    }
}
