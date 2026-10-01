use mysql_async::TxOpts;

use super::*;

impl Database {
    pub async fn get_service_capacity(&self, service_id: u32) -> Result<usize, mysql_async::Error> {
        let mut conn = self.pool.get_conn().await?;

        let Some(capacity) = conn
            .exec_first(
                r"SELECT capacity FROM services WHERE status = 'UP' AND id = ?;",
                (service_id,),
            )
            .await?
        else {
            unreachable!();
        };

        Ok(capacity)
    }

    pub async fn next_pending_request(
        &self,
    ) -> Result<Option<(u32, Vec<u8>, u32, String, String)>, mysql_async::Error> {
        let mut conn = self.pool.get_conn().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;

        let row: Option<(String, String, u32, Vec<u8>, u32)> = tx
            .exec_first(
                r"SELECT s.url, q.payload, q.client_id, q.event_id, q.service_id
                        FROM queries q JOIN services s
                            ON q.service_id = s.id
                        WHERE q.status = 'PENDING'
                        ORDER BY q.recv_time
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED;",
                (),
            )
            .await?;

        let Some((url, payload, client_id, event_id, service_id)) = row else {
            tx.rollback().await?;
            return Ok(None);
        };

        tx.exec_drop(
            r"UPDATE queries
                    SET status = 'PROCESSING'
                    WHERE client_id = ? AND event_id = ? AND service_id = ?;",
            (client_id, &event_id, service_id),
        )
        .await?;

        tx.commit().await?;

        Ok(Some((client_id, event_id, service_id, url, payload)))
    }

    pub async fn set_query_status(
        &self,
        client_id: u32,
        event_id: &[u8],
        service_id: u32,
        status: &str,
    ) -> Result<(), mysql_async::Error> {
        let mut conn = self.pool.get_conn().await?;

        conn.exec_drop(
            r"UPDATE queries
                    SET status = ?
                    WHERE client_id = ? AND event_id = ? AND service_id = ?;",
            (status, client_id, event_id, service_id),
        )
        .await
    }
}
