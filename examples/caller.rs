//! Calls the `user` service from `examples/user_service.rs` (or the original
//! TypeScript `examples/nats-user`): `cargo run --example caller`.

use std::time::Duration;

use futures::StreamExt;
use nevo_messaging::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize)]
struct GetById {
    id: BigInt,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct User {
    id: BigInt,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let client = NatsClient::builder()
        .servers([url])
        .service_name("coordinator")
        .timeout(Duration::from_secs(5))
        .retry(RetryPolicy::new().max_attempts(3))
        .circuit_breaker(CircuitBreakerConfig::consecutive(5))
        .compression(Compression::gzip().threshold(1024))
        .connect()
        .await?;

    let user: User = client
        .query(
            "user",
            "user.getById",
            &GetById {
                id: BigInt::from(42u8),
            },
        )
        .await?;
    println!("query → {user:?}");

    let created: User = client
        .query_with(
            "user",
            "user.create",
            &json!({ "name": "Ada", "email": "ada@example.com" }),
            CallOptions::new().idempotency_key("create-ada"),
        )
        .await?;
    println!("create → {created:?}");

    let mut updates = client
        .subscribe::<serde_json::Value>("user", "user.updated", SubscribeOptions::new())
        .await?;
    client
        .publish("user", "user.updated", &json!({ "id": 42 }))
        .await?;
    if let Some(Ok(msg)) = tokio::time::timeout(Duration::from_secs(2), updates.next())
        .await
        .ok()
        .flatten()
    {
        println!("subscribe → {} from {:?}", msg.params, msg.meta.service);
    }

    client
        .emit(
            "user",
            "user.delete",
            &GetById {
                id: BigInt::from(7u8),
            },
        )
        .await?;
    client
        .broadcast("system.status", &json!({ "ok": true }))
        .await?;

    match client
        .query::<_, User>("user", "user.missing", &json!({}))
        .await
    {
        Err(err) if err.code() == ErrorCode::MethodNotFound => println!("error → {err}"),
        other => println!("unexpected: {other:?}"),
    }

    client.close(Duration::from_secs(2)).await
}
