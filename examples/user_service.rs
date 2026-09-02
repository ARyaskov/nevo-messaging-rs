//! A `user` service: `cargo run --example user_service`
//! (expects NATS on `nats://127.0.0.1:4222`, override with `NATS_URL`).

use std::time::Duration;

use nevo_messaging::acl::{AccessControl, AccessRule};
use nevo_messaging::prelude::*;
use nevo_messaging::rate_limit::RateLimitConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct GetById {
    id: BigInt,
}

#[derive(Deserialize)]
struct CreateUser {
    name: String,
    email: String,
}

#[derive(Serialize)]
struct User {
    id: BigInt,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());

    let health = HealthRegistry::builder("user").version("0.1.0").build();
    health.register("self", HealthKind::Both, || async { CheckResult::ok() });

    let router = Router::new("user")
        .service_version("0.1.0")
        .health(health)
        .rate_limit(RateLimitConfig::new(200.0, 100.0))
        .access_control(
            AccessControl::new()
                .rule(
                    AccessRule::new()
                        .topic("user-events")
                        .method("user.delete")
                        .allow(["coordinator"]),
                )
                .allow_all_by_default(true),
        )
        .route("user.getById", |ctx: Context, p: GetById| async move {
            tracing::info!(caller = ctx.caller.as_deref().unwrap_or("-"), id = %p.id, "getById");
            Ok(User {
                id: p.id,
                name: "John Doe".into(),
                email: "john@example.com".into(),
            })
        })
        .route("user.create", |_ctx: Context, p: CreateUser| async move {
            Ok(User {
                id: BigInt::from(123u32),
                name: p.name,
                email: p.email,
            })
        })
        .route("user.delete", |_ctx: Context, p: GetById| async move {
            Ok(serde_json::json!({ "deleted": p.id }))
        });

    let server = NatsServer::builder()
        .router(router)
        .servers([url])
        .queue_group("user")
        .drain_timeout(Duration::from_secs(10))
        .serve()
        .await?;
    println!("user service is listening on user-events; press Ctrl+C to stop");
    server
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
