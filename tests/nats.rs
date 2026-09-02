//! End-to-end tests against a real NATS server. Set `NEVO_TEST_NATS_URL`
//! (for example `nats://127.0.0.1:4222`, JetStream enabled) to run them.

#![cfg(feature = "nats")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::StreamExt;
use nevo_messaging::acl::{AccessControl, AccessRule};
use nevo_messaging::client::{CallOptions, SubscribeOptions};
use nevo_messaging::discovery::DiscoveryConfig;
use nevo_messaging::filter::SubscriptionFilter;
use nevo_messaging::health::{CheckResult, HealthKind, HealthRegistry};
use nevo_messaging::resilience::RetryPolicy;
use nevo_messaging::transport::nats::{
    ConsumerSpec, PublishOptions, PullOptions, RequestManyOptions, StreamSpec,
};
use nevo_messaging::wire::BigInt;
use nevo_messaging::{
    Codec, Compression, Context, Error, ErrorCode, NatsClient, NatsServer, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

fn nats_url() -> Option<String> {
    std::env::var("NEVO_TEST_NATS_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

macro_rules! require_nats {
    () => {
        match nats_url() {
            Some(url) => url,
            None => {
                eprintln!("NEVO_TEST_NATS_URL is not set; skipping");
                return;
            }
        }
    };
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

#[derive(Debug, Serialize, Deserialize)]
struct GetById {
    id: BigInt,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct User {
    id: BigInt,
    name: String,
    tags: Vec<String>,
}

fn user_router(service: &str, calls: Arc<AtomicU32>) -> Router {
    let counted = Arc::clone(&calls);
    Router::new(service)
        .route("user.getById", move |_ctx: Context, p: GetById| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(User {
                    id: p.id,
                    name: "Eddie".into(),
                    tags: vec!["@@nevo:tricky".into(), "plain".into()],
                })
            }
        })
        .route("user.getById@v2", |_ctx: Context, p: GetById| async move {
            Ok(json!({"id": p.id, "version": 2}))
        })
        .route("user.echo", |ctx: Context, p: Value| async move {
            Ok(json!({"params": p, "caller": ctx.caller, "tenant": ctx.tenant_id(), "chain": ctx.chain_id}))
        })
        .route("user.fail", |_ctx: Context, _p: Value| async move {
            Err::<(), _>(Error::new(ErrorCode::ValidationFailed, "nope").with_detail("field", "id"))
        })
        .route("user.slow", |_ctx: Context, _p: Value| async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            Ok("late")
        })
        .route("user.big", |_ctx: Context, p: Value| async move {
            let size = p["size"].as_u64().unwrap_or(0) as usize;
            Ok("x".repeat(size))
        })
}

async fn client(url: &str, name: &str) -> NatsClient {
    NatsClient::builder()
        .servers([url])
        .service_name(name)
        .timeout(Duration::from_secs(3))
        .retry(RetryPolicy::disabled())
        .connect()
        .await
        .expect("connect client")
}

#[tokio::test]
async fn query_round_trips_in_both_codecs() {
    let url = require_nats!();
    let service = unique("user");
    let calls = Arc::new(AtomicU32::new(0));
    let server = NatsServer::bind(user_router(&service, calls.clone()), [url.as_str()])
        .await
        .unwrap();

    let msgpack = client(&url, "frontend").await;
    let json = NatsClient::builder()
        .servers([url.as_str()])
        .service_name("frontend")
        .codec(Codec::Json)
        .connect()
        .await
        .unwrap();

    let big = BigInt::parse("9007199254740993").unwrap();
    for c in [&msgpack, &json] {
        let user: User = c
            .query(&service, "user.getById", &GetById { id: big.clone() })
            .await
            .unwrap();
        assert_eq!(user.id, big);
        assert_eq!(user.tags[0], "@@nevo:tricky");
    }
    let v2: Value = msgpack
        .query_with(
            &service,
            "user.getById",
            &GetById { id: big.clone() },
            CallOptions::new().version("v2"),
        )
        .await
        .unwrap();
    assert_eq!(v2["version"], 2);
    let echoed: Value = json
        .query_with(
            &service,
            "user.echo",
            &json!({"a": 1}),
            CallOptions::new().tenant_id("t1"),
        )
        .await
        .unwrap();
    assert_eq!(echoed["params"]["a"], 1);
    assert_eq!(echoed["caller"], "frontend");
    assert_eq!(echoed["tenant"], "t1");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    msgpack.close(Duration::from_secs(1)).await.unwrap();
    json.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn errors_and_timeouts_surface_with_codes() {
    let url = require_nats!();
    let service = unique("user");
    let server = NatsServer::bind(user_router(&service, Arc::default()), [url.as_str()])
        .await
        .unwrap();
    let c = client(&url, "frontend").await;

    let err = c
        .query::<_, Value>(&service, "user.fail", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValidationFailed);
    assert_eq!(err.message(), "nope");
    assert_eq!(err.service(), Some(service.as_str()));

    let err = c
        .query::<_, Value>(&service, "user.missing", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::MethodNotFound);

    let err = c
        .query_with::<_, Value>(
            &service,
            "user.slow",
            &json!({}),
            CallOptions::new().timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Timeout);
    assert!(err.is_retryable());

    let err = c
        .query::<_, Value>(&unique("ghost"), "x", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ServiceUnavailable);

    let bad: Result<Value, _> = c
        .query(&service, "user.getById", &json!({"id": "not-a-number"}))
        .await;
    assert_eq!(bad.unwrap_err().code(), ErrorCode::ValidationFailed);

    c.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn publish_subscribe_and_broadcast() {
    let url = require_nats!();
    let service = unique("orders");
    let producer = client(&url, "producer").await;
    let consumer = client(&url, "consumer").await;

    let mut sub = consumer
        .subscribe::<Value>(
            &service,
            "order.placed",
            SubscribeOptions::new().filter(SubscriptionFilter::new().meta("tenantId", "eu")),
        )
        .await
        .unwrap();
    let mut wildcard = consumer
        .subscribe::<Value>(&service, "order.*", SubscribeOptions::new())
        .await
        .unwrap();
    let mut broadcasts = consumer
        .subscribe::<Value>("__broadcast", "system.status", SubscribeOptions::new())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    producer
        .publish_with(
            &service,
            "order.placed",
            &json!({"n": 1}),
            CallOptions::new().tenant_id("us"),
        )
        .await
        .unwrap();
    producer
        .publish_with(
            &service,
            "order.placed",
            &json!({"n": 2}),
            CallOptions::new().tenant_id("eu"),
        )
        .await
        .unwrap();
    producer
        .broadcast("system.status", &json!({"ok": true}))
        .await
        .unwrap();

    let filtered = tokio::time::timeout(Duration::from_secs(3), sub.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(filtered.params["n"], 2);
    assert_eq!(filtered.meta.tenant_id.as_deref(), Some("eu"));
    assert_eq!(filtered.method, "order.placed@v1");
    let first = tokio::time::timeout(Duration::from_secs(3), wildcard.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(3), wildcard.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.params["n"], 1);
    assert_eq!(second.params["n"], 2);
    let status = tokio::time::timeout(Duration::from_secs(3), broadcasts.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(status.params["ok"], true);
    assert_eq!(status.subject, "__broadcast.system.status");

    producer.close(Duration::from_secs(1)).await.unwrap();
    consumer.close(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn emit_reaches_the_router_without_a_reply() {
    let url = require_nats!();
    let service = unique("audit");
    let hits = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&hits);
    let router = Router::new(&service).route("audit.write", move |_ctx: Context, _p: Value| {
        let seen = Arc::clone(&seen);
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let server = NatsServer::bind(router, [url.as_str()]).await.unwrap();
    let c = client(&url, "frontend").await;
    for i in 0..3 {
        c.emit(&service, "audit.write", &json!({"i": i}))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    c.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn request_many_and_queue_groups() {
    let url = require_nats!();
    let service = unique("user");
    let a = NatsServer::bind(user_router(&service, Arc::default()), [url.as_str()])
        .await
        .unwrap();
    let b = NatsServer::bind(user_router(&service, Arc::default()), [url.as_str()])
        .await
        .unwrap();
    let c = client(&url, "frontend").await;

    let replies: Vec<Value> = c
        .request_many(
            &service,
            "user.echo",
            &json!({"x": 1}),
            RequestManyOptions::new()
                .max_messages(2)
                .max_wait(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(replies.len(), 2);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();

    let group = unique("grp");
    let hits_a = Arc::new(AtomicU32::new(0));
    let hits_b = Arc::new(AtomicU32::new(0));
    let a = NatsServer::builder()
        .router(user_router(&service, hits_a.clone()))
        .servers([url.as_str()])
        .queue_group(&group)
        .serve()
        .await
        .unwrap();
    let b = NatsServer::builder()
        .router(user_router(&service, hits_b.clone()))
        .servers([url.as_str()])
        .queue_group(&group)
        .serve()
        .await
        .unwrap();
    for _ in 0..10 {
        let _: User = c
            .query(
                &service,
                "user.getById",
                &GetById {
                    id: BigInt::from(1u8),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        hits_a.load(Ordering::SeqCst) + hits_b.load(Ordering::SeqCst),
        10
    );
    c.close(Duration::from_secs(1)).await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn compression_is_negotiated_per_direction() {
    let url = require_nats!();
    let service = unique("user");
    let router =
        user_router(&service, Arc::default()).compression(Compression::gzip().threshold(256));
    let server = NatsServer::bind(router, [url.as_str()]).await.unwrap();
    let c = NatsClient::builder()
        .servers([url.as_str()])
        .compression(Compression::deflate().threshold(1))
        .connect()
        .await
        .unwrap();
    let out: String = c
        .query(&service, "user.big", &json!({"size": 50_000}))
        .await
        .unwrap();
    assert_eq!(out.len(), 50_000);
    let small: String = c
        .query(&service, "user.big", &json!({"size": 10}))
        .await
        .unwrap();
    assert_eq!(small.len(), 10);
    c.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn idempotency_keys_deduplicate_on_both_sides() {
    let url = require_nats!();
    let service = unique("user");
    let calls = Arc::new(AtomicU32::new(0));
    let server = NatsServer::bind(user_router(&service, calls.clone()), [url.as_str()])
        .await
        .unwrap();
    let c = client(&url, "frontend").await;
    let params = GetById {
        id: BigInt::from(5u8),
    };
    let opts = CallOptions::new().idempotency_key("charge-1");
    let first: User = c
        .query_with(&service, "user.getById", &params, opts.clone())
        .await
        .unwrap();
    let second: User = c
        .query_with(&service, "user.getById", &params, opts.clone())
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!c.response_cache().is_empty());

    c.response_cache().clear();
    let third: User = c
        .query_with(&service, "user.getById", &params, opts.clone())
        .await
        .unwrap();
    assert_eq!(third, first);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "server replays the stored response"
    );

    c.response_cache().clear();
    let conflict = c
        .query_with::<_, User>(
            &service,
            "user.getById",
            &GetById {
                id: BigInt::from(6u8),
            },
            opts,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), ErrorCode::IdempotencyKeyConflict);
    c.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn builtins_acl_and_health() {
    let url = require_nats!();
    let service = unique("user");
    let health = HealthRegistry::builder(&service).version("1.2.3").build();
    health.register("db", HealthKind::Readiness, || async { CheckResult::ok() });
    let router = user_router(&service, Arc::default())
        .service_version("1.2.3")
        .health(health)
        .access_control(
            AccessControl::new().rule(
                AccessRule::new()
                    .topic(format!("{service}-events"))
                    .method("user.echo")
                    .allow(["frontend"]),
            ),
        );
    let server = NatsServer::bind(router, [url.as_str()]).await.unwrap();

    let frontend = client(&url, "frontend").await;
    let contract: Value = frontend
        .query(&service, "nevo.contract", &json!({}))
        .await
        .unwrap();
    assert_eq!(contract["serviceName"], service);
    assert_eq!(contract["serviceVersion"], "1.2.3");
    assert!(
        contract["methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["signalName"] == "user.getById" && m["version"] == "v2")
    );
    let ready: Value = frontend
        .query(&service, "nevo.ready", &json!({}))
        .await
        .unwrap();
    assert_eq!(ready["status"], "ok");
    assert_eq!(ready["checks"]["db"]["kind"], "readiness");

    let stranger = client(&url, "stranger").await;
    let denied = stranger
        .query::<_, Value>(&service, "user.echo", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), ErrorCode::Unauthorized);
    let contract: Value = stranger
        .query(&service, "nevo.contract", &json!({}))
        .await
        .unwrap();
    assert_eq!(contract["protocol"], "1");

    frontend.close(Duration::from_secs(1)).await.unwrap();
    stranger.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn discovery_heartbeats_populate_the_registry() {
    let url = require_nats!();
    let name = unique("svc");
    let discovery = DiscoveryConfig::new()
        .heartbeat(Duration::from_millis(100))
        .ttl(Duration::from_millis(500))
        .capabilities(["a.b"]);
    let announcer = NatsClient::builder()
        .servers([url.as_str()])
        .service_name(&name)
        .discovery(discovery.clone())
        .connect()
        .await
        .unwrap();
    let watcher = NatsClient::builder()
        .servers([url.as_str()])
        .service_name(unique("watcher"))
        .discovery(discovery)
        .connect()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(watcher.is_service_available(&name));
    let entry = watcher
        .discovered()
        .into_iter()
        .find(|e| e.service_name() == name)
        .unwrap();
    assert_eq!(entry.announcement.transport, "nats");
    assert_eq!(
        entry.announcement.capabilities.as_deref(),
        Some(&["a.b".to_owned()][..])
    );
    announcer.close(Duration::from_secs(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!watcher.is_service_available(&name));
    watcher.close(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn jetstream_publish_and_pull() {
    let url = require_nats!();
    let stream = unique("NEVO_TEST").to_uppercase().replace('-', "_");
    let subject = format!("{}.events", stream.to_lowercase());
    let c = client(&url, "js").await;
    let js = c.jetstream();
    js.ensure_stream(
        &StreamSpec::new(&stream, [subject.as_str()]).duplicate_window(Duration::from_secs(30)),
    )
    .await
    .unwrap();
    let first = js
        .publish(
            &subject,
            &json!({"n": 1}),
            PublishOptions::new().msg_id("m1"),
        )
        .await
        .unwrap();
    let dup = js
        .publish(
            &subject,
            &json!({"n": 1}),
            PublishOptions::new().msg_id("m1"),
        )
        .await
        .unwrap();
    assert!(!first.duplicate);
    assert!(dup.duplicate);
    js.publish(&subject, &json!({"n": 2}), PublishOptions::new())
        .await
        .unwrap();

    let mut sub = js
        .pull_subscribe(
            &ConsumerSpec::new(&stream, "worker").ack_wait(Duration::from_secs(5)),
            PullOptions::new().batch(2),
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let value: Value = msg.decode().unwrap();
        assert!(msg.info().unwrap().stream_sequence > 0);
        msg.ack().await.unwrap();
        seen.push(value["n"].as_u64().unwrap());
    }
    assert_eq!(seen, vec![1, 2]);
    drop(sub);
    js.context().delete_stream(&stream).await.unwrap();
    c.close(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_inflight_handlers() {
    let url = require_nats!();
    let service = unique("user");
    let server = NatsServer::bind(user_router(&service, Arc::default()), [url.as_str()])
        .await
        .unwrap();
    let c = client(&url, "frontend").await;
    let pending = {
        let c = c.clone();
        let service = service.clone();
        tokio::spawn(async move {
            c.query::<_, String>(&service, "user.slow", &json!({}))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    server.shutdown().await.unwrap();
    assert_eq!(pending.await.unwrap().unwrap(), "late");
    c.close(Duration::from_secs(1)).await.unwrap();
}
