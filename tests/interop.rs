//! Wire-compatibility test against the original TypeScript implementation.
//! Needs `NEVO_TEST_NATS_URL`, Node.js and `npm install` inside `tests/interop`.

#![cfg(feature = "nats")]

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::StreamExt;
use nevo_messaging::client::SubscribeOptions;
use nevo_messaging::resilience::RetryPolicy;
use nevo_messaging::wire::BigInt;
use nevo_messaging::{Context, Error, ErrorCode, NatsClient, NatsServer, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

fn interop_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("interop")
}

fn prerequisites() -> Option<String> {
    let url = std::env::var("NEVO_TEST_NATS_URL")
        .ok()
        .filter(|u| !u.is_empty())?;
    if !interop_dir().join("node_modules").exists() {
        eprintln!(
            "tests/interop/node_modules is missing; run `npm install` there to enable the interop test"
        );
        return None;
    }
    Some(url)
}

struct Peer {
    child: Child,
    lines: mpsc::UnboundedReceiver<Value>,
}

impl Peer {
    async fn spawn(url: &str, rust_service: &str, ts_service: &str) -> Peer {
        let mut child = Command::new("node")
            .arg(interop_dir().join("peer.cjs"))
            .env("NEVO_TEST_NATS_URL", url)
            .env("NEVO_INTEROP_RUST_SERVICE", rust_service)
            .env("NEVO_INTEROP_TS_SERVICE", ts_service)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("node must be installed for the interop test");
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                match serde_json::from_str::<Value>(&line) {
                    Ok(value) => {
                        if tx.send(value).is_err() {
                            break;
                        }
                    }
                    Err(_) => eprintln!("[peer] {line}"),
                }
            }
        });
        Peer { child, lines }
    }

    async fn expect(&mut self, kind: &str) -> Value {
        loop {
            let value = tokio::time::timeout(Duration::from_secs(20), self.lines.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for peer line {kind:?}"))
                .unwrap_or_else(|| panic!("peer exited before emitting {kind:?}"));
            if value["kind"] == "fatal" {
                panic!("peer failed: {}", value["message"]);
            }
            if value["kind"] == kind {
                return value;
            }
            eprintln!("[peer] {value}");
        }
    }

    async fn finish(mut self) {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.shutdown().await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await;
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GetById {
    id: BigInt,
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: BigInt,
    name: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    tags: Vec<String>,
    big: BigInt,
    #[serde(default)]
    list: Vec<Option<BigInt>>,
    #[serde(default, rename = "idType")]
    id_type: Option<String>,
}

fn rust_router(service: &str, audits: Arc<AtomicU32>) -> Router {
    Router::new(service)
        .route("user.getById", |_ctx: Context, p: GetById| async move {
            Ok(json!({
                "id": p.id,
                "name": "Rust Eddie",
                "createdAt": "2026-01-02T03:04:05.000Z",
                "tags": ["@@nevo:tricky", "plain"],
                "big": BigInt::parse("1180591620717411303425").unwrap(),
            }))
        })
        .route("user.getById@v2", |_ctx: Context, p: GetById| async move {
            Ok(json!({"id": p.id, "version": 2}))
        })
        .route("user.echo", |ctx: Context, p: Value| async move {
            Ok(json!({
                "params": p,
                "caller": ctx.caller,
                "tenant": ctx.tenant_id(),
                "header": ctx.header("x-h"),
                "idempotencyKey": ctx.idempotency_key(),
                "attempt": ctx.header("nevo-attempt"),
            }))
        })
        .route("user.fail", |_ctx: Context, _p: Value| async move {
            Err::<(), _>(
                Error::new(ErrorCode::ValidationFailed, "rs says no").with_detail("field", "id"),
            )
        })
        .route("user.large", |_ctx: Context, p: Value| async move {
            Ok("x".repeat(p["size"].as_u64().unwrap_or(0) as usize))
        })
        .route("audit.write", move |_ctx: Context, _p: Value| {
            let audits = Arc::clone(&audits);
            async move {
                audits.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
}

#[tokio::test]
async fn typescript_and_rust_peers_interoperate() {
    let Some(url) = prerequisites() else {
        eprintln!("skipping interop test");
        return;
    };
    let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let rust_service = format!("rs-user-{suffix}");
    let ts_service = format!("ts-user-{suffix}");
    let audits = Arc::new(AtomicU32::new(0));
    let server = NatsServer::bind(rust_router(&rust_service, audits.clone()), [url.as_str()])
        .await
        .unwrap();
    let client = NatsClient::builder()
        .servers([url.as_str()])
        .service_name("rs-caller")
        .timeout(Duration::from_secs(5))
        .retry(RetryPolicy::disabled())
        .connect()
        .await
        .unwrap();
    let mut placed = client
        .subscribe::<Value>(&rust_service, "order.placed", SubscribeOptions::new())
        .await
        .unwrap();
    let mut broadcasts = client
        .subscribe::<Value>("__broadcast", "system.status", SubscribeOptions::new())
        .await
        .unwrap();

    let mut peer = Peer::spawn(&url, &rust_service, &ts_service).await;
    peer.expect("ready").await;

    let query = peer.expect("query").await;
    assert_eq!(query["id"], "9007199254740993n");
    assert_eq!(query["idType"], "bigint");
    assert_eq!(query["big"], "1180591620717411303425n");
    assert_eq!(query["bigType"], "bigint");
    assert_eq!(query["tricky"], "@@nevo:tricky");
    assert_eq!(query["name"], "Rust Eddie");
    assert_eq!(peer.expect("query-v2").await["version"], 2);

    let error = peer.expect("error").await;
    assert_eq!(error["code"], 7);
    assert_eq!(error["message"], "rs says no");
    assert_eq!(error["service"], rust_service);
    assert_eq!(error["field"], "id");

    let echo = peer.expect("echo").await;
    assert_eq!(echo["echoed"]["params"]["s"], "5n");
    assert_eq!(echo["echoed"]["params"]["n"], "2n");
    assert_eq!(echo["echoed"]["caller"], "ts-caller");
    assert_eq!(echo["echoed"]["tenant"], "t1");
    assert_eq!(echo["echoed"]["header"], "v");
    assert_eq!(echo["echoed"]["idempotencyKey"], "k-1");
    assert_eq!(echo["echoed"]["attempt"], "1");

    let contract = peer.expect("contract").await;
    assert_eq!(contract["protocol"], "1");
    assert!(
        contract["methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m == "user.getById@v2")
    );
    assert_eq!(peer.expect("large").await["length"], 40000);
    let json_query = peer.expect("json-query").await;
    assert_eq!(json_query["id"], "7n");
    assert_eq!(json_query["idType"], "bigint");
    peer.expect("done").await;

    let published = tokio::time::timeout(Duration::from_secs(5), placed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(published.params["n"], 2);
    assert_eq!(published.meta.service.as_deref(), Some("ts-caller"));
    let broadcast = tokio::time::timeout(Duration::from_secs(5), broadcasts.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(broadcast.params["ok"], true);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(audits.load(Ordering::SeqCst), 1);

    let user: User = client
        .query(
            &ts_service,
            "user.getById",
            &GetById {
                id: BigInt::parse("9007199254740993").unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(user.id.as_str(), "9007199254740993");
    assert_eq!(user.id_type.as_deref(), Some("bigint"));
    assert_eq!(user.name, "TS Eddie");
    assert_eq!(user.created_at, "2026-01-02T03:04:05.000Z");
    assert_eq!(user.tags[0], "@@nevo:tricky");
    assert_eq!(user.big.as_str(), "1180591620717411303425");
    assert_eq!(user.list, vec![None, Some(BigInt::from(1u8))]);

    let v2: Value = client
        .query_with(
            &ts_service,
            "user.getById",
            &GetById {
                id: BigInt::from(1u8),
            },
            nevo_messaging::CallOptions::new().version("v2"),
        )
        .await
        .unwrap();
    assert_eq!(v2["version"], 2);

    let echoed: Value = client
        .query(
            &ts_service,
            "user.echo",
            &json!({"s": "@@nevo:str:x", "n": 1}),
        )
        .await
        .unwrap();
    assert_eq!(echoed["s"], "@@nevo:str:x");
    assert_eq!(echoed["n"], 1);

    let err = client
        .query::<_, Value>(&ts_service, "user.fail", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::ValidationFailed);
    assert_eq!(err.message(), "ts says no");
    assert_eq!(err.detail("field").and_then(Value::as_str), Some("id"));
    assert_eq!(
        err.service(),
        Some("unknown"),
        "TypeScript errors without a service name say \"unknown\""
    );

    let err = client
        .query::<_, Value>(&ts_service, "user.nope", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::MethodNotFound);

    let large: String = client
        .query(&ts_service, "user.large", &json!({"size": 30000}))
        .await
        .unwrap();
    assert_eq!(large.len(), 30000);

    let contract: Value = client
        .query(&ts_service, "nevo.contract", &json!({}))
        .await
        .unwrap();
    assert_eq!(contract["serviceName"], ts_service);

    peer.finish().await;
    client.close(Duration::from_secs(1)).await.unwrap();
    server.shutdown().await.unwrap();
}
