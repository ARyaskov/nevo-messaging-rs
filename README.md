# nevo-messaging

A Rust 2024 implementation of the [Nevo Messaging](https://github.com/ARyaskov/nevo-messaging)
protocol. Services written with this crate exchange messages with services built on the
original TypeScript/NestJS framework, byte for byte: same envelopes, same NATS subjects,
same MessagePack/JSON encoding, same bigint sentinels, compression headers, error codes and
built-in signals.

The first transport is NATS. The client, router and resilience layers are written against a
small [`Transport`](docs/transports.md) contract so other brokers can be added without
touching them.

## Highlights

- Request/reply (`query`), fire-and-forget (`emit`), pub/sub (`publish` / `subscribe`),
  `broadcast`, scatter/gather (`request_many`), streaming replies, wildcard subscriptions.
- `Router`: typed async handlers, method versioning (`user.getById@v2`), interceptors,
  replay guard, ACL with optional token verification, rate limiting, claim-before-execute
  idempotency with pluggable distributed stores, dead-letter routing, `nevo.contract` and
  health built-ins.
- Client: retry with full jitter and `retryAfterMs` hints, consecutive or sliding-window
  circuit breaker, hedging, idempotent response cache, graceful drain.
- MessagePack (default) and JSON codecs, per-message codec sniffing, gzip/deflate/zstd with
  decompression bombs rejected, 4 MiB payload cap.
- Chain-id propagation through Tokio task-locals, optional W3C `traceparent`, heartbeat
  discovery, JetStream helpers with real acks, `tracing` events and optional `metrics`.
- Verified against the original package: `tests/interop.rs` drives the real
  `@riaskov/nevo-messaging` client and router through Node.js in both directions.

## Requirements

- Rust 1.95 or newer (edition 2024).
- A NATS 2.x server; enable JetStream (`nats-server -js`) for the JetStream helpers.

```toml
[dependencies]
nevo-messaging = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## A service

```rust,no_run
use nevo_messaging::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct GetById { id: BigInt }

#[derive(Serialize)]
struct User { id: BigInt, name: String, email: String }

#[tokio::main]
async fn main() -> Result<()> {
    let router = Router::new("user")
        .service_version("1.0.0")
        .route("user.getById", |_ctx: Context, p: GetById| async move {
            Ok(User { id: p.id, name: "John Doe".into(), email: "john@example.com".into() })
        })
        .route("user.getById@v2", |_ctx: Context, p: GetById| async move {
            Ok(serde_json::json!({ "id": p.id, "version": 2 }))
        });

    let server = NatsServer::builder()
        .router(router)
        .servers(["nats://127.0.0.1:4222"])
        .serve()
        .await?;
    server.run_until(async { let _ = tokio::signal::ctrl_c().await; }).await
}
```

Handlers are `async fn(Context, P) -> Result<R>` for any `P: Deserialize`, `R: Serialize`.
`Context` carries the envelope uuid, method, caller identity, tenant, headers and chain id.

## A caller

```rust,no_run
use futures::StreamExt;
use nevo_messaging::prelude::*;
use std::time::Duration;
# use serde::{Deserialize, Serialize};
# #[derive(Serialize)] struct GetById { id: BigInt }
# #[derive(Serialize, Deserialize)] struct User { id: BigInt, name: String }
# async fn run() -> Result<()> {

let client = NatsClient::builder()
    .servers(["nats://127.0.0.1:4222"])
    .service_name("coordinator")
    .timeout(Duration::from_secs(5))
    .retry(RetryPolicy::new().max_attempts(3))
    .circuit_breaker(CircuitBreakerConfig::consecutive(5))
    .compression(Compression::gzip().threshold(1024))
    .connect()
    .await?;

let user: User = client.query("user", "user.getById", &GetById { id: BigInt::from(42u8) }).await?;
let v2: serde_json::Value = client
    .query_with("user", "user.getById", &GetById { id: BigInt::from(42u8) }, CallOptions::new().version("v2"))
    .await?;

client.emit("notifications", "user.created", &user).await?;
client.publish("user", "user.updated", &user).await?;
client.broadcast("system.status", &serde_json::json!({ "ok": true })).await?;

let mut updates = client.subscribe::<User>("user", "user.updated", SubscribeOptions::new()).await?;
while let Some(Ok(msg)) = updates.next().await {
    println!("{} from {:?}", msg.params.name, msg.meta.service);
}
# Ok(()) }
```

Errors come back as `Error` with the peer's `ErrorCode`, message, details and service; use
`err.code()` to branch. Both examples live in [`examples/`](examples/).

## Interoperability

The [wire protocol](docs/wire-protocol.md) is documented in full. In short:

| Concern | Behaviour |
|---|---|
| Subjects | `<service>-events`, `<service>-events.sub.<method>`, `__broadcast.<method>`, `__nevo.discovery` |
| Codec | MessagePack maps with string keys; JSON alternatively; inbound codec sniffed, reply in kind |
| bigint | `BigInt` type or `#[serde(with = "nevo_messaging::wire::bigint::as_i64")]` ↔ `@@nevo:bigint:<digits>` |
| Strings starting with `@@nevo:` | escaped/unescaped exactly like the TypeScript codecs |
| Compression | `content-encoding` header; gzip, deflate (zlib), zstd (`zstd` feature) |
| Errors | numeric codes 0–20, `details.retryable`, `details.retryAfterMs` |
| Built-ins | `nevo.contract`, `nevo.health`, `nevo.live`, `nevo.ready` |
| Identity | `meta.service`, or a verified token when a verifier is configured |

[`docs/interop.md`](docs/interop.md) maps the TypeScript API onto this crate and lists the
behavioural differences and the features that are not ported yet (Kafka/HTTP/WebSocket
transports, outbox/inbox/saga/workflow, Postgres and Redis stores, DevTools).

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `nats` | yes | The NATS transport (`async-nats`) |
| `msgpack` | yes | MessagePack codec; without it JSON is the default |
| `compression` | yes | gzip and deflate via `flate2` |
| `zstd` | no | zstd compression |
| `metrics` | no | Emit `nevo_messaging_*` counters and histograms through the `metrics` facade |

## Configuration at a glance

```rust,ignore
Router::new("payments")
    .replay_window(Duration::from_secs(300))
    .rate_limit(RateLimitConfig::new(100.0, 50.0).key_by([KeyDimension::CallerService]))
    .access_control(AccessControl::new()
        .rule(AccessRule::new().topic("payments-events").method("pay.refund").allow(["coordinator"]))
        .allow_all_by_default(true))
    .idempotency(IdempotencyConfig::new().ttl(Duration::from_secs(600)))
    .idempotency_store(redis_store)            // any `IdempotencyStore`
    .dlq(DlqRouter::new().sink(InMemoryDlqStore::shared(1000)))
    .health(health_registry)
    .interceptor(AuditInterceptor)
    .compression(Compression::gzip());
```

Clients accept `RetryPolicy`, `CircuitBreakerConfig`, `HedgePolicy`, `IdempotencyConfig`,
`Compression`, `DiscoveryConfig` and per-call `CallOptions` (version, idempotency key,
headers, tenant, timeout, hedge, retry override).

## Testing

```bash
cargo test                                                   # unit tests, no broker needed
NEVO_TEST_NATS_URL=nats://127.0.0.1:4222 cargo test          # plus end-to-end tests
(cd tests/interop && npm install)                            # once, for the TypeScript peer
NEVO_TEST_NATS_URL=nats://127.0.0.1:4222 cargo test --test interop
```

A local broker: `docker run --rm -p 4222:4222 nats:2 -js`.

## License

MIT, see [LICENSE](./LICENSE).
