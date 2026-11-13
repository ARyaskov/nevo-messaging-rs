# Talking to TypeScript services

The test in `tests/interop.rs` runs the original `@riaskov/nevo-messaging`
package (through Node.js) against this crate in both directions: a TypeScript
`NevoNatsClient` calls a Rust `Router`, and a Rust `NatsClient` calls a
TypeScript signal router. It covers request/reply in both codecs, versioned
methods, error propagation, compression in both directions, `emit`, `publish`
→ `subscribe`, `broadcast`, `requestMany`, `nevo.contract`, bigint, `Date`,
`undefined` and escaped strings.

## Mapping the TypeScript API

| TypeScript | Rust |
|---|---|
| `createNevoNatsClient(["user"], { clientIdPrefix, servers, … })` | `NatsClient::builder().servers(…).service_name(…).services(["user"]).connect().await` |
| `this.query(service, method, params, opts)` | `client.query_with(service, method, &params, CallOptions::new()…).await` |
| `this.emit` / `this.publish` / `this.broadcast` | `client.emit` / `client.publish` / `client.broadcast` |
| `this.subscribe(service, method, { groupId, filter }, handler)` | `client.subscribe::<T>(service, method, SubscribeOptions::new()…)` returns a `Stream` |
| `client.requestMany` | `client.request_many` |
| `client.subscribeWildcard(pattern)` | `client.subscribe_wildcard::<T>(pattern)` |
| `client.subscribeQuery` | `client.query_stream` |
| `@NatsSignalRouter([UserService])` + `@Signal("user.getById", "getById", (d) => [d.id])` | `Router::new("user").route("user.getById", \|ctx, p: GetById\| async move { … })` |
| `@Signal("user.getById", …, { version: "v2" })` | `.route("user.getById@v2", …)` |
| `before` / `after` hooks | `Router::interceptor(...)` |
| `accessControl` | `Router::access_control(AccessControl::new().rule(…))` |
| `security.replayWindowMs` | `Router::replay_window(Duration)` |
| `rateLimit` / `@RateLimit` | `Router::rate_limit(RateLimitConfig)` |
| `idempotency`, `idempotencyStore` | `Router::idempotency(...)`, `Router::idempotency_store(Arc<dyn IdempotencyStore>)` |
| `HealthRegistry` | `health::HealthRegistry` |
| `DlqRouter` | `dlq::DlqRouter` |
| `getJetStreamHelper(nc)` | `client.jetstream()` |
| `discovery: { enabled: true }` | `.discovery(DiscoveryConfig::new())` |
| `runInChain` / `getCurrentChainId` | `context::scope` / `context::current_chain_id` |

## Behavioural notes

- **Caller identity.** Without a token verifier, `meta.service` is trusted, as
  in the TypeScript "trusted network" mode. With
  `AccessControl::verifier(...)`, identity comes from the verified token only.
- **Idempotency.** The server keys entries by caller, tenant, method and
  `meta.idempotencyKey` (falling back to the envelope uuid). A replay returns
  the stored response verbatim, including its original uuid. A reused key with
  a different payload answers `IDEMPOTENCY_KEY_CONFLICT`. The client also keeps
  a response cache keyed by service, method and idempotency key.
- **Queue groups.** The TypeScript router subscribes without a queue group, so
  every instance receives every request. `NatsServerBuilder::queue_group`
  opts into load balancing; leave it unset for parity.
- **Concurrency.** The TypeScript router processes messages one at a time;
  this server dispatches concurrently, bounded by
  `NatsServerBuilder::concurrency` (1024 by default).
- **Bad envelopes.** An undecodable request is answered with `PARSE_ERROR`
  rather than left to time out.
- **Params validation.** A `params` value that does not deserialise into the
  handler's type answers `VALIDATION_FAILED`, the code the TypeScript side uses
  for schema failures.
- **Error details in production.** When `NODE_ENV`, `NEVO_ENV` or `MODE` is
  `production`, error details are reduced to the public keys and did-you-mean
  hints are suppressed, matching the TypeScript behaviour. Override with
  `Router::expose_errors`.
- **Strings that look like bigints.** Both sides escape any string that starts
  with `@@nevo:` when serializing, so `"@@nevo:bigint:5"` travels as
  `"@@nevo:str:@@nevo:bigint:5"` and arrives as the same string. Only the typed
  `BigInt` path emits the unescaped `@@nevo:bigint:<digits>` sentinel, so the
  TypeScript peer receives a `bigint`. A `BigInt` wrapped inside a
  `serde_json::Value` (e.g. `json!({"id": BigInt::from(1)})`) does **not** use
  the typed path: serde_json erases the newtype, the value degrades to a
  `"@@nevo:str:@@nevo:bigint:7"` string, and the peer receives that string. This
  is an intentional breaking wire change: old-Rust senders relying on a literal
  sentinel string now get it back escaped (new-Rust to old-TS is safe, since
  `@@nevo:str:` is unescaped on the TS side too); a plain string can no longer
  be mistaken for an encoded bigint.

## Not ported (yet)

Kafka, HTTP/SSE, HTTP/2, WebSocket and Socket.IO transports; outbox, inbox,
saga, workflow, scheduler and event store; the Postgres/Redis/etcd stores;
audit log; adaptive concurrency and backpressure decorators; tenant policy
registry; DevTools bus and dashboard; OpenAPI/contract code generation. The
`Transport` trait and `Router::handle` are the seams for adding transports.
