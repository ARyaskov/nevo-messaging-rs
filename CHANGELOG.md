# Changelog

## 0.1.0

Initial release: a Rust 2024 implementation of the Nevo Messaging protocol,
wire-compatible with `@riaskov/nevo-messaging` 2.x.

- NATS transport: query, emit, publish/subscribe, broadcast, `request_many`,
  streaming replies, wildcard subscriptions, JetStream helpers.
- MessagePack and JSON codecs with per-message codec sniffing, bigint
  sentinels, wire-string escaping, gzip/deflate/zstd compression with
  decompression bombs rejected.
- `Router` request pipeline: replay guard, caller identity and token
  verification, rate limiting, ACL, `nevo.contract` / `nevo.health` /
  `nevo.live` / `nevo.ready` built-ins, claim-before-execute idempotency with
  pluggable distributed stores, interceptors, dead-letter routing, metrics.
- Client resilience: retry with full jitter and `retryAfterMs` hints,
  consecutive and sliding-window circuit breakers, hedging, idempotent replay,
  graceful drain.
- Chain-id propagation through task-local context, optional W3C trace
  context, heartbeat-based discovery.
