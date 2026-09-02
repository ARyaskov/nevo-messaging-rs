# Wire protocol

Everything below is what the TypeScript implementation puts on the wire; this
crate produces and accepts exactly the same bytes.

## Envelopes

Requests (`query`, `emit`, `publish`, `broadcast`) are one map:

```json
{
  "uuid": "0192b0f1-2d3e-7a4b-9c8d-1e2f3a4b5c6d",
  "method": "user.getById@v1",
  "params": { "id": "@@nevo:bigint:42" },
  "meta": {
    "type": "query",
    "service": "coordinator",
    "instanceId": "…",
    "ts": 1756700000000,
    "version": "v1",
    "codec": "msgpack",
    "idempotencyKey": "…",
    "tenantId": "…",
    "headers": { "nevo-attempt": "1" },
    "nevoChainId": "…",
    "nevoParentUuid": "…",
    "auth": { "token": "…" },
    "trace": { "traceparent": "00-…" },
    "contentEncoding": "gzip"
  }
}
```

`uuid` is a UUIDv7. `method` always carries a version suffix; `v1` is the
default. `meta` keys this crate does not know are kept in `Meta::extra`.

Responses echo `uuid`, `method` and the inbound `meta`:

```json
{ "uuid": "…", "method": "user.getById@v1", "params": { "result": { … } }, "meta": { … } }
{ "uuid": "…", "method": "user.getById@v1", "params": { "result": "error", "error": { "code": 7, "message": "…", "details": { … }, "service": "user" } } }
```

A response is an error when `params.error` is present.

## Codecs

`Codec::MsgPack` (default) writes structs as maps with string keys, which is
what `@msgpack/msgpack` expects. `Codec::Json` is plain JSON. Inbound
envelopes are sniffed from their first byte, so a JSON caller talking to a
MessagePack service gets a JSON reply.

Shared value model:

| Rust value | Wire |
|---|---|
| `BigInt` (or `#[serde(with = "nevo_messaging::wire::bigint::as_i64")]`) | `"@@nevo:bigint:<digits>"` string; a JavaScript `bigint` on the other side |
| string starting with `@@nevo:` | `"@@nevo:str:<original>"`, restored on decode |
| `Option::None` | `null` (TypeScript drops `undefined` object members and turns array holes into `null`) |
| `serde_bytes` / `Vec<u8>` | MessagePack `bin`; a `Uint8Array` in TypeScript |

A well-formed `@@nevo:bigint:<digits>` string is never escaped, so a `BigInt`
embedded in a `serde_json::Value` (for example via `json!`) still arrives as a
bigint. A `serde_json::Value` received from a TypeScript `bigint` is the raw
sentinel string; parse it with `BigInt::parse`.

## Compression

Compressed payloads carry `content-encoding: gzip|deflate|zstd` as a transport
header (and in `meta.contentEncoding`). `deflate` is zlib-framed, matching
Node's `zlib.deflate`. Decompression is capped at `max_payload_bytes`
(4 MiB by default); a bomb is rejected with `PAYLOAD_TOO_LARGE` before it is
allocated.

## NATS subjects

| Operation | Subject |
|---|---|
| `query`, `emit` | `<service>-events` |
| `publish` | `<service>-events.sub.<method>` (version suffix stripped, `*`/`>`/whitespace replaced by `_`) |
| `subscribe` | same as publish; `*` and `>` pass through, an empty method means `<service>-events.sub.>` |
| `broadcast` | `__broadcast.<method>` |
| discovery heartbeats | `__nevo.discovery` (bare announcement, no envelope) |

Service names are lower-cased on both sides. Every entry of `meta.headers` is
also sent as a NATS header.

## Built-in signals

| Signal | Reply |
|---|---|
| `nevo.contract` | `{ protocol: "1", serviceName, serviceVersion?, instanceId, capabilities?, generatedAt, methods: [{ signalName, version, paramsSchema: null, resultSchema: null }] }` |
| `nevo.health`, `nevo.live`, `nevo.ready` | `{ status: "ok"|"degraded"|"down", service, instanceId?, version?, uptimeMs, ts, checks: { name: { status, message?, kind } } }` (only when a `HealthRegistry` is attached) |

## Error codes

| Code | Name | Retried by default |
|---|---|---|
| 0 | UNKNOWN | |
| 1 | UNAUTHORIZED | |
| 2 | TIMEOUT | yes |
| 3 | METHOD_NOT_FOUND | |
| 4 | SERVICE_NOT_FOUND | |
| 5 | SERVICE_UNAVAILABLE | yes |
| 6 | BAD_REQUEST | |
| 7 | VALIDATION_FAILED | |
| 8 | RATE_LIMITED | when `retryAfterMs` is present |
| 9 | CIRCUIT_OPEN | |
| 10 | PAYLOAD_TOO_LARGE | |
| 11 | PARSE_ERROR | |
| 12 | REPLAY_DETECTED | |
| 13 | IDEMPOTENT_REPLAY | |
| 14 | CONNECTION_LOST | yes |
| 15 | INTERNAL | |
| 16 | CANCELLED | |
| 17 | UNSUPPORTED_VERSION | |
| 18 | ACK_FAILED | |
| 19 | REMOTE_ERROR | |
| 20 | IDEMPOTENCY_KEY_CONFLICT | |

`details.retryable` and `details.retryAfterMs` travel with the error and
override the defaults on the receiving side. Unknown codes are preserved as
`ErrorCode::Other(n)`.
