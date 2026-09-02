# Adding a transport

The crate is split so that a broker binding only moves bytes:

```
Client<T: Transport>  ──encode/retry/breaker/idempotency──▶  T::request / send / publish / subscribe
Router::handle(Inbound) -> Outbound                          ◀── a pump per binding
```

## Client side

Implement [`transport::Transport`](../src/transport/mod.rs):

- `request(service, Outbound, timeout)` — request/reply against the service's
  request address; return the reply as an `Inbound` (payload plus headers).
- `send(service, Outbound)` — fire-and-forget to the same address.
- `publish(service, method, Outbound)` / `broadcast(method, Outbound)` — fan
  out to the method-scoped address and to the broadcast channel.
- `subscribe(service, method, queue_group)` — a stream of `Inbound`; dropping
  the stream must unsubscribe. `service == "__broadcast"` selects the
  broadcast channel.
- `publish_raw` / `subscribe_raw` — bare topics (discovery uses them).
- `flush` / `close`.

Map errors to `Error` with the right `ErrorCode`: timeouts to `Timeout`
(retryable), missing responders to `ServiceUnavailable`, connection failures
to `ConnectionLost`. `Client<T>` handles the rest: envelope construction,
compression, retry, circuit breaking, response caching, discovery heartbeats
and drain-on-close.

The `content-encoding` header must be carried in both directions. Transports
that cannot carry headers should refuse compression in their constructor.

## Server side

A binding subscribes to `Router::request_topic()`, wraps each delivery in an
`Inbound` and calls `Router::handle`, publishing the returned `Outbound` to
the reply address when there is one. See `transport/nats/server.rs` for the
NATS pump: it bounds concurrency with a semaphore, stops on a watch channel and
drains `Router::inflight()` before closing the connection.

`Router::handle` is fully transport-agnostic; it sniffs the codec, decompresses,
runs the pipeline and returns an encoded, optionally compressed response.
