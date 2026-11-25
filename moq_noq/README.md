# moq_noq (experimental)

This crate adapts an already-established `web_transport_trait::Session` to
`moq_core::Transport` and `moq_core_async::AsyncTransport`. It does not change
the existing quiche backends or perform TLS/ALPN/HTTP/3 negotiation itself.

```rust,ignore
use moq_noq::{ConnectParams, MoqSession, web_transport_noq};

// conn: an established noq::Connection, configured by the application.
let native = conn.clone();
let wt = web_transport_noq::Session::raw(conn);
// A native connection handle alone does not keep the WT session alive.
let (moq, driver) = MoqSession::connect(
    ConnectParams::new(wt.clone(), false),
    config, // MoQ configuration matching the negotiated protocol version
).await.unwrap();
tokio::spawn(driver);
// Use `native` or `wt` for noq-specific path management and statistics.
```

For WebTransport over HTTP/3, pass an established WT session instead of using
`Session::raw`. The caller chooses the MoQ configuration; the adapter does not
infer it from ALPN or the WT subprotocol. `WebTransport<S>` is generic over
native, Send-capable implementations of the async WT trait. Server applications
can use `WebTransport::new(ConnectParams::new(session, true))` and pass the
returned transport and notifier to `moq_core_async::AsyncMoqSession::accept`.
Dropping the adapter closes the session, including if another handle exists.

## Semantics and limitations

- Stream IDs are logical, not native QUIC IDs. Low two bits encode initiator
  and direction, as required by the current MoQ core.
- A successful open allocates a logical stream immediately; actual opening
  may wait for peer credit. Writes are accepted atomically into bounded queues
  (default 8 MiB per stream, 64 MiB per session), not necessarily QUIC buffers.
  Each send/receive half runs independently; priority/reset/stop are forwarded.
- Backpressure returns `Done` / `InsufficientCapacity`. This is **not** a
  drop-in solution for the current `OutStream::send_obj` atomic helper, which
  unwraps payload sends and assumes full acceptance. Use incremental publishing
  and retry bounded payload chunks; adapting that helper requires a separate
  core change. In particular, a write larger than the configured budget cannot
  succeed as a single call. Pending stream count and total receive bytes are not
  independently capped by this draft (each receive half holds at most one
  64 KiB chunk in addition to the native QUIC buffers).
- `flush_sends` checks transport failure and retires bookkeeping, but does not
  wait for all buffered bytes to reach QUIC or the peer.
- WT error codes are 32-bit. Unrepresentable codes are mapped to zero rather
  than truncated. Incremental scheduling and native qlog events are unavailable
  through the standard WT trait. STOP_SENDING error tombstones are bounded.
- This is a first implementation, not a production-validated adapter. In
  particular, stream-credit exhaustion, cancellation races and interoperability
  need actual transport tests before merging.

## Verification

From the workspace root:

```sh
cargo fmt -p moq_noq
cargo check -p moq_noq
cargo test -p moq_noq
```

The workspace lockfile must be regenerated and committed after resolving the new
crates. Add a noq loopback test with both client and server sides, partial writes,
FIN, RESET_STREAM, STOP_SENDING, connection loss, and tiny queue budgets before
considering the transport production-ready.
