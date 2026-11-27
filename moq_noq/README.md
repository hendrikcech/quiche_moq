# moq_noq (experimental)

This crate adapts an already-established `web_transport_trait::Session` to
`moq_core::Transport` and `moq_core_async::AsyncTransport`. It does not change
the existing quiche backends or perform TLS/ALPN/HTTP/3 negotiation itself.
Construct and run the adapter inside a Tokio runtime.

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
infer it from ALPN or the WT subprotocol. `Config::default()` negotiates MoQT
drafts 7–14 in-band. For MoQT drafts 15–19, the caller must negotiate the
`moqt-N` subprotocol (or native QUIC ALPN) and configure that version on both
ends. `WebTransport<S>` is generic over
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
  connection failure, which returns `Failed`. Use incremental publishing and
  retry bounded payload chunks; a write larger than the configured budget
  cannot succeed as a single call. Both budgets must also fit each encoded MoQ
  control message, which the core initially submits as one write.
- Pending stream count and total receive bytes are not independently capped
  (each receive half holds at most one 64 KiB chunk in addition to the native
  QUIC buffers).
- `flush_sends` checks transport failure but does not wait for buffered bytes
  to reach QUIC or the peer. On noq, `stream_finished` uses acknowledgment,
  not the act of queueing FIN. Other `Session` implementations can only
  provide the guarantees of their `SendStream::closed`.
- WT error codes are 32-bit. Unrepresentable codes are mapped to zero rather
  than truncated. Incremental scheduling and native qlog events are unavailable
  through the standard WT trait. STOP_SENDING error tombstones are bounded.
  This adapter does not expose datagrams to MoQ.
- The locked `web-transport-noq` 0.5.0 / `web-transport-proto` 0.7.0 pair uses
  legacy HTTP/3 WebTransport (`:protocol=webtransport`), not WebTransport
  draft 16 (`webtransport-h3`). It does not advertise draft 16 or accept a
  peer advertising only draft 16. Interop with quiche requires its legacy
  fallback. This is independent of the MoQT version: MoQT draft 19 can run
  over that legacy HTTP/3 WebTransport.
- This is a first implementation, not a production-validated adapter. In
  particular, stream-credit exhaustion, cancellation races and interoperability
  need actual transport tests before merging.

## Verification

From the workspace root:

```sh
cargo fmt -p moq_noq -- --check
cargo check --locked -p moq_noq
cargo test --locked -p moq_noq
```

The loopback tests cover raw QUIC streams, flow control, bounded send queues,
FIN acknowledgment, STOP_SENDING, MoQT draft 14 setup and peer close, and
MoQT draft 19 setup and SUBSCRIBE over noq-to-noq HTTP/3 WebTransport.
A settings test checks the locked stack's draft-16 limitation. Stream-credit
exhaustion and broader cancellation races still need coverage.
