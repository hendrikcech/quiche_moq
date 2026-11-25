# MoQ-aware scheduling in noq

Start with **subgroup-stream priority**, then add object metadata only if it
improves playback. MinRTT and BLEST are transport baselines: neither knows
which bytes belong to a keyframe or whether an object is still worth delivering.
The scheduler that matters for subscriber media runs on the **relay**, the
sender on that leg. Leave the publisher–relay leg unchanged in a first test.

## What is available now

- The relay already applies the subscriber's priority to forwarded tracks and
  calls `send_obj_hdr_with` with location, length, extensions, and the
  local receive time in [`moq_cli/src/node.rs`](../../moq_cli/src/node.rs).
  [`MoqTransportSession::send_obj_hdr_with`](../../moq_core/src/session.rs)
  opens and prioritizes a unidirectional stream when the group or subgroup
  changes. A stream can carry several objects; an older stream can still have
  queued bytes. The separate `send_obj` helper does not set priority on its
  first open, so use the header/payload path for comparisons.
- The [`moq_noq` adapter](../src/lib.rs) allocates synthetic stream IDs, queues
  bytes, and applies the negated MoQ urgency to the native send stream before
  writing. The incremental flag is not propagated. The priority is visible as
  `StreamView.priority` in the fork, but stock MinRTT ranks paths by RTT and
  path ID, not streams by MoQ priority.
- In `../noq/noq-proto/src/connection/scheduler/conn_state.rs`, `ConnState`
  exposes native stream IDs, readiness, offsets, buffered/in-flight/lost
  ranges, FIN, and path data such as validation, RTT, congestion window and
  bytes in flight. It does not expose MoQ identity or media semantics, and
  no callback runs per MoQ object or per frame. `Scheduler::schedule` gets
  coalesced state updates; its levels rank eligible paths globally, and the
  token drain can put several STREAM frames into a packet. Native validation,
  pacing and congestion admission still apply.

## Which metadata to expose

1. **Stream role and subgroup identity.** Distinguish MoQ control/requests
   from media; do not infer role from the QUIC stream direction. A native
   connection also carries HTTP/3 setup streams. MoQ control is STREAM data,
   not the fork's `prepared_control()` (QUIC control frames). For media,
   start with effective urgency plus track/group/subgroup, scoped to a
   session. Mark the role at the core stream-open sites in `session.rs` and
   [`control_channel.rs`](../../moq_core/src/control_channel.rs). An optional
   local transport hook can pass the role to the adapter; bind the synthetic
   ID to the actual `web_transport_noq::SendStream::quic_id()` when the
   async open completes. The generic WebTransport send trait lacks this
   accessor, so use the concrete noq adapter seam, not stream-open order.
2. **Object ranges and delivery value.** If useful, add object ID, byte range,
   size, dependency hints (for example keyframe vs. dependent frame, when
   known), and an optional estimated deadline or remaining delivery budget.
   Group starts alone are not reliable codec dependency metadata. The relay
   can read preserved extension headers, but cannot invent missing media or
   playout metadata. `received_at` is local and currently feeds an age check;
   it is not the subscriber's render deadline. Keep absolute capture time,
   local cache age and any estimated delivery deadline distinct.
3. **Carry metadata out of band.** Register object metadata alongside
   successful writes in [`OutStream`](../../moq_core/src/out_stream.rs)
   rather than parsing bytes inside the scheduler. Translate logical byte
   ranges to QUIC offsets using bytes actually accepted by each native write
   (including partial writes and the WebTransport stream prefix). Retain
   mappings for retransmission until ACK/reset retires them, not just until
   MoQ queues FIN. A connection-local lookup keyed by native `StreamId` and
   offset gives the scheduler these hints without new wire fields. Sending a
   later byte range first cannot bypass earlier missing bytes for the ordered
   reader on the same stream.

## First experiment

Compare stock MinRTT with a test-only variant that retains its QUIC control
routing and persistent `SendQueue` tokens but separates two known media
priorities into different token levels. Start with one path to isolate stream
selection; decide whether urgency or RTT rank wins when testing two. Make
both tracks natively ready before measuring order: simultaneously enqueuing
them in the adapter is not enough. Assert the higher-urgency stream gets the
first data opportunity, then drain and verify both decoded tracks and FINs.

Next run on both nixnet links and measure per-path media bytes, time to first
decodable group, late objects and low-priority starvation. Add a link outage
to distinguish failover from mere path validation. Token ordering alone is
not deadline enforcement; do not drop a range inside a reliable stream to
skip stale media.
