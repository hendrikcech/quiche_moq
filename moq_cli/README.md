# moq_cli

A unified MoQ node binary (`moq`): publish, subscribe, relay, and route tracks
over MoQ transport, built on `quiche_moq`/`moq_core`.

## Usage

```shell
$ moq --listen 8080 --pub-passive "*" --sub-passive "*"
```

A node can simultaneously listen for incoming connections (`--listen`) and
connect out to others (`--connect`), and combine any number of active/passive
publish/subscribe roles:

| Flag | Role |
|------|------|
| `--pub <NS[--TRACK][#in=FILE]>` | Actively publish a track, or announce a namespace |
| `--pub-passive <NS--TRACK\|NS--*\|*>` | Serve a track on demand / auto-accept published namespaces |
| `--sub <NS--TRACK[#PARAMS]>` | Actively subscribe to a track |
| `--sub-passive <NS--TRACK\|NS--*\|*>` | Accept incoming publishes / relay-forward subscriptions upstream |
| `--relay-namespace <NS\|NS--*\|*>` | Re-announce namespaces to all peers (discovery hub) |

Exactly one `--connect` target can be marked `#upstream`; subscriptions for
namespaces this node doesn't know locally are forwarded there, and namespaces
it announces become available to local subscribers. This, together with
wildcard `*`/`NS--*` patterns on the passive flags, is what makes a node act
as a relay.

Run `moq --help` for the full flag reference, including per-connection QUIC
tuning (congestion window, flow control, stream limits), keepalive/PTO
control, delivery-timeout/priority SUBSCRIBE options, and TLS/qlog setup.

## Examples

```shell
# publisher: stream lines typed on stdin as chat--messages
$ moq --listen 8080 --pub chat--messages

# subscriber: connect and print the track to stdout
$ moq --connect https://127.0.0.1:8080 --sub chat--messages
```

## Nixnet experiment

From the repo root on Linux, run `nix run ./moq_cli/nixnet`. The experiment
generates test audio and video, sends them through a MoQ relay, and exits
after FFmpeg decodes both tracks. No display or media download is needed.

Run `nix run ./moq_cli/nixnet#noq` for the same experiment over the
experimental noq WebTransport backend. Outside nixnet, use `moq --backend
noq` with the same publish, subscribe, and relay flags. Clients verify TLS
against system roots unless `--noq-insecure` is set (used only for the
experiment's self-signed certificates). Quiche transport tuning, qlog, TLS
key logging, and QUIC ping flags are not available with noq.

To build and run with the sibling `../noq` fork instead of crates.io:

```sh
nix run ./moq_cli/nixnet#noq-local \
  --override-input moq/noq-src "git+file:$(realpath ../noq)"
```

This uses Git-tracked files, including local edits, without copying the
fork's `target/` directory or changing either checkout's default lockfile.
`nixnet/noq.Cargo.lock` records the fork's dependency graph for offline Nix
builds. If that graph changes, update it from a scratch copy of the workspace:
start from the root `Cargo.lock`, run
`cargo update -p noq --config 'patch.crates-io.noq.path="/absolute/path/to/noq/noq"'`,
and copy the resulting lockfile to `nixnet/noq.Cargo.lock`.
