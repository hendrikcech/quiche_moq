use std::{sync::Arc, time::Duration};

use moq_core::{Config, Transport, transport::StreamError};
use moq_core_async::{AsyncMoqSession, AsyncTransport, Notify};
use moq_noq::{
    ConnectParams, MoqSession, NoqTransport,
    web_transport_noq::{
        Session,
        noq::{self, rustls},
    },
};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(10);

async fn raw_pair() -> (Session, Session) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).unwrap();

    // Writes larger than the receive window must make progress in several steps.
    let mut config = noq::TransportConfig::default();
    config.stream_receive_window(16_u32.into());
    let config = Arc::new(config);
    let mut server_config = noq::ServerConfig::with_single_cert(vec![cert], key.into()).unwrap();
    server_config.transport_config(config.clone());
    let mut client_config = noq::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    client_config.transport_config(config);

    let addr = "127.0.0.1:0".parse().unwrap();
    let server = noq::Endpoint::server(server_config, addr).unwrap();
    let client = noq::Endpoint::client(addr).unwrap();
    client.set_default_client_config(client_config);
    let connecting = client
        .connect(server.local_addr().unwrap(), "localhost")
        .unwrap();
    let (client, server) = timeout(TIMEOUT, async {
        tokio::join!(connecting, async { server.accept().await.unwrap().await })
    })
    .await
    .expect("raw QUIC handshake timed out");

    (Session::raw(client.unwrap()), Session::raw(server.unwrap()))
}

async fn loopback() -> ((NoqTransport, Notify), (NoqTransport, Notify)) {
    let (client, server) = raw_pair().await;
    let client = ConnectParams::new(client, false);
    let mut server = ConnectParams::new(server, true);
    server.max_send_bytes_per_stream = 32;
    server.max_send_bytes_total = 48;
    (
        NoqTransport::connect(client).await.unwrap(),
        NoqTransport::new(server),
    )
}

async fn receive_to_fin(transport: &mut NoqTransport, wake: &Notify, id: u64, expected: &[u8]) {
    timeout(TIMEOUT, async {
        let mut received = Vec::new();
        let mut buf = [0; 7];
        loop {
            transport.flush_sends().await.unwrap();
            if !transport.readable_stream_ids().contains(&id) {
                wake.notified().await;
                continue;
            }
            match transport.recv_stream(id, &mut buf) {
                Ok(n) => {
                    assert!(n > 0);
                    received.extend_from_slice(&buf[..n]);
                    assert!(received.len() <= expected.len());
                }
                Err(StreamError::Fin) => {
                    assert_eq!(received, expected);
                    assert!(!transport.readable_stream_ids().contains(&id));
                    return;
                }
                other => panic!("unexpected read from stream {id}: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for data and FIN on stream {id}"));
}

#[tokio::test]
async fn client_bidi_round_trip_and_fin() {
    let ((mut client, client_wake), (mut server, server_wake)) = loopback().await;
    let id = client.open_bidi_stream().unwrap();
    assert_eq!(id, 0);
    assert_eq!(client.send_stream(id, b"request", true).unwrap(), 7);
    receive_to_fin(&mut server, &server_wake, id, b"request").await;

    assert_eq!(server.send_stream(id, b"response", false).unwrap(), 8);
    assert_eq!(server.send_stream(id, b"", true).unwrap(), 0);
    receive_to_fin(&mut client, &client_wake, id, b"response").await;
}

#[tokio::test(flavor = "current_thread")]
async fn server_queue_backpressure_recovers_without_losing_data_or_fin() {
    let ((mut client, client_wake), (mut server, server_wake)) = loopback().await;
    let uni = server.open_uni_stream().unwrap();
    let bidi = server.open_bidi_stream().unwrap();
    assert_eq!((uni, bidi), (3, 1));
    let first = b"0123456789abcdefghijklmnopqrstuv";
    let last = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^";

    // With no await, the current-thread runtime cannot drain either queue yet.
    assert_eq!(server.send_stream(uni, first, false).unwrap(), first.len());
    assert!(matches!(
        server.send_stream_if_capacity(uni, b"x", true),
        Err(StreamError::InsufficientCapacity)
    ));
    assert!(matches!(
        server.send_stream(bidi, b"17 bytes rejected", true),
        Err(StreamError::Done)
    ));
    assert_eq!(
        server.send_stream(bidi, b"16 bytes queued!", true).unwrap(),
        16
    );

    let expected = [first.as_slice(), last.as_slice()].concat();
    tokio::join!(
        receive_to_fin(&mut client, &client_wake, uni, &expected),
        async {
            timeout(TIMEOUT, async {
                loop {
                    server.flush_sends().await.unwrap();
                    match server.send_stream_if_capacity(uni, last, true) {
                        Ok(n) => {
                            assert_eq!(n, last.len());
                            return;
                        }
                        Err(StreamError::InsufficientCapacity) => server_wake.notified().await,
                        other => panic!("unexpected retry result: {other:?}"),
                    }
                }
            })
            .await
            .expect("send queue did not regain capacity");
        }
    );
    receive_to_fin(&mut client, &client_wake, bidi, b"16 bytes queued!").await;
}

#[tokio::test]
async fn moq_setup_and_clean_peer_close() {
    let (client, server) = raw_pair().await;
    let client_conn = (*client).clone();
    let config = Config::default();
    let version = config.version_negotiation.preferred();
    let (server, wake) = NoqTransport::new(ConnectParams::new(server, true));
    let mut server = AsyncMoqSession::accept(server, config.clone(), wake).unwrap();
    assert!(!server.session().initialized());

    let (client, ()) = timeout(TIMEOUT, async {
        tokio::join!(
            MoqSession::connect(ConnectParams::new(client, false), config),
            async {
                while !server.session().initialized() {
                    server.run_once().await.unwrap();
                }
            }
        )
    })
    .await
    .expect("MoQ setup timed out");
    let (_client, driver) = client.unwrap();
    assert_eq!(server.session().version(), Some(version));

    server.transport_mut().close_session(0, "setup complete");
    let (close, ()) = timeout(TIMEOUT, async {
        tokio::join!(client_conn.closed(), driver)
    })
    .await
    .expect("client did not observe peer close and exit");
    let noq::ConnectionError::ApplicationClosed(close) = close else {
        panic!("expected clean application close, got {close:?}");
    };
    assert_eq!(close.error_code.into_inner(), 0);
    assert_eq!(close.reason.as_ref(), b"setup complete");
}
