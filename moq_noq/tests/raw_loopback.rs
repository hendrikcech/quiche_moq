use std::{sync::Arc, time::Duration};

use moq_core::{
    Config, Event, IncomingRequest, StreamError, SubscribeOk, Transport, VersionNegotiation,
    wire::{MOQ_VERSION_DRAFT_14, MOQ_VERSION_DRAFT_19, ResetCode, SessionCloseCode, Version},
};
use moq_core_async::{AsyncMoqSession, AsyncTransport, Notify};
use moq_noq::{
    ConnectParams, MoqSession, NoqTransport,
    web_transport_noq::{
        ClientBuilder, ServerBuilder, Session,
        noq::{self, rustls},
        proto::{ConnectRequest, ConnectResponse, Setting, Settings, VarInt},
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
    assert!(!client.stream_finished(id));
    assert_eq!(client.send_stream(id, b"request", true).unwrap(), 7);
    assert!(!client.stream_finished(id));
    receive_to_fin(&mut server, &server_wake, id, b"request").await;
    timeout(TIMEOUT, async {
        // The stream still has a receive half, so this must observe the FIN ACK, not retirement.
        while !client.stream_finished(id) {
            client.flush_sends().await.unwrap();
            client_wake.notified().await;
        }
    })
    .await
    .expect("client did not report the FIN acknowledgement");

    assert_eq!(server.send_stream(id, b"response", false).unwrap(), 8);
    assert_eq!(server.send_stream(id, b"", true).unwrap(), 0);
    receive_to_fin(&mut client, &client_wake, id, b"response").await;
}

#[tokio::test]
async fn peer_stop_is_visible_without_another_write() {
    let ((mut client, client_wake), (mut server, server_wake)) = loopback().await;
    let id = client.open_bidi_stream().unwrap();
    client.send_stream(id, b"discard me", false).unwrap();
    timeout(TIMEOUT, async {
        while !server.readable_stream_ids().contains(&id) {
            server.flush_sends().await.unwrap();
            server_wake.notified().await;
        }
        assert!(server.stop_sending(id, ResetCode::Canceled));
        assert!(!server.readable_stream_ids().contains(&id));
        while !client.stream_stopped(id) {
            client.flush_sends().await.unwrap();
            client_wake.notified().await;
        }
    })
    .await
    .expect("client did not report STOP_SENDING");
    assert!(!client.stream_finished(id));
    assert!(matches!(
        client.send_stream(id, b"too late", false),
        Err(StreamError::StreamStopped(code)) if code == u64::from(ResetCode::Canceled)
    ));
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

async fn establish_moq(
    client: Session,
    server: Session,
    config: Config,
    version: Version,
) -> (
    AsyncMoqSession<NoqTransport>,
    MoqSession,
    impl Future<Output = ()>,
) {
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
    let (client, driver) = client.unwrap();
    assert_eq!(server.session().version(), Some(version));
    (server, client, driver)
}

#[tokio::test]
async fn moq_setup_and_clean_peer_close() {
    let (client, server) = raw_pair().await;
    let client_conn = (*client).clone();
    let (mut server, _client, driver) =
        establish_moq(client, server, Config::default(), MOQ_VERSION_DRAFT_14).await;
    server
        .transport_mut()
        .close_session(SessionCloseCode::NoError, "setup complete");
    assert!(matches!(
        server.transport_mut().open_bidi_stream(),
        Err(StreamError::Failed)
    ));
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

#[tokio::test]
async fn webtransport_negotiates_moq_draft_19_over_legacy_http3() {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der());
    let mut server = ServerBuilder::new()
        .with_addr("127.0.0.1:0".parse().unwrap())
        .with_certificate(vec![cert.clone()], key.into())
        .unwrap();
    let client = ClientBuilder::new()
        .with_server_certificates(vec![cert])
        .unwrap();
    let protocol = MOQ_VERSION_DRAFT_19.alpn_protocol().unwrap();
    let request = ConnectRequest::new(
        format!("https://{}", server.local_addr().unwrap())
            .parse::<url::Url>()
            .unwrap(),
    )
    .with_protocol(&protocol);
    let (client, server) = timeout(TIMEOUT, async {
        tokio::join!(client.connect(request), async {
            let request = server.accept().await.unwrap();
            assert_eq!(request.protocols, [protocol.as_str()]);
            request
                .respond(ConnectResponse::default().with_protocol(&protocol))
                .await
                .unwrap()
        })
    })
    .await
    .expect("WebTransport handshake timed out");
    let client = client.unwrap();
    assert_eq!(client.protocol(), Some(protocol.as_str()));
    let (mut server, client, driver) = establish_moq(
        client,
        server,
        Config {
            version_negotiation: VersionNegotiation::only(MOQ_VERSION_DRAFT_19),
            ..Config::default()
        },
        MOQ_VERSION_DRAFT_19,
    )
    .await;

    timeout(TIMEOUT, async {
        tokio::select! {
            _ = driver => panic!("MoQ driver ended before SUBSCRIBE_OK"),
            (subscription, _publication) = async {
                tokio::join!(client.subscribe("test--track"), async {
                    loop {
                        server.run_once().await.unwrap();
                        while let Some(event) = server.next_event() {
                            if let Event::Request(IncomingRequest::Subscribe(request)) = event {
                                let (session, transport) = server.parts_mut();
                                return session.accept_request(request, SubscribeOk::default(), transport).unwrap();
                            }
                        }
                    }
                })
            } => { subscription.unwrap(); }
        }
    })
    .await
    .expect("MoQT draft 19 SUBSCRIBE timed out");
}

#[test]
fn locked_webtransport_stack_requires_legacy_settings() {
    // draft-ietf-webtrans-http3-16's SETTINGS_WT_ENABLED, not a MoQT version.
    let wt_enabled_16 = Setting(VarInt::from_u32(0x2c7cf000));
    let enabled = VarInt::from_u32(1);
    let mut settings = Settings::default();
    settings.insert(Setting::ENABLE_CONNECT_PROTOCOL, enabled);
    settings.insert(Setting::ENABLE_DATAGRAM, enabled);
    settings.insert(wt_enabled_16, enabled);
    assert_eq!(settings.supports_webtransport(), 0);

    settings.enable_webtransport(1);
    assert_eq!(settings.supports_webtransport(), 1);
    let mut advertised = Settings::default();
    advertised.enable_webtransport(1);
    assert!(!advertised.contains_key(&wt_enabled_16));
}
