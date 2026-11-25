use super::*;
use moq_core_async::AsyncTransport;
use moq_noq::web_transport_noq::noq::rustls::pki_types::PrivatePkcs8KeyDer;
use moq_noq::web_transport_noq::{ClientBuilder, Request, Server, ServerBuilder, Session, proto};
use moq_noq::{ConnectParams, NoqTransport};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;

type AnyError = Box<dyn std::error::Error>;

pub(super) fn run(args: Args) -> Result<(), AnyError> {
    if args.keepalive != 0
        || args.keepalive_no_pto_limit
        || args.timeout != 30_000
        || args.usr1_ping
        || args.qlog_dir.is_some()
        || args.ssl_key_log_file.is_some()
        || args.listen_init_cwnd().is_some()
        || args.connect_init_cwnd().is_some()
        || args.listen_max_data().is_some()
        || args.connect_max_data().is_some()
        || args.listen_max_stream_data().is_some()
        || args.connect_max_stream_data().is_some()
        || args.listen_max_streams_uni().is_some()
        || args.connect_max_streams_uni().is_some()
        || args.listen_max_streams_bidi().is_some()
        || args.connect_max_streams_bidi().is_some()
    {
        return Err("--backend noq does not support quiche transport tuning, keylog, qlog or QUIC ping flags".into());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run_async(args))
}

async fn run_async(args: Args) -> Result<(), AnyError> {
    let cfg = make_moq_config(&args);
    // Validate before opening sockets or accepting peers.
    MoqTransportSession::accept(cfg.clone())
        .map_err(|e| format!("invalid MoQ configuration: {e:?}"))?;
    let protocol = match cfg.version_negotiation {
        moq::VersionNegotiation::Pinned(v) => {
            Some(moq_wire::alpn_protocol_string(v).ok_or("invalid pinned draft")?)
        }
        moq::VersionNegotiation::InBand { .. } => None,
    };
    let (mut ad, _, _) = build_app_data(&args);
    let mut server = if let Some(port) = args.listen {
        let (cert, key) = load_or_generate_keys(&args.cert, &args.key);
        let key = PrivatePkcs8KeyDer::from(key.private_key_to_der_pkcs8()?);
        let server = ServerBuilder::new()
            .with_addr(([0, 0, 0, 0], port).into())
            .with_certificate(vec![cert.to_der()?.into()], key.into())?;
        info!("listening on {}", server.local_addr()?);
        Some(server)
    } else {
        None
    };
    let mut connects = JoinSet::new();
    let mut next_id = 0;
    if !args.connect.is_empty() {
        let builder = ClientBuilder::new();
        let client = if args.noq_insecure {
            builder.dangerous().with_no_certificate_verification()?
        } else {
            builder.with_system_roots()?
        };
        for target in &args.connect {
            let mut request = proto::ConnectRequest::new(Url::parse(&target.url)?);
            if request.url.scheme() != "https" {
                return Err("--backend noq requires an https:// WebTransport URL".into());
            }
            if let Some(protocol) = &protocol {
                request = request.with_protocol(protocol.clone());
            }
            let cid = next_id;
            next_id += 1;
            register_outbound(&mut ad, cid, target);
            let client = client.clone();
            connects.spawn(async move { (cid, client.connect(request).await) });
        }
    }

    let mut conns = Connections::<MoqTransportSession, NoqTransport, ConnState>::new();
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut usr1 = signal(SignalKind::user_defined1())?;
    let mut usr1_requested = false;
    // shortcut: poll nonblocking file I/O and MoQ together, switch to readiness for larger experiments.
    let mut ticker = tokio::time::interval(Duration::from_millis(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interrupt.recv() => break,
            _ = terminate.recv() => break,
            _ = usr1.recv() => usr1_requested = true,
            Some(result) = connects.join_next() => {
                let (cid, result) = result?;
                let session = result?;
                if session.protocol() != protocol.as_deref() {
                    return Err("peer did not negotiate the configured MoQ WebTransport protocol".into());
                }
                conns.insert(cid, make_connection(session, false, cfg.clone())?);
            }
            Some(request) = accept(&mut server) => {
                match tokio::time::timeout(Duration::from_secs(1), respond(request, protocol.as_deref())).await {
                    Ok(Ok(Some(session))) => {
                        conns.insert(next_id, make_connection(session, true, cfg.clone())?);
                        next_id += 1;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(error)) => debug!("WebTransport handshake failed: {error}"),
                    Err(_) => debug!("WebTransport response timed out"),
                }
            }
            _ = ticker.tick() => {
                for input in &mut ad.file_inputs {
                    if let Some(local) = ad.local_tracks.get_mut(&input.track) {
                        input.drain_into(local, ad.embed_timestamps);
                    }
                }
                for track in ad.local_tracks.values_mut() {
                    for out in &mut track.file_outputs {
                        out.drain(&track.cache);
                    }
                }
                let mut closed = Vec::new();
                for (&cid, conn) in &mut conns {
                    if conn.transport.flush_sends().await.is_err() {
                        closed.push(cid);
                        continue;
                    }
                    let readable = conn.transport.readable_stream_ids();
                    conn.session.handle(&mut conn.transport, &readable);
                    if let Some(v) = conn.session.version() && !conn.state.logged_connect {
                        info!("conn {cid} noq WebTransport established v{}", version_to_name(v));
                        conn.state.logged_connect = true;
                    }
                }
                for cid in closed {
                    conns.remove(&cid);
                    disconnect(&mut ad, cid);
                }
                let toggle = usr1_requested && args.usr1_toggle_padding_stream;
                let unpub = if usr1_requested { args.usr1_unpub.as_slice() } else { &[] };
                usr1_requested = false;
                tick(&mut conns, &mut ad, toggle, unpub);
            }
        }
    }
    for conn in conns.values_mut() {
        conn.transport.close_session(moq_wire::NO_ERROR.into(), "");
    }
    Ok(())
}

async fn accept(server: &mut Option<Server>) -> Option<Request> {
    match server {
        Some(server) => server.accept().await,
        None => std::future::pending().await,
    }
}

async fn respond(
    request: Request,
    protocol: Option<&str>,
) -> Result<Option<Session>, moq_noq::web_transport_noq::ServerError> {
    let mut response = proto::ConnectResponse::default();
    if let Some(protocol) = protocol {
        if !request.protocols.iter().any(|offered| offered == protocol) {
            request
                .reject(moq_noq::web_transport_noq::http::StatusCode::BAD_REQUEST)
                .await?;
            return Ok(None);
        }
        response = response.with_protocol(protocol);
    }
    request.respond(response).await.map(Some)
}

fn make_connection(
    session: Session,
    is_server: bool,
    cfg: moq::Config,
) -> Result<Connection<MoqTransportSession, NoqTransport, ConnState>, String> {
    let (mut transport, _) = NoqTransport::new(ConnectParams::new(session, is_server));
    let session = if is_server {
        MoqTransportSession::accept(cfg)
    } else {
        MoqTransportSession::connect(&mut transport, cfg)
    }
    .map_err(|e| format!("failed to initialize MoQ: {e:?}"))?;
    Ok(Connection {
        session,
        transport,
        state: ConnState::default(),
        is_server,
    })
}
