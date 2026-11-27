use super::*;
use crate::conn::{NodeTransport, NoqConn};
use log::debug;
use moq_core_async::AsyncTransport;
use moq_noq::web_transport_noq::noq::rustls::pki_types::PrivatePkcs8KeyDer;
use moq_noq::web_transport_noq::{ClientBuilder, Request, Server, ServerBuilder, Session, proto};
use moq_noq::{ConnectParams, NoqTransport};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;

type AnyError = Box<dyn std::error::Error>;

pub(super) fn run(args: Args) -> Result<(), AnyError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run_async(args))
}

async fn run_async(args: Args) -> Result<(), AnyError> {
    let cfg = make_moq_config(&args);
    // Validate before opening sockets or accepting peers.
    let _: moq_core::MoqTransportSession = moq_core::MoqTransportSession::accept(cfg.clone())
        .map_err(|e| format!("invalid MoQ configuration: {e:?}"))?;
    let protocol = match cfg.version_negotiation {
        moq::VersionNegotiation::Pinned(v) => {
            Some(v.alpn_protocol().ok_or("invalid pinned draft")?)
        }
        moq::VersionNegotiation::InBand { .. } => None,
    };
    let (mut ad, _, _) = build_app_data(&args);
    let mut server = if let Some(listen) = &args.listen {
        let (cert, key) = load_or_generate_keys(&args.cert, &args.key);
        let key = PrivatePkcs8KeyDer::from(key.private_key_to_der_pkcs8()?);
        let server = ServerBuilder::new()
            .with_addr(([0, 0, 0, 0], listen.port).into())
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
            if let Some(protocol) = &protocol {
                request = request.with_protocol(protocol.clone());
            }
            let cid = next_id;
            next_id += 1;
            let upstream = target.upstream;
            let client = client.clone();
            connects.spawn(async move { (cid, upstream, client.connect(request).await) });
        }
    }

    let mut conns = HashMap::new();
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut usr1 = signal(SignalKind::user_defined1())?;
    let mut usr2 = signal(SignalKind::user_defined2())?;
    // shortcut: poll nonblocking file I/O and MoQ together, switch to readiness for larger experiments.
    let mut ticker = tokio::time::interval(Duration::from_millis(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interrupt.recv() => break,
            _ = terminate.recv() => break,
            _ = usr1.recv() => ad.usr1_requested = true,
            _ = usr2.recv() => ad.usr2_requested = true,
            Some(result) = connects.join_next() => {
                let (cid, upstream, result) = result?;
                let session = result?;
                if session.protocol() != protocol.as_deref() {
                    return Err("peer did not negotiate the configured MoQ WebTransport protocol".into());
                }
                conns.insert(cid, make_connection(session, false, cfg.clone())?);
                if upstream {
                    ad.upstream_cids.insert(cid);
                    sync_sub_track_templates(&mut ad);
                }
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
                let now = Instant::now();
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
                    conn.session.handle(&mut conn.transport, &readable, moq_core::Timestamp::now());
                    if conn.session.is_closed() {
                        closed.push(cid);
                        continue;
                    }
                    if let Some(v) = conn.session.version() && !conn.state.logged_connect {
                        info!("conn {cid} noq WebTransport established v{v}");
                        conn.state.logged_connect = true;
                    }
                    let mut moq = MoqHandle { session: &mut conn.session, t: NodeTransport::Noq(&mut conn.transport) };
                    drive_conn_events(cid, &mut moq, &mut conn.state, &mut ad, conn.is_server);
                }
                for cid in closed {
                    conns.remove(&cid);
                    disconnect(cid, &mut ad, false);
                }
                let actions = take_signal_actions(&mut ad);
                drive_node(&mut Connections::Noq(&mut conns), &mut ad, &actions, now);
            }
        }
    }
    log_track_stats(&ad);
    for conn in conns.values_mut() {
        conn.transport.close_session(SessionCloseCode::NoError, "");
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
) -> Result<NoqConn, String> {
    let (mut transport, _) = NoqTransport::new(ConnectParams::new(session, is_server));
    let session = if is_server {
        moq_core::MoqTransportSession::accept(cfg)
    } else {
        moq_core::MoqTransportSession::connect(&mut transport, cfg)
    }
    .map_err(|e| format!("failed to initialize MoQ: {e:?}"))?;
    Ok(NoqConn {
        session,
        transport,
        state: ConnState::default(),
        is_server,
    })
}
