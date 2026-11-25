use crate::args::{Args, MultipathArgs, Scheduler};
use moq_noq::web_transport_noq::{ALPN, Server, Session, crypto, noq, proto::ConnectRequest};
use noq::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use noq_proto::scheduler::{BlestScheduler, MinRttScheduler};
use std::{
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::Duration,
};

pub(super) fn validate(args: &Args) -> Result<(), &'static str> {
    let options = &args.mpquic;
    if !options.multipath {
        return Ok(());
    }
    if options.scheduler.is_none() {
        return Err("--multipath requires --scheduler");
    }
    if args.listen.is_none() && args.connect.is_empty() {
        return Err("--multipath requires --listen or --connect");
    }
    if options.bind.is_some() && args.connect.len() != 1 {
        return Err("--bind requires exactly one --connect target");
    }
    if let Some(path) = options.additional_path {
        let bind = options.bind.ok_or("--additional-path requires --bind")?;
        if bind.ip().is_unspecified()
            || bind.ip() == path.local.ip()
            || bind.is_ipv4() != path.local.is_ipv4()
        {
            return Err(
                "--bind and the additional LOCAL must use distinct concrete IPs of the same family",
            );
        }
    }
    Ok(())
}

fn transport() -> Arc<noq::TransportConfig> {
    let mut config = noq::TransportConfig::default();
    config.max_concurrent_multipath_paths(2);
    Arc::new(config)
}

pub(super) fn listen(
    addr: SocketAddr,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Server, super::AnyError> {
    let mut tls = rustls::ServerConfig::builder_with_provider(crypto::default_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(chain, key)?;
    tls.alpn_protocols = vec![ALPN.as_bytes().to_vec()];
    let mut config = noq::ServerConfig::with_crypto(Arc::new(
        noq::crypto::rustls::QuicServerConfig::try_from(tls)?,
    ));
    config.transport_config(transport());
    Ok(Server::new(noq::Endpoint::server(config, addr)?))
}

pub(super) async fn connect(
    request: ConnectRequest,
    options: MultipathArgs,
    insecure: bool,
) -> Result<Session, Box<dyn std::error::Error + Send + Sync>> {
    let host = request
        .url
        .host_str()
        .ok_or("WebTransport URL requires a host")?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let port = request.url.port().unwrap_or(443);
    let remote = tokio::net::lookup_host((host, port))
        .await?
        .find(|remote| {
            options
                .bind
                .is_none_or(|bind| bind.is_ipv4() == remote.is_ipv4())
        })
        .ok_or("no relay address matching the outbound socket's IP family")?;
    let bind = options.bind.unwrap_or_else(|| {
        if remote.is_ipv4() {
            ([0; 4], 0).into()
        } else {
            ([0; 8], 0).into()
        }
    });
    let endpoint = noq::Endpoint::client(bind)?;
    let additional = if let Some(path) = options.additional_path {
        let socket = UdpSocket::bind(path.local)?;
        let local = socket.local_addr()?;
        endpoint.add_socket(socket)?;
        Some((local, path.remote))
    } else {
        None
    };
    let config = client_config(insecure)?;
    let conn = endpoint.connect_with(config, remote, host)?.await?;
    set_scheduler(
        &conn,
        options
            .scheduler
            .ok_or("--multipath requires --scheduler")?,
    );
    if let Some((local, remote)) = additional {
        if !conn.is_multipath_enabled() {
            return Err("peer did not negotiate MPQUIC; cannot open the requested path".into());
        }
        let tuple = noq::FourTuple::new(remote, Some(local.ip()));
        let path = tokio::time::timeout(
            Duration::from_secs(5),
            conn.open_path(tuple, noq::PathStatus::Available),
        )
        .await
        .map_err(|_| format!("timed out validating MPQUIC path {local} -> {remote}"))?
        .map_err(|error| format!("could not open MPQUIC path {local} -> {remote}: {error}"))?;
        log::info!("MPQUIC path {} established: {local} -> {remote}", path.id());
    }
    Ok(Session::connect(conn, request).await?)
}

pub(super) fn set_scheduler(conn: &noq::Connection, scheduler: Scheduler) {
    if !conn.is_multipath_enabled() {
        return;
    }
    conn.set_scheduler(match scheduler {
        Scheduler::MinRtt => Box::new(MinRttScheduler::new()),
        Scheduler::Blest => Box::new(BlestScheduler::new()),
    });
    use clap::ValueEnum;
    log::info!(
        "MPQUIC scheduler: {}",
        scheduler.to_possible_value().unwrap().get_name()
    );
}

fn client_config(
    insecure: bool,
) -> Result<noq::ClientConfig, Box<dyn std::error::Error + Send + Sync>> {
    let provider = crypto::default_provider();
    let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> = if insecure {
        Arc::new(NoCertificateVerification(provider.clone()))
    } else {
        Arc::new(rustls_platform_verifier::Verifier::new(provider.clone())?)
    };
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.as_bytes().to_vec()];
    let crypto = noq::crypto::rustls::QuicClientConfig::try_from(tls)?;
    let mut config = noq::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport());
    Ok(config)
}

// The stock WebTransport builder cannot apply a transport config. Match its
// --noq-insecure behavior: skip trust checks but still verify TLS signatures.
#[derive(Debug)]
struct NoCertificateVerification(crypto::Provider);

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("MPQUIC requires TLS 1.3".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn path_flags_require_a_scheduler_and_distinct_source_ips() {
        assert!(Args::try_parse_from(["moq", "--multipath", "--listen", "443"]).is_err());
        let args = Args::try_parse_from([
            "moq",
            "--backend",
            "noq",
            "--multipath",
            "--scheduler",
            "min-rtt",
            "--connect",
            "https://127.0.0.1:443",
            "--bind",
            "127.0.0.1:0",
            "--additional-path",
            "127.0.0.1:0=127.0.0.2:443",
        ])
        .unwrap();
        assert!(validate(&args).is_err());
        assert!(
            "0.0.0.0:0=127.0.0.2:443"
                .parse::<crate::args::AdditionalPath>()
                .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn webtransport_establishes_two_paths_and_installs_schedulers() {
        use moq_noq::web_transport_noq::proto::ConnectResponse;

        let (cert, key) = crate::node::load_or_generate_keys(&None, &None);
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.private_key_to_der_pkcs8().unwrap());
        let mut server = listen(
            "0.0.0.0:0".parse().unwrap(),
            vec![cert.to_der().unwrap().into()],
            key.into(),
        )
        .unwrap();
        let port = server.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let request = server.accept().await.unwrap();
            set_scheduler(request.conn(), Scheduler::Blest);
            request.respond(ConnectResponse::default()).await.unwrap()
        });
        let remote = ([127, 0, 0, 2], port).into();
        let options = MultipathArgs {
            multipath: true,
            scheduler: Some(Scheduler::MinRtt),
            bind: Some("127.0.0.1:0".parse().unwrap()),
            additional_path: Some(crate::args::AdditionalPath {
                local: "127.0.0.2:0".parse().unwrap(),
                remote,
            }),
        };
        let request =
            ConnectRequest::new(url::Url::parse(&format!("https://127.0.0.1:{port}")).unwrap());
        let (client, server) = tokio::time::timeout(Duration::from_secs(10), async {
            let client = connect(request, options, true).await.unwrap();
            (client, server.await.unwrap())
        })
        .await
        .unwrap();
        assert!(client.has_scheduler() && server.has_scheduler());
        assert_eq!(
            client.path(1u32.into()).unwrap().remote_address().unwrap(),
            remote
        );
        assert_eq!(
            server
                .path(1u32.into())
                .unwrap()
                .remote_address()
                .unwrap()
                .ip(),
            remote.ip()
        );
    }
}
