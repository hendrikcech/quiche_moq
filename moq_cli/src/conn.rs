use std::collections::{HashMap, HashSet};

use quiche_moq::{Announcement, Event, FetchOk, NamespaceSubscription, Publication, RequestFailure, Subscription, TracksSubscription};
use quiche_moq::wire::control_message::{PublishOkMessage, SubscribeOkMessage};
use quiche_moq::wire::{Location, Namespace, NamespaceTrackname, NamespaceTuple, RequestId, TrackAlias};
use quiche_moq_webtransport_helper::MoqWebTransportHelper;
use quiche_mio_runner::quiche_endpoint::{ClientId, ConnMap};
use quiche_utils::keepalive_handler::KeepaliveHandler;
use moq_core::{MoqTransportSession, StreamError, Transport};
use moq_noq::NoqTransport;
use moq_wire::{ResetCode, SessionCloseCode};

/// Per-connection state for an actively-PUBLISHed track (--pub ns--track).
pub(crate) struct ClientActivePubTrack {
    pub(crate) track: NamespaceTrackname,
    pub(crate) request_id: Option<RequestId>,
}

pub(crate) struct ConnAppData {
    pub(crate) moq_helper: MoqWebTransportHelper,
    pub(crate) state: ConnState,
    /// Only set for outbound connections (--connect / --upstream); None for accepted connections.
    pub(crate) keepalive: Option<KeepaliveHandler>,
    /// When this connection was first seen, for the stuck-session warning in
    /// `tick_connections`. A connection whose QUIC handshake completed but
    /// whose MoQ session never initializes is otherwise silent: it holds an
    /// established session, transfers nothing, and only shows up as a client
    /// reporting zero objects.
    pub(crate) first_seen: Option<std::time::Instant>,
    /// One-shot latch for that warning.
    pub(crate) warned_moq_stuck: bool,
    /// One-shot latch for the `moq_cli:connection_ids` qlog record.
    pub(crate) logged_conn_ids: bool,
    /// `--reconnect`: upstream URL to re-dial once this connection closes.
    pub(crate) reconnect_url: Option<String>,
}

impl ConnAppData {
    pub(crate) fn new(moq_helper: MoqWebTransportHelper, keepalive: Option<KeepaliveHandler>) -> Self {
        Self {
            moq_helper,
            state: ConnState::default(),
            keepalive,
            first_seen: None,
            warned_moq_stuck: false,
            logged_conn_ids: false,
            reconnect_url: None,
        }
    }
}

#[derive(Default)]
pub(crate) struct ConnState {
    pub(crate) announced_namespaces: HashMap<Namespace, Announcement>,
    pub(crate) logged_connect: bool,
    pub(crate) responses: Responses,
    pub(crate) handles: Handles,
}

pub(crate) struct NoqConn {
    pub(crate) session: MoqTransportSession,
    pub(crate) transport: NoqTransport,
    pub(crate) state: ConnState,
    pub(crate) is_server: bool,
}

pub(crate) enum Connections<'a> {
    Quiche(&'a mut ConnMap<ConnAppData>),
    Noq(&'a mut HashMap<ClientId, NoqConn>),
}

pub(crate) struct ConnView<'a> {
    pub(crate) moq: Option<MoqHandle<'a>>,
    pub(crate) state: &'a mut ConnState,
    pub(crate) is_server: bool,
}

fn quiche_view(conn: &mut quiche_mio_runner::quiche_endpoint::Conn<ConnAppData>) -> ConnView<'_> {
    let is_server = conn.conn.is_server();
    let moq = conn.app_data.moq_helper.moq_handle(&mut conn.conn).map(Into::into);
    ConnView { moq, state: &mut conn.app_data.state, is_server }
}

fn noq_view(conn: &mut NoqConn) -> ConnView<'_> {
    let moq = MoqHandle { session: &mut conn.session, t: NodeTransport::Noq(&mut conn.transport) };
    ConnView { moq: Some(moq), state: &mut conn.state, is_server: conn.is_server }
}

impl Connections<'_> {
    pub(crate) fn get_mut(&mut self, cid: ClientId) -> Option<ConnView<'_>> {
        match self {
            Self::Quiche(conns) => conns.get_mut(cid).map(quiche_view),
            Self::Noq(conns) => conns.get_mut(&cid).map(noq_view),
        }
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (ClientId, ConnView<'_>)> {
        let (quiche, noq) = match self {
            Self::Quiche(conns) => (Some(conns.iter_mut()), None),
            Self::Noq(conns) => (None, Some(conns.iter_mut())),
        };
        quiche.into_iter().flatten().map(|(cid, conn)| (cid, quiche_view(conn)))
            .chain(noq.into_iter().flatten().map(|(&cid, conn)| (cid, noq_view(conn))))
    }
}

pub(crate) struct MoqHandle<'a> {
    pub(crate) session: &'a mut MoqTransportSession,
    pub(crate) t: NodeTransport<'a>,
}

impl<'a> From<quiche_moq::MoqHandle<'a>> for MoqHandle<'a> {
    fn from(moq: quiche_moq::MoqHandle<'a>) -> Self {
        Self { session: moq.session, t: NodeTransport::Quiche(moq.t) }
    }
}

pub(crate) enum NodeTransport<'a> {
    Quiche(quiche_moq::QuicheTransport<'a>),
    Noq(&'a mut NoqTransport),
}

macro_rules! forward_transport {
    ($($(#[$attr:meta])* fn $name:ident($self:ident: $borrow:ty $(, $arg:ident: $ty:ty)*) $(-> $ret:ty)?;)*) => {
        $($(#[$attr])* fn $name($self: $borrow $(, $arg: $ty)*) $(-> $ret)? {
            match $self {
                Self::Quiche(t) => t.$name($($arg),*),
                Self::Noq(t) => t.$name($($arg),*),
            }
        })*
    };
}

impl Transport for NodeTransport<'_> {
    forward_transport! {
        fn recv_stream(self: &mut Self, id: u64, buf: &mut [u8]) -> Result<usize, StreamError>;
        fn send_stream(self: &mut Self, id: u64, data: &[u8], fin: bool) -> Result<usize, StreamError>;
        fn send_stream_if_capacity(self: &mut Self, id: u64, data: &[u8], fin: bool) -> Result<usize, StreamError>;
        fn open_uni_stream(self: &mut Self) -> Result<u64, StreamError>;
        fn open_bidi_stream(self: &mut Self) -> Result<u64, StreamError>;
        fn set_stream_priority(self: &mut Self, id: u64, urgency: u8, incremental: bool);
        fn reset_stream(self: &mut Self, id: u64, code: ResetCode);
        fn stop_sending(self: &mut Self, id: u64, code: ResetCode) -> bool;
        fn stream_stopped(self: &mut Self, id: u64) -> bool;
        fn stream_finished(self: &Self, id: u64) -> bool;
        fn send_datagram(self: &mut Self, datagram: &[u8]) -> Result<(), StreamError>;
        fn recv_datagram(self: &mut Self) -> Option<Vec<u8>>;
        fn close_session(self: &mut Self, code: SessionCloseCode, reason: &str);
        fn qlog_streamer(self: &mut Self) -> Option<&mut qlog::streamer::QlogStreamer>;
    }
}

/// Our ends of this connection's tracks, by Request ID: the relay's track
/// state names them by ID. Ended with the connection.
#[derive(Default)]
pub(crate) struct Handles {
    pub(crate) subscriptions: HashMap<RequestId, Subscription>,
    pub(crate) publications: HashMap<RequestId, Publication>,
    pub(crate) namespace_subscriptions: HashMap<RequestId, NamespaceSubscriber>,
    pub(crate) tracks_subscriptions: HashMap<RequestId, TracksSubscriber>,
}

/// The peer's SUBSCRIBE_NAMESPACE, with the namespaces under its prefix named to it.
pub(crate) struct NamespaceSubscriber {
    pub(crate) prefix: NamespaceTuple,
    pub(crate) handle: NamespaceSubscription,
    pub(crate) named: HashSet<Namespace>,
}

/// The peer's SUBSCRIBE_TRACKS, with the tracks under its prefix PUBLISHed or PUBLISH_BLOCKED to it,
/// and the PUBLISH's Request ID while it awaits its answer.
pub(crate) struct TracksSubscriber {
    pub(crate) prefix: NamespaceTuple,
    pub(crate) forward: bool,
    pub(crate) handle: TracksSubscription,
    pub(crate) published: HashMap<NamespaceTrackname, Option<RequestId>>,
}

impl Handles {
    /// The peer ended or rejected `request_id`.
    pub(crate) fn forget(&mut self, request_id: RequestId) {
        self.subscriptions.remove(&request_id);
        self.publications.remove(&request_id);
        self.namespace_subscriptions.remove(&request_id);
        self.tracks_subscriptions.remove(&request_id);
    }
}

/// Answers to this node's requests on one connection, filed from the
/// session's events until the logic that sent the request takes them.
#[derive(Default)]
pub(crate) struct Responses(HashMap<RequestId, Response>);

enum Response {
    SubscribeOk(TrackAlias, SubscribeOkMessage),
    PublishOk(PublishOkMessage),
    RequestUpdateOk(Option<Location>),
    FetchOk(FetchOk),
    Error(RequestFailure),
}

impl Responses {
    /// Files a response, hands back any other event.
    pub(crate) fn file(&mut self, event: Event) -> Option<Event> {
        let (request_id, response) = match event {
            Event::SubscribeOk { request_id, track_alias, message } => (request_id, Response::SubscribeOk(track_alias, message)),
            Event::PublishOk { request_id, message } => (request_id, Response::PublishOk(message)),
            Event::RequestUpdateOk { request_id, largest_location } => (request_id, Response::RequestUpdateOk(largest_location)),
            Event::FetchOk { request_id, end_of_track, end_location } => (request_id, Response::FetchOk(FetchOk { end_of_track, end_location })),
            Event::RequestFailed { request_id, failure } => (request_id, Response::Error(failure)),
            event => return Some(event),
        };
        self.0.insert(request_id, response);
        None
    }

    /// The answer to `request_id` if `accepted` takes it or it is a failure, any other stays filed.
    fn take<T>(&mut self, request_id: RequestId, accepted: fn(Response) -> Result<T, Response>) -> Option<Result<T, RequestFailure>> {
        match self.0.remove(&request_id)? {
            Response::Error(e) => Some(Err(e)),
            response => match accepted(response) {
                Ok(answer) => Some(Ok(answer)),
                Err(other) => { self.0.insert(request_id, other); None }
            },
        }
    }

    pub(crate) fn take_subscribe(&mut self, request_id: RequestId) -> Option<Result<(TrackAlias, SubscribeOkMessage), RequestFailure>> {
        self.take(request_id, |r| match r { Response::SubscribeOk(alias, message) => Ok((alias, message)), other => Err(other) })
    }

    pub(crate) fn take_publish(&mut self, request_id: RequestId) -> Option<Result<PublishOkMessage, RequestFailure>> {
        self.take(request_id, |r| match r { Response::PublishOk(message) => Ok(message), other => Err(other) })
    }

    pub(crate) fn take_request_update(&mut self, request_id: RequestId) -> Option<Result<Option<Location>, RequestFailure>> {
        self.take(request_id, |r| match r { Response::RequestUpdateOk(largest) => Ok(largest), other => Err(other) })
    }

    pub(crate) fn take_fetch(&mut self, request_id: RequestId) -> Option<Result<FetchOk, RequestFailure>> {
        self.take(request_id, |r| match r { Response::FetchOk(ok) => Ok(ok), other => Err(other) })
    }

    /// The request ended without its answer taken.
    pub(crate) fn forget(&mut self, request_id: RequestId) {
        self.0.remove(&request_id);
    }
}
