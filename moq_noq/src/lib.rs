//! Adapter from an established async WebTransport (or raw QUIC) session to
//! the synchronous MoQ core. All actual transport I/O runs on Tokio tasks.

use bytes::{Buf, Bytes};
use moq_core::{
    SharedDropQueue, StreamError, Transport,
    wire::{ResetCode, SessionCloseCode},
};
use moq_core_async::{AsyncTransport, Notify};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify as TaskNotify, watch};
use web_transport_trait::{Error as WtError, RecvStream, SendStream, Session};

pub use web_transport_noq;
pub type NoqTransport = WebTransport<web_transport_noq::Session>;
pub type MoqSession = moq_core_async::MoqSession<NoqTransport>;

/// Parameters for an already established transport. `is_server` determines
/// synthetic stream ID parity, not how the QUIC connection was established.
pub struct ConnectParams<S> {
    pub session: S,
    pub is_server: bool,
    pub max_send_bytes_per_stream: usize,
    pub max_send_bytes_total: usize,
}

impl<S> ConnectParams<S> {
    pub fn new(session: S, is_server: bool) -> Self {
        Self {
            session,
            is_server,
            max_send_bytes_per_stream: 8 * 1024 * 1024,
            max_send_bytes_total: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionError(pub String);
impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConnectionError {}

#[derive(Default)]
struct SendState {
    queue: VecDeque<Bytes>,
    offset: usize,
    bytes: usize,
    fin: bool,
    fin_acked: bool,
    reset: Option<u32>,
    priority: i32,
    done: bool,
    stopped: Option<u64>,
}
impl SendState {
    fn advance(&mut self, n: usize) -> usize {
        self.offset += n;
        // A partial write still retains the entire front allocation.
        if self.offset < self.queue[0].len() {
            return 0;
        }
        self.offset = 0;
        let released = self.queue.pop_front().unwrap().len();
        self.bytes -= released;
        released
    }
}
#[derive(Default)]
struct RecvState {
    chunk: Bytes,
    end: Option<RecvEnd>,
    stop: Option<u32>,
    retired: bool,
    done: bool,
}
#[derive(Clone, Copy)]
enum RecvEnd {
    Fin,
    Reset(u64),
}
struct Stream {
    send: Option<SendState>,
    recv: Option<RecvState>,
    send_wake: Arc<TaskNotify>,
    recv_wake: Arc<TaskNotify>,
}
struct State {
    next: [u64; 4],
    streams: HashMap<u64, Stream>,
    queued: usize,
    error: Option<ConnectionError>,
    // Bound retained STOP_SENDING codes for streams already removed.
    stopped: VecDeque<(u64, u64)>,
}
impl State {
    fn stopped_code(&self, id: u64) -> Option<u64> {
        match self.streams.get(&id) {
            Some(st) => st.send.as_ref().and_then(|send| send.stopped),
            None => self
                .stopped
                .iter()
                .find(|(sid, _)| *sid == id)
                .map(|(_, code)| *code),
        }
    }

    fn retire_if_finished(&mut self, id: u64) {
        // A receive terminal stays visible until it is read or explicitly discarded.
        let finished = self.streams.get(&id).is_some_and(|st| {
            st.send.as_ref().is_none_or(|v| v.done)
                && st.recv.as_ref().is_none_or(|v| v.done && v.retired)
        });
        if !finished {
            return;
        }
        if let Some(send) = self.streams.remove(&id).and_then(|st| st.send) {
            self.queued -= send.bytes;
            if let Some(code) = send.stopped {
                self.stopped.push_back((id, code));
                if self.stopped.len() > 1024 {
                    self.stopped.pop_front();
                }
            }
        }
    }
}
struct Shared {
    state: Mutex<State>,
    wake: Notify,
    cancel: watch::Sender<bool>,
    per_stream: usize,
    total: usize,
}
impl Shared {
    fn allocate(
        &self,
        kind: usize,
        send: bool,
        recv: bool,
    ) -> (u64, Arc<TaskNotify>, Arc<TaskNotify>) {
        let mut s = self.state.lock().unwrap();
        let id = s.next[kind];
        s.next[kind] += 4;
        let send_wake = Arc::new(TaskNotify::new());
        let recv_wake = Arc::new(TaskNotify::new());
        s.streams.insert(
            id,
            Stream {
                send: send.then(SendState::default),
                recv: recv.then(RecvState::default),
                send_wake: send_wake.clone(),
                recv_wake: recv_wake.clone(),
            },
        );
        (id, send_wake, recv_wake)
    }
    fn fail(&self, e: impl std::fmt::Display) {
        let mut s = self.state.lock().unwrap();
        if s.error.is_none() {
            s.error = Some(ConnectionError(e.to_string()));
        }
        drop(s);
        self.cancel.send_replace(true);
        self.wake.notify();
    }
}

/// The native Tokio adapter. The concrete session is retained, so callers
/// can also keep a clone and access noq-specific connection methods.
pub struct WebTransport<S: Session> {
    session: S,
    shared: Arc<Shared>,
    local_kind: usize,
    closed: bool,
}

// Every worker races its I/O with a retained cancellation signal. The
// adapter owns the MoQ session; dropping it also closes that session.
async fn cancelled(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.wait_for(|v| *v).await;
}

impl<S> WebTransport<S>
where
    S: Session + Send + Sync,
    S::SendStream: Send + 'static,
    S::RecvStream: Send + 'static,
{
    /// Starts the adapter's I/O workers.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn new(params: ConnectParams<S>) -> (Self, Notify) {
        let wake = Notify::new();
        let (cancel, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                next: [0, 1, 2, 3],
                streams: HashMap::new(),
                queued: 0,
                error: None,
                stopped: VecDeque::new(),
            }),
            wake: wake.clone(),
            cancel,
            per_stream: params.max_send_bytes_per_stream,
            total: params.max_send_bytes_total,
        });
        let local_kind = usize::from(params.is_server);
        let session = params.session;
        for bidi in [false, true] {
            let (session, shared) = (session.clone(), shared.clone());
            tokio::spawn(async move {
                let mut cancel = shared.cancel.subscribe();
                loop {
                    if bidi {
                        let result = tokio::select! {
                            _ = cancelled(&mut cancel) => return,
                            r = session.accept_bi() => r,
                        };
                        match result {
                            Ok((send, recv)) => {
                                let (id, sw, rw) = shared.allocate(local_kind ^ 1, true, true);
                                tokio::spawn(send_worker(send, shared.clone(), id, sw));
                                tokio::spawn(recv_worker(recv, shared.clone(), id, rw));
                            }
                            Err(e) => {
                                shared.fail(e);
                                return;
                            }
                        }
                    } else {
                        let result = tokio::select! {
                            _ = cancelled(&mut cancel) => return,
                            r = session.accept_uni() => r,
                        };
                        match result {
                            Ok(recv) => {
                                let (id, _, rw) =
                                    shared.allocate((local_kind ^ 1) | 2, false, true);
                                tokio::spawn(recv_worker(recv, shared.clone(), id, rw));
                            }
                            Err(e) => {
                                shared.fail(e);
                                return;
                            }
                        }
                    }
                }
            });
        }
        (
            Self {
                session,
                shared,
                local_kind,
                closed: false,
            },
            wake,
        )
    }

    pub fn session(&self) -> &S {
        &self.session
    }

    fn open(&self, bidi: bool) -> Result<u64, StreamError> {
        if self.shared.state.lock().unwrap().error.is_some() {
            return Err(StreamError::Failed);
        }
        let kind = self.local_kind | if bidi { 0 } else { 2 };
        let (id, sw, rw) = self.shared.allocate(kind, true, bidi);
        let (session, shared) = (self.session.clone(), self.shared.clone());
        tokio::spawn(async move {
            let mut cancel = shared.cancel.subscribe();
            // Keep a single open future alive across local notifications;
            // a reset before opening cancels the pending stream allocation.
            let opening = async {
                if bidi {
                    session.open_bi().await.map(|(s, r)| (s, Some(r)))
                } else {
                    session.open_uni().await.map(|s| (s, None))
                }
            };
            tokio::pin!(opening);
            let result = loop {
                let reset = shared
                    .state
                    .lock()
                    .unwrap()
                    .streams
                    .get(&id)
                    .and_then(|s| s.send.as_ref())
                    .and_then(|s| s.reset);
                if reset.is_some() && !bidi {
                    let mut s = shared.state.lock().unwrap();
                    if let Some(send) = s.streams.get_mut(&id).and_then(|s| s.send.as_mut()) {
                        send.done = true;
                    }
                    s.retire_if_finished(id);
                    drop(s);
                    shared.wake.notify();
                    return;
                }
                tokio::select! {
                    _ = cancelled(&mut cancel) => return,
                    _ = sw.notified() => continue,
                    r = &mut opening => break r,
                }
            };
            match result {
                Ok((send, recv)) => {
                    if let Some(recv) = recv {
                        tokio::spawn(recv_worker(recv, shared.clone(), id, rw));
                    }
                    send_worker(send, shared, id, sw).await;
                }
                Err(e) => shared.fail(e),
            }
        });
        Ok(id)
    }

    fn enqueue(
        &self,
        id: u64,
        data: &[u8],
        fin: bool,
        full: StreamError,
    ) -> Result<usize, StreamError> {
        let mut s = self.shared.state.lock().unwrap();
        if s.error.is_some() {
            return Err(StreamError::Failed);
        }
        let remaining = self.shared.total.saturating_sub(s.queued);
        let Some(stream) = s.streams.get_mut(&id) else {
            return Err(s
                .stopped_code(id)
                .map(StreamError::StreamStopped)
                .unwrap_or(StreamError::InvalidStreamState(id)));
        };
        let send = stream
            .send
            .as_mut()
            .ok_or(StreamError::InvalidStreamState(id))?;
        if let Some(code) = send.stopped {
            return Err(StreamError::StreamStopped(code));
        }
        if send.done || send.fin || send.reset.is_some() {
            return Err(StreamError::InvalidStreamState(id));
        }
        if data.len() > remaining || data.len() > self.shared.per_stream.saturating_sub(send.bytes)
        {
            return Err(full);
        }
        if !data.is_empty() {
            send.queue.push_back(Bytes::copy_from_slice(data));
        }
        send.bytes += data.len();
        send.fin = fin;
        stream.send_wake.notify_one();
        s.queued += data.len();
        Ok(data.len())
    }
}

impl<S: Session> Drop for WebTransport<S> {
    fn drop(&mut self) {
        if !self.closed {
            self.session.close(0, "MoQ transport dropped");
        }
        self.shared.cancel.send_replace(true);
    }
}

impl<S> Transport for WebTransport<S>
where
    S: Session + Send + Sync,
    S::SendStream: Send + 'static,
    S::RecvStream: Send + 'static,
{
    fn recv_stream(&mut self, id: u64, dst: &mut [u8]) -> Result<usize, StreamError> {
        let mut s = self.shared.state.lock().unwrap();
        if s.error.is_some() {
            return Err(StreamError::Failed);
        }
        let stream = s
            .streams
            .get_mut(&id)
            .ok_or(StreamError::InvalidStreamState(id))?;
        let recv = stream
            .recv
            .as_mut()
            .ok_or(StreamError::InvalidStreamState(id))?;
        if !recv.chunk.is_empty() {
            let n = dst.len().min(recv.chunk.len());
            dst[..n].copy_from_slice(&recv.chunk[..n]);
            recv.chunk.advance(n);
            if recv.chunk.is_empty() {
                stream.recv_wake.notify_one();
            }
            return Ok(n);
        }
        let Some(end) = recv.end else {
            return Err(StreamError::Done);
        };
        recv.retired = true;
        s.retire_if_finished(id);
        Err(match end {
            RecvEnd::Fin => StreamError::Fin,
            RecvEnd::Reset(code) => StreamError::StreamReset(code),
        })
    }
    fn send_stream(&mut self, id: u64, data: &[u8], fin: bool) -> Result<usize, StreamError> {
        self.enqueue(id, data, fin, StreamError::Done)
    }
    fn send_stream_if_capacity(
        &mut self,
        id: u64,
        data: &[u8],
        fin: bool,
    ) -> Result<usize, StreamError> {
        self.enqueue(id, data, fin, StreamError::InsufficientCapacity)
    }
    fn open_uni_stream(&mut self) -> Result<u64, StreamError> {
        self.open(false)
    }
    fn open_bidi_stream(&mut self) -> Result<u64, StreamError> {
        self.open(true)
    }
    fn set_stream_priority(&mut self, id: u64, urgency: u8, _incremental: bool) {
        let mut s = self.shared.state.lock().unwrap();
        if let Some(st) = s.streams.get_mut(&id)
            && let Some(send) = st.send.as_mut()
        {
            send.priority = -i32::from(urgency);
            st.send_wake.notify_one();
        }
    }
    fn reset_stream(&mut self, id: u64, code: ResetCode) {
        let mut s = self.shared.state.lock().unwrap();
        if let Some(st) = s.streams.get_mut(&id)
            && let Some(send) = st.send.as_mut()
        {
            send.reset = Some(code.to_u32());
            st.send_wake.notify_one();
        }
    }
    fn stop_sending(&mut self, id: u64, code: ResetCode) -> bool {
        let mut s = self.shared.state.lock().unwrap();
        if let Some(st) = s.streams.get_mut(&id)
            && let Some(recv) = st.recv.as_mut()
        {
            recv.stop = Some(code.to_u32());
            recv.chunk = Bytes::new();
            recv.retired = true;
            st.recv_wake.notify_one();
            s.retire_if_finished(id);
        }
        true
    }
    fn stream_stopped(&mut self, id: u64) -> bool {
        self.shared.state.lock().unwrap().stopped_code(id).is_some()
    }
    fn stream_finished(&self, id: u64) -> bool {
        self.shared
            .state
            .lock()
            .unwrap()
            .streams
            .get(&id)
            .is_none_or(|st| st.send.as_ref().is_some_and(|send| send.fin_acked))
    }
    fn close_session(&mut self, code: SessionCloseCode, reason: &str) {
        if !self.closed {
            self.closed = true;
            self.session.close(code.into(), reason);
            self.shared.fail(reason);
        }
    }
}

impl<S> AsyncTransport for WebTransport<S>
where
    S: Session + Send + Sync,
    S::SendStream: Send + 'static,
    S::RecvStream: Send + 'static,
{
    type FlushError = ConnectionError;
    type DropQueue = SharedDropQueue;
    type ConnectParams = ConnectParams<S>;
    type ConnectError = std::convert::Infallible;
    async fn connect(params: Self::ConnectParams) -> Result<(Self, Notify), Self::ConnectError> {
        Ok(Self::new(params))
    }
    fn readable_stream_ids(&self) -> Vec<u64> {
        let s = self.shared.state.lock().unwrap();
        if s.error.is_some() {
            return Vec::new();
        }
        s.streams
            .iter()
            .filter_map(|(&id, st)| {
                st.recv
                    .as_ref()
                    .filter(|r| !r.retired && (!r.chunk.is_empty() || r.end.is_some()))
                    .map(|_| id)
            })
            .collect()
    }
    async fn flush_sends(&mut self) -> Result<(), Self::FlushError> {
        match self.shared.state.lock().unwrap().error.clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

// One worker per send half. A blocked write cannot hold up another stream,
// and the local wake interrupts it for RESET_STREAM / priority changes.
async fn send_worker<T: SendStream + Send + 'static>(
    mut stream: T,
    shared: Arc<Shared>,
    id: u64,
    wake: Arc<TaskNotify>,
) {
    enum Action {
        Reset(u32),
        Write(Bytes),
        Finish,
        Wait,
    }
    let mut cancel = shared.cancel.subscribe();
    let mut finished = false;
    let mut fin_acked = false;
    let mut last_priority = None;
    let error = loop {
        let (priority, action) = {
            let s = shared.state.lock().unwrap();
            let Some(send) = s.streams.get(&id).and_then(|s| s.send.as_ref()) else {
                return;
            };
            let action = if let Some(code) = send.reset {
                Action::Reset(code)
            } else if let Some(chunk) = send.queue.front() {
                Action::Write(chunk.slice(send.offset..))
            } else if send.fin && !finished {
                Action::Finish
            } else {
                Action::Wait
            };
            (send.priority, action)
        };
        if last_priority != Some(priority) {
            stream.set_priority(priority);
            last_priority = Some(priority);
        }
        match action {
            Action::Reset(code) => {
                stream.reset(code);
                break None;
            }
            Action::Write(chunk) => {
                let result = tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => { stream.reset(0); return; }
                    _ = wake.notified() => continue,
                    r = stream.write(&chunk) => r,
                };
                match result {
                    Ok(n) if n > 0 && n <= chunk.len() => {
                        drop(chunk);
                        let mut s = shared.state.lock().unwrap();
                        let Some(send) = s.streams.get_mut(&id).and_then(|s| s.send.as_mut())
                        else {
                            return;
                        };
                        let released = if send.reset.is_none() {
                            send.advance(n)
                        } else {
                            0
                        };
                        s.queued -= released;
                        drop(s);
                        if released != 0 {
                            shared.wake.notify();
                        }
                    }
                    Ok(_) => {
                        stream.reset(0);
                        shared.fail("WT write made no valid progress");
                        return;
                    }
                    Err(e) => break Some(e),
                }
            }
            Action::Finish => match stream.finish() {
                Ok(()) => finished = true,
                Err(e) => break Some(e),
            },
            Action::Wait => {
                let result = tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => { stream.reset(0); return; }
                    _ = wake.notified() => continue,
                    r = stream.closed() => r,
                };
                fin_acked = finished && result.is_ok();
                break result.err();
            }
        }
    };
    let stopped = error.as_ref().and_then(|e| e.stream_error()).map(u64::from);
    if let Some(e) = error
        && stopped.is_none()
    {
        shared.fail(e);
    }
    {
        let mut s = shared.state.lock().unwrap();
        if let Some(send) = s.streams.get_mut(&id).and_then(|s| s.send.as_mut()) {
            let bytes = std::mem::take(&mut send.bytes);
            send.queue.clear();
            send.done = true;
            send.fin_acked = fin_acked;
            send.stopped = stopped;
            s.queued -= bytes;
        }
        s.retire_if_finished(id);
    }
    shared.wake.notify();
}

// At most one unread chunk is buffered per receive half. Waiting for the
// MoQ core to drain it leaves subsequent receive credit in the QUIC stack.
async fn recv_worker<T: RecvStream + Send + 'static>(
    mut stream: T,
    shared: Arc<Shared>,
    id: u64,
    wake: Arc<TaskNotify>,
) {
    const CHUNK: usize = 64 * 1024;
    let mut cancel = shared.cancel.subscribe();
    let end = 'worker: loop {
        let (stop, empty) = {
            let s = shared.state.lock().unwrap();
            let Some(recv) = s.streams.get(&id).and_then(|s| s.recv.as_ref()) else {
                return;
            };
            (recv.stop, recv.chunk.is_empty())
        };
        if let Some(code) = stop {
            stream.stop(code);
            break None;
        }
        if !empty {
            tokio::select! {
                _ = cancelled(&mut cancel) => { stream.stop(0); return; }
                _ = wake.notified() => continue,
            }
        }
        let result = {
            let read = stream.read_chunk(CHUNK);
            tokio::pin!(read);
            loop {
                // Retain the read future across spurious notifications.
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return,
                    _ = wake.notified() => {
                        let stop = shared.state.lock().unwrap().streams.get(&id)
                            .and_then(|s| s.recv.as_ref()).and_then(|s| s.stop);
                        if stop.is_some() { continue 'worker; }
                    }
                    r = &mut read => break r,
                }
            }
        };
        match result {
            Ok(Some(chunk)) if !chunk.is_empty() => {
                let mut s = shared.state.lock().unwrap();
                let Some(recv) = s.streams.get_mut(&id).and_then(|s| s.recv.as_mut()) else {
                    return;
                };
                if recv.stop.is_none() {
                    recv.chunk = chunk;
                }
                drop(s);
                shared.wake.notify();
            }
            Ok(Some(_)) => {
                tokio::task::yield_now().await;
            }
            Ok(None) => break Some(RecvEnd::Fin),
            Err(e) => match e.stream_error() {
                Some(code) => break Some(RecvEnd::Reset(u64::from(code))),
                None => {
                    shared.fail(e);
                    break None;
                }
            },
        }
    };
    {
        let mut s = shared.state.lock().unwrap();
        if let Some(recv) = s.streams.get_mut(&id).and_then(|s| s.recv.as_mut()) {
            recv.end = end;
            recv.done = true;
            if end.is_none() {
                recv.retired = true;
            }
        }
        s.retire_if_finished(id);
    }
    shared.wake.notify();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Shared {
        let (cancel, _) = watch::channel(false);
        Shared {
            state: Mutex::new(State {
                next: [0, 1, 2, 3],
                streams: HashMap::new(),
                queued: 0,
                error: None,
                stopped: VecDeque::new(),
            }),
            wake: Notify::new(),
            cancel,
            per_stream: 1024,
            total: 4096,
        }
    }

    #[test]
    fn synthetic_stream_ids_preserve_direction_and_initiator() {
        let s = shared();
        for kind in 0..4 {
            for n in 0..3 {
                let (id, _, _) = s.allocate(kind, true, true);
                assert_eq!(id, kind as u64 + 4 * n);
            }
        }
        assert_eq!(s.state.lock().unwrap().streams.len(), 12);
    }

    #[test]
    fn retirement_releases_budget_and_preserves_stop_code() {
        let s = shared();
        let (id, _, _) = s.allocate(0, true, true);
        let (other, _, _) = s.allocate(2, true, false);
        let mut state = s.state.lock().unwrap();
        state.queued = 4;
        let st = state.streams.get_mut(&id).unwrap();
        let send = st.send.as_mut().unwrap();
        send.bytes = 4;
        send.done = true;
        send.stopped = Some(42);
        let recv = st.recv.as_mut().unwrap();
        recv.done = true;
        recv.end = Some(RecvEnd::Reset(7));

        state.retire_if_finished(id);
        let recv = state.streams.get_mut(&id).unwrap().recv.as_mut().unwrap();
        assert!(matches!(recv.end, Some(RecvEnd::Reset(7))));
        recv.retired = true;
        state.retire_if_finished(id);

        assert!(!state.streams.contains_key(&id));
        assert!(state.streams.contains_key(&other));
        assert_eq!(state.queued, 0);
        assert_eq!(state.stopped.front(), Some(&(id, 42)));
    }

    #[test]
    fn partial_write_keeps_the_buffer_charged_until_it_is_freed() {
        let mut send = SendState {
            queue: [Bytes::from_static(b"abcd"), Bytes::from_static(b"ef")].into(),
            bytes: 6,
            ..SendState::default()
        };
        assert_eq!(send.advance(3), 0);
        assert_eq!(send.bytes, 6);
        assert_eq!(&send.queue[0][send.offset..], b"d");
        assert_eq!(send.advance(1), 4);
        assert_eq!(send.bytes, 2);
        assert_eq!(&send.queue[0][send.offset..], b"ef");
    }

    #[test]
    fn noq_implements_adapter_bounds() {
        fn assert_transport<T: AsyncTransport>() {}
        assert_transport::<NoqTransport>();
    }
}
