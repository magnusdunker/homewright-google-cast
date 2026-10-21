//! One cast device: a persistent connection that reconnects with backoff,
//! keeps the receiver and media status, pushes state to HomewrightHQ when it
//! changes, and carries out service calls.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Map, Value};
use thehome_plugin::{Error, Host};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::conn::{self, Stream};
use crate::proto::{self, CastMessage, NS_CONNECTION, NS_HEARTBEAT, NS_MEDIA, NS_RECEIVER, RECEIVER, SENDER};
use crate::status::{self, DEFAULT_MEDIA_RECEIVER};

const HEARTBEAT: Duration = Duration::from_secs(5);
/// Give up on a connection that has been silent this long.
const SILENCE: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// How long play_media waits for the app to start and the media to load.
const LOAD_TIMEOUT: Duration = Duration::from_secs(20);

pub type Reply = oneshot::Sender<Result<(), Error>>;

pub enum Command {
    Service { service: String, data: Map<String, Value>, reply: Reply },
    /// Status for diagnostics.
    Snapshot(oneshot::Sender<Value>),
}

/// Holds the connection task; dropping it stops the task.
pub struct DeviceHandle {
    cmd: mpsc::UnboundedSender<Command>,
    addr: watch::Sender<(String, u16)>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for DeviceHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DeviceHandle {
    pub fn spawn(host: Host, entity_id: String, label: String, addr: (String, u16)) -> Self {
        let (cmd, cmds) = mpsc::unbounded_channel();
        let (addr_tx, addr_rx) = watch::channel(addr);
        let task = tokio::spawn(run(host, entity_id, label, addr_rx, cmds));
        Self { cmd, addr: addr_tx, task }
    }

    /// Move to a new address, e.g. after DHCP gave the device another one.
    pub fn set_addr(&self, addr: (String, u16)) {
        self.addr.send_if_modified(|a| {
            let changed = *a != addr;
            *a = addr;
            changed
        });
    }

    pub fn addr(&self) -> (String, u16) {
        self.addr.borrow().clone()
    }

    /// Something to send commands with that does not borrow the handle.
    pub fn caller(&self) -> Caller {
        Caller(self.cmd.clone())
    }
}

#[derive(Clone)]
pub struct Caller(mpsc::UnboundedSender<Command>);

impl Caller {
    pub async fn call(&self, service: &str, data: Map<String, Value>) -> Result<(), Error> {
        let (reply, rx) = oneshot::channel();
        self.0
            .send(Command::Service { service: service.into(), data, reply })
            .map_err(|_| Error::unavailable("forbindelsen er stoppet"))?;
        rx.await.unwrap_or_else(|_| Err(Error::unavailable("forbindelsen blev afbrudt")))
    }

    pub async fn snapshot(&self) -> Value {
        let (tx, rx) = oneshot::channel();
        if self.0.send(Command::Snapshot(tx)).is_err() {
            return Value::Null;
        }
        tokio::time::timeout(Duration::from_secs(5), rx).await.ok().and_then(Result::ok).unwrap_or(Value::Null)
    }
}

/// What was last sent to HomewrightHQ, so unchanged states are not sent again.
#[derive(PartialEq)]
enum Published {
    Nothing,
    Unavailable,
    State(String, Map<String, Value>),
}

struct Out {
    host: Host,
    entity_id: String,
    last: Published,
}

impl Out {
    async fn unavailable(&mut self) {
        if self.last != Published::Unavailable {
            self.last = Published::Unavailable;
            if let Err(e) = self.host.set_unavailable(&self.entity_id).await {
                crate::warn!("{}: {e}", self.entity_id);
            }
        }
    }

    async fn state(&mut self, state: String, attrs: Map<String, Value>) {
        // the timestamp alone is no reason to send it again
        let mut cmp = attrs.clone();
        cmp.remove("media_position_updated_at");
        if let Published::State(s, a) = &self.last {
            let mut old = a.clone();
            old.remove("media_position_updated_at");
            if *s == state && old == cmp {
                return;
            }
        }
        self.last = Published::State(state.clone(), attrs.clone());
        if let Err(e) = self.host.set_state(&self.entity_id, state, attrs).await {
            crate::warn!("{}: {e}", self.entity_id);
        }
    }
}

async fn run(host: Host, entity_id: String, label: String, mut addr: watch::Receiver<(String, u16)>, mut cmds: mpsc::UnboundedReceiver<Command>) {
    let mut out = Out { host, entity_id, last: Published::Nothing };
    let mut backoff = Duration::from_secs(1);
    let mut logged_failure = false;
    loop {
        let (h, p) = addr.borrow_and_update().clone();
        let reason = match conn::connect(&h, p).await {
            Ok(stream) => {
                crate::info!("{label}: forbundet til {h}:{p}");
                logged_failure = false;
                backoff = Duration::from_secs(1);
                let mut s = Session::new(stream);
                let why = s.run(&mut cmds, &mut addr, &mut out).await;
                s.fail_pending("forbindelsen blev afbrudt");
                match why {
                    End::Stopped => return,
                    End::NewAddress => continue,
                    End::Lost(r) => {
                        crate::info!("{label}: forbindelsen til {h}:{p} blev afbrudt: {r}");
                        r
                    }
                }
            }
            Err(e) => {
                if !logged_failure {
                    crate::info!("{label}: kan ikke forbinde til {h}:{p}: {e}");
                    logged_failure = true;
                }
                e.to_string()
            }
        };
        out.unavailable().await;

        let sleep = tokio::time::sleep(backoff);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                r = addr.changed() => {
                    if r.is_err() { return; }
                    break;
                }
                cmd = cmds.recv() => match cmd {
                    None => return,
                    Some(Command::Service { reply, .. }) => {
                        let _ = reply.send(Err(Error::unavailable(format!("{label} er ikke forbundet"))));
                    }
                    Some(Command::Snapshot(tx)) => {
                        let _ = tx.send(json!({ "connected": false, "host": h, "port": p, "error": reason }));
                    }
                },
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

enum End {
    Stopped,
    NewAddress,
    Lost(String),
}

struct PendingLoad {
    media: Value,
    reply: Reply,
    deadline: Instant,
    launch_request: u64,
}

struct Session {
    w: WriteHalf<Stream>,
    incoming: mpsc::Receiver<std::io::Result<CastMessage>>,
    reader: tokio::task::JoinHandle<()>,
    next_id: u64,
    receiver: Value,
    media: Option<Value>,
    /// The app transport we have connected to for media.
    transport: Option<String>,
    pending_load: Option<PendingLoad>,
    /// LOAD requests waiting for their MEDIA_STATUS.
    loads: HashMap<u64, (Instant, Reply)>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

async fn read_loop(mut r: ReadHalf<Stream>, tx: mpsc::Sender<std::io::Result<CastMessage>>) {
    loop {
        let m = proto::read_message(&mut r).await;
        let end = m.is_err();
        if tx.send(m).await.is_err() || end {
            break;
        }
    }
}

impl Session {
    fn new(stream: Stream) -> Self {
        let (r, w) = tokio::io::split(stream);
        let (tx, incoming) = mpsc::channel(64);
        let reader = tokio::spawn(read_loop(r, tx));
        Self {
            w,
            incoming,
            reader,
            next_id: 1,
            receiver: Value::Null,
            media: None,
            transport: None,
            pending_load: None,
            loads: HashMap::new(),
        }
    }

    fn fail_pending(&mut self, why: &str) {
        if let Some(p) = self.pending_load.take() {
            let _ = p.reply.send(Err(Error::unavailable(why)));
        }
        for (_, (_, reply)) in self.loads.drain() {
            let _ = reply.send(Err(Error::unavailable(why)));
        }
    }

    async fn send(&mut self, dest: &str, ns: &str, payload: Value) -> std::io::Result<()> {
        proto::write_message(&mut self.w, &CastMessage::json(SENDER, dest, ns, &payload)).await
    }

    /// Send a request with a fresh requestId and return the id.
    async fn request(&mut self, dest: &str, ns: &str, mut payload: Value) -> std::io::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        payload["requestId"] = json!(id);
        self.send(dest, ns, payload).await?;
        Ok(id)
    }

    async fn run(&mut self, cmds: &mut mpsc::UnboundedReceiver<Command>, addr: &mut watch::Receiver<(String, u16)>, out: &mut Out) -> End {
        let start = async {
            self.send(RECEIVER, NS_CONNECTION, json!({ "type": "CONNECT", "userAgent": "HomewrightHQ" })).await?;
            self.request(RECEIVER, NS_RECEIVER, json!({ "type": "GET_STATUS" })).await
        };
        if let Err(e) = start.await {
            return End::Lost(e.to_string());
        }
        let mut last_seen = Instant::now();
        let mut tick = tokio::time::interval_at(Instant::now() + HEARTBEAT, HEARTBEAT);
        loop {
            let res = tokio::select! {
                m = self.incoming.recv() => match m {
                    Some(Ok(m)) => {
                        last_seen = Instant::now();
                        self.on_message(m).await
                    }
                    Some(Err(e)) => return End::Lost(e.to_string()),
                    None => return End::Lost("forbindelsen lukkede".into()),
                },
                _ = tick.tick() => {
                    if last_seen.elapsed() > SILENCE {
                        return End::Lost("enheden svarer ikke".into());
                    }
                    self.expire();
                    self.send(RECEIVER, NS_HEARTBEAT, json!({ "type": "PING" })).await.map(|_| false).map_err(|e| e.to_string())
                }
                cmd = cmds.recv() => match cmd {
                    None => return End::Stopped,
                    Some(Command::Snapshot(tx)) => {
                        let (h, p) = addr.borrow().clone();
                        let _ = tx.send(json!({ "connected": true, "host": h, "port": p, "receiver": self.receiver, "media": self.media }));
                        Ok(false)
                    }
                    Some(Command::Service { service, data, reply }) => self.on_service(&service, &data, reply).await.map(|_| false).map_err(|e| e.to_string()),
                },
                r = addr.changed() => {
                    return if r.is_ok() { End::NewAddress } else { End::Stopped };
                }
            };
            match res {
                Ok(true) => self.publish(out).await,
                Ok(false) => {}
                Err(e) => return End::Lost(e),
            }
        }
    }

    async fn publish(&self, out: &mut Out) {
        if self.receiver.is_null() {
            return;
        }
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let (state, attrs) = status::state_of(&self.receiver, self.media.as_ref(), &now);
        out.state(state, attrs).await;
    }

    fn expire(&mut self) {
        let now = Instant::now();
        if self.pending_load.as_ref().is_some_and(|p| p.deadline <= now) {
            let p = self.pending_load.take().unwrap();
            let _ = p.reply.send(Err(Error::failed("appen startede ikke i tide")));
        }
        let late: Vec<u64> = self.loads.iter().filter(|(_, (d, _))| *d <= now).map(|(id, _)| *id).collect();
        for id in late {
            if let Some((_, reply)) = self.loads.remove(&id) {
                let _ = reply.send(Err(Error::failed("mediet blev ikke indlæst i tide")));
            }
        }
    }

    /// Handle a message from the device. `Ok(true)` when the state changed.
    async fn on_message(&mut self, m: CastMessage) -> Result<bool, String> {
        let Some(p) = m.payload() else { return Ok(false) };
        let ty = p["type"].as_str().unwrap_or_default();
        let io = |e: std::io::Error| e.to_string();
        match (m.namespace.as_str(), ty) {
            (NS_HEARTBEAT, "PING") => {
                proto::write_message(&mut self.w, &CastMessage::json(SENDER, &m.source_id, NS_HEARTBEAT, &json!({ "type": "PONG" })))
                    .await
                    .map_err(io)?;
                Ok(false)
            }
            (NS_CONNECTION, "CLOSE") => {
                if m.source_id == RECEIVER {
                    return Err("enheden lukkede forbindelsen".into());
                }
                if self.transport.as_deref() == Some(m.source_id.as_str()) {
                    self.transport = None;
                    self.media = None;
                    return Ok(true);
                }
                Ok(false)
            }
            (NS_RECEIVER, "RECEIVER_STATUS") => {
                self.receiver = p["status"].clone();
                self.sync_transport().await.map_err(io)?;
                self.continue_load().await.map_err(io)?;
                Ok(true)
            }
            (NS_RECEIVER, "LAUNCH_ERROR") => {
                if self.pending_load.as_ref().is_some_and(|l| p["requestId"].as_u64() == Some(l.launch_request)) {
                    let l = self.pending_load.take().unwrap();
                    let why = p["reason"].as_str().unwrap_or("ukendt fejl");
                    let _ = l.reply.send(Err(Error::failed(format!("appen kunne ikke starte: {why}"))));
                }
                Ok(false)
            }
            (NS_MEDIA, "MEDIA_STATUS") => {
                if let Some(reply) = p["requestId"].as_u64().and_then(|id| self.loads.remove(&id)) {
                    let _ = reply.1.send(Ok(()));
                }
                if self.transport.as_deref() == Some(m.source_id.as_str()) {
                    self.media = status::merge_media(self.media.as_ref(), &p);
                    return Ok(true);
                }
                Ok(false)
            }
            (NS_MEDIA, "LOAD_FAILED" | "LOAD_CANCELLED" | "INVALID_REQUEST" | "ERROR") => {
                if let Some((_, reply)) = p["requestId"].as_u64().and_then(|id| self.loads.remove(&id)) {
                    let why = p["reason"].as_str().or(p["detailedErrorCode"].as_str()).unwrap_or(ty);
                    let _ = reply.send(Err(Error::failed(format!("enheden kunne ikke afspille mediet: {why}"))));
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Connect to the running app's media channel, or forget it when the app stopped.
    async fn sync_transport(&mut self) -> std::io::Result<()> {
        let app = status::active_app(&self.receiver).filter(|a| status::has_media(a));
        let want = app.and_then(|a| a["transportId"].as_str()).map(String::from);
        if want == self.transport {
            return Ok(());
        }
        self.media = None;
        self.transport = want.clone();
        if let Some(t) = want {
            self.send(&t, NS_CONNECTION, json!({ "type": "CONNECT", "userAgent": "HomewrightHQ" })).await?;
            self.request(&t, NS_MEDIA, json!({ "type": "GET_STATUS" })).await?;
        }
        Ok(())
    }

    /// play_media waits for the Default Media Receiver; send LOAD once it runs.
    async fn continue_load(&mut self) -> std::io::Result<()> {
        let ready = status::active_app(&self.receiver).is_some_and(|a| a["appId"] == DEFAULT_MEDIA_RECEIVER) && self.transport.is_some();
        if ready {
            if let Some(l) = self.pending_load.take() {
                self.load(l.media, l.reply).await?;
            }
        }
        Ok(())
    }

    async fn load(&mut self, media: Value, reply: Reply) -> std::io::Result<()> {
        let t = self.transport.clone().unwrap_or_default();
        let id = self.request(&t, NS_MEDIA, json!({ "type": "LOAD", "media": media, "autoplay": true, "currentTime": 0 })).await?;
        self.loads.insert(id, (Instant::now() + LOAD_TIMEOUT, reply));
        Ok(())
    }

    fn app(&self) -> Option<&Value> {
        status::active_app(&self.receiver)
    }

    /// The media session and its transport, needed for media commands.
    fn media_session(&self) -> Result<(String, u64), Error> {
        let t = self.transport.clone();
        let id = self.media.as_ref().and_then(|m| m["mediaSessionId"].as_u64());
        match (t, id) {
            (Some(t), Some(id)) => Ok((t, id)),
            _ => Err(Error::invalid("der afspilles ikke noget")),
        }
    }

    async fn on_service(&mut self, service: &str, data: &Map<String, Value>, reply: Reply) -> std::io::Result<()> {
        // play_media answers later, when the media has loaded
        if service == "play_media" {
            return self.play_media(data, reply).await;
        }
        let res = self.service(service, data).await;
        match res {
            Ok(r) => {
                let _ = reply.send(r);
                Ok(())
            }
            Err(e) => {
                let _ = reply.send(Err(Error::unavailable(format!("kunne ikke sende til enheden: {e}"))));
                Err(e)
            }
        }
    }

    /// The outer error is a broken connection; the inner one goes to the caller.
    async fn service(&mut self, service: &str, data: &Map<String, Value>) -> std::io::Result<Result<(), Error>> {
        let level = self.receiver.pointer("/volume/level").and_then(Value::as_f64).unwrap_or(0.0);
        let on = self.app().is_some();
        let service = match service {
            "toggle" if on => "turn_off",
            "toggle" => "turn_on",
            "media_play_pause" if self.media.as_ref().is_some_and(|m| m["playerState"] == "PLAYING") => "media_pause",
            "media_play_pause" => "media_play",
            s => s,
        };
        match service {
            "turn_on" => {
                if !on {
                    self.request(RECEIVER, NS_RECEIVER, json!({ "type": "LAUNCH", "appId": DEFAULT_MEDIA_RECEIVER })).await?;
                }
                Ok(Ok(()))
            }
            "turn_off" => {
                if let Some(sid) = self.app().and_then(|a| a["sessionId"].as_str()).map(String::from) {
                    self.request(RECEIVER, NS_RECEIVER, json!({ "type": "STOP", "sessionId": sid })).await?;
                }
                Ok(Ok(()))
            }
            "volume_set" | "volume_up" | "volume_down" => {
                let target = match service {
                    "volume_up" => level + 0.1,
                    "volume_down" => level - 0.1,
                    _ => match data.get("volume_level").and_then(Value::as_f64) {
                        Some(v) => v,
                        None => return Ok(Err(Error::invalid("volume_level mangler"))),
                    },
                };
                let target = (target.clamp(0.0, 1.0) * 1000.0).round() / 1000.0;
                self.request(RECEIVER, NS_RECEIVER, json!({ "type": "SET_VOLUME", "volume": { "level": target } })).await?;
                Ok(Ok(()))
            }
            "volume_mute" => {
                let Some(muted) = data.get("is_volume_muted").and_then(Value::as_bool) else {
                    return Ok(Err(Error::invalid("is_volume_muted mangler")));
                };
                self.request(RECEIVER, NS_RECEIVER, json!({ "type": "SET_VOLUME", "volume": { "muted": muted } })).await?;
                Ok(Ok(()))
            }
            "media_play" | "media_pause" | "media_stop" | "media_seek" | "media_next_track" | "media_previous_track" => {
                let (t, sid) = match self.media_session() {
                    Ok(s) => s,
                    Err(e) => return Ok(Err(e)),
                };
                let msg = match service {
                    "media_play" => json!({ "type": "PLAY" }),
                    "media_pause" => json!({ "type": "PAUSE" }),
                    "media_stop" => json!({ "type": "STOP" }),
                    "media_next_track" => json!({ "type": "QUEUE_UPDATE", "jump": 1 }),
                    "media_previous_track" => json!({ "type": "QUEUE_UPDATE", "jump": -1 }),
                    _ => match data.get("seek_position").and_then(Value::as_f64) {
                        Some(pos) => json!({ "type": "SEEK", "currentTime": pos.max(0.0) }),
                        None => return Ok(Err(Error::invalid("seek_position mangler"))),
                    },
                };
                let mut msg = msg;
                msg["mediaSessionId"] = json!(sid);
                self.request(&t, NS_MEDIA, msg).await?;
                Ok(Ok(()))
            }
            s => Ok(Err(Error::not_supported(format!("media_player.{s}")))),
        }
    }

    async fn play_media(&mut self, data: &Map<String, Value>, reply: Reply) -> std::io::Result<()> {
        let Some(url) = data.get("media_content_id").and_then(Value::as_str).filter(|s| !s.is_empty()) else {
            let _ = reply.send(Err(Error::invalid("media_content_id mangler")));
            return Ok(());
        };
        let kind = data.get("media_content_type").and_then(Value::as_str).unwrap_or("video");
        let extra = data.get("extra").cloned().unwrap_or(Value::Null);
        let mut metadata = json!({ "metadataType": 0 });
        if let Some(t) = extra["title"].as_str() {
            metadata["title"] = json!(t);
        }
        if let Some(img) = extra["thumb"].as_str().or(extra["image"].as_str()) {
            metadata["images"] = json!([{ "url": img }]);
        }
        if let Some(m) = extra["metadata"].as_object() {
            for (k, v) in m {
                metadata[k] = v.clone();
            }
        }
        let stream_type = match extra["stream_type"].as_str() {
            Some(s) if s.eq_ignore_ascii_case("live") => "LIVE",
            _ => "BUFFERED",
        };
        let media = json!({ "contentId": url, "contentType": status::mime_for(url, kind), "streamType": stream_type, "metadata": metadata });

        let ready = self.app().is_some_and(|a| a["appId"] == DEFAULT_MEDIA_RECEIVER) && self.transport.is_some();
        if ready {
            return self.load(media, reply).await;
        }
        if let Some(old) = self.pending_load.take() {
            let _ = old.reply.send(Err(Error::failed("erstattet af et nyere play_media")));
        }
        let launch_request = self.request(RECEIVER, NS_RECEIVER, json!({ "type": "LAUNCH", "appId": DEFAULT_MEDIA_RECEIVER })).await?;
        self.pending_load = Some(PendingLoad { media, reply, deadline: Instant::now() + LOAD_TIMEOUT, launch_request });
        Ok(())
    }
}
