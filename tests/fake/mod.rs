//! A cast device on localhost: TLS with a self-signed certificate and
//! enough of the receiver and media namespaces to play something.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use thehome_google_cast::proto::{self, CastMessage, NS_CONNECTION, NS_HEARTBEAT, NS_MEDIA, NS_RECEIVER, RECEIVER};
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Default)]
struct State {
    level: f64,
    muted: bool,
    /// appId, displayName, sessionId, transportId
    app: Option<(String, String, String, String)>,
    media: Option<Value>,
    launches: u32,
}

pub struct FakeCast {
    pub port: u16,
    state: Arc<Mutex<State>>,
    /// Every message the device got: (destination, namespace, payload).
    pub received: Arc<Mutex<Vec<(String, String, Value)>>>,
    kick: watch::Sender<u32>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeCast {
    fn drop(&mut self) {
        self.task.abort();
        // also end open connections
        self.kick.send_modify(|k| *k += 1);
    }
}

fn receiver_status(s: &State, request_id: Value) -> Value {
    let apps: Vec<Value> = s
        .app
        .iter()
        .map(|(id, name, sid, tid)| {
            json!({ "appId": id, "displayName": name, "sessionId": sid, "transportId": tid, "isIdleScreen": false,
                    "namespaces": [{ "name": NS_MEDIA }] })
        })
        .collect();
    json!({ "type": "RECEIVER_STATUS", "requestId": request_id,
            "status": { "volume": { "level": s.level, "muted": s.muted, "controlType": "attenuation" }, "applications": apps } })
}

fn media_status(s: &State, request_id: Value, with_media: bool) -> Value {
    let status: Vec<Value> = s
        .media
        .iter()
        .map(|m| {
            let mut m = m.clone();
            if !with_media {
                m.as_object_mut().unwrap().remove("media");
            }
            m
        })
        .collect();
    json!({ "type": "MEDIA_STATUS", "requestId": request_id, "status": status })
}

impl FakeCast {
    pub async fn start() -> Self {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = ck.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(ck.key_pair.serialize_der().into());
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(State { level: 0.5, ..Default::default() }));
        let received: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
        let (kick, kick_rx) = watch::channel(0u32);
        let (st, rec) = (state.clone(), received.clone());
        let task = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else { break };
                let Ok(tls) = acceptor.accept(tcp).await else { continue };
                tokio::spawn(serve(tls, st.clone(), rec.clone(), kick_rx.clone()));
            }
        });
        Self { port, state, received, kick, task }
    }

    /// Close every open connection, like a device that rebooted.
    pub fn drop_connections(&self) {
        self.kick.send_modify(|k| *k += 1);
    }

    pub fn launches(&self) -> u32 {
        self.state.lock().unwrap().launches
    }

    pub fn sent(&self, ty: &str) -> Vec<Value> {
        self.received.lock().unwrap().iter().filter(|(_, _, p)| p["type"] == ty).map(|(_, _, p)| p.clone()).collect()
    }
}

async fn serve(
    tls: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    state: Arc<Mutex<State>>,
    received: Arc<Mutex<Vec<(String, String, Value)>>>,
    mut kick: watch::Receiver<u32>,
) {
    kick.borrow_and_update();
    let (mut r, mut w) = tokio::io::split(tls);
    loop {
        let m = tokio::select! {
            m = proto::read_message(&mut r) => m,
            _ = kick.changed() => return,
        };
        let Ok(m) = m else { return };
        let Some(p) = m.payload() else { continue };
        received.lock().unwrap().push((m.destination_id.clone(), m.namespace.clone(), p.clone()));
        let rid = p["requestId"].clone();
        let ty = p["type"].as_str().unwrap_or_default().to_string();
        let mut replies: Vec<(String, &str, Value)> = vec![];
        {
            let mut s = state.lock().unwrap();
            let transport = s.app.as_ref().map(|a| a.3.clone()).unwrap_or_default();
            match (m.namespace.as_str(), ty.as_str()) {
                (NS_HEARTBEAT, "PING") => replies.push((m.destination_id.clone(), NS_HEARTBEAT, json!({ "type": "PONG" }))),
                (NS_RECEIVER, "GET_STATUS") => replies.push((RECEIVER.into(), NS_RECEIVER, receiver_status(&s, rid))),
                (NS_RECEIVER, "SET_VOLUME") => {
                    if let Some(l) = p["volume"]["level"].as_f64() {
                        s.level = l;
                    }
                    if let Some(m) = p["volume"]["muted"].as_bool() {
                        s.muted = m;
                    }
                    replies.push((RECEIVER.into(), NS_RECEIVER, receiver_status(&s, rid)));
                }
                (NS_RECEIVER, "LAUNCH") => {
                    s.launches += 1;
                    let n = s.launches;
                    let id = p["appId"].as_str().unwrap_or_default().to_string();
                    s.app = Some((id, "Default Media Receiver".into(), format!("session-{n}"), format!("web-{n}")));
                    s.media = None;
                    replies.push((RECEIVER.into(), NS_RECEIVER, receiver_status(&s, rid)));
                }
                (NS_RECEIVER, "STOP") => {
                    s.app = None;
                    s.media = None;
                    replies.push((RECEIVER.into(), NS_RECEIVER, receiver_status(&s, rid)));
                }
                (NS_MEDIA, "GET_STATUS") => replies.push((transport.clone(), NS_MEDIA, media_status(&s, rid, true))),
                (NS_MEDIA, "LOAD") => {
                    if p["media"]["contentId"].as_str().unwrap_or_default().contains("fail") {
                        replies.push((transport.clone(), NS_MEDIA, json!({ "type": "LOAD_FAILED", "requestId": rid })));
                    } else {
                        s.media = Some(json!({ "mediaSessionId": 7, "playerState": "PLAYING", "currentTime": 0.0,
                                               "supportedMediaCommands": 15, "media": p["media"] }));
                        replies.push((transport.clone(), NS_MEDIA, media_status(&s, rid, true)));
                    }
                }
                (NS_MEDIA, "PLAY" | "PAUSE" | "SEEK" | "STOP") => {
                    if let Some(media) = s.media.as_mut() {
                        match ty.as_str() {
                            "PLAY" => media["playerState"] = json!("PLAYING"),
                            "PAUSE" => media["playerState"] = json!("PAUSED"),
                            "SEEK" => media["currentTime"] = p["currentTime"].clone(),
                            _ => media["playerState"] = json!("IDLE"),
                        }
                    }
                    // updates leave out the media, like real devices
                    replies.push((transport.clone(), NS_MEDIA, media_status(&s, rid, false)));
                }
                (NS_CONNECTION, _) => {}
                _ => {}
            }
        }
        for (src, ns, payload) in replies {
            let msg = CastMessage { source_id: src, destination_id: "sender-0".into(), ..CastMessage::json("", "", ns, &payload) };
            if proto::write_message(&mut w, &msg).await.is_err() {
                return;
            }
        }
    }
}
