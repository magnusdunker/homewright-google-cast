//! Google Cast: Chromecasts, Google and Nest speakers and displays, Android
//! TVs and cast groups as media players in TheHome. An external integration.
//!
//! One setup covers every cast device on the network. Devices are found
//! with mDNS while the setup runs, plus any addresses the user typed for
//! devices mDNS cannot reach. The addresses found are remembered, so a
//! device that does not answer mDNS after a restart is still connected to.
//! Each device gets a persistent Cast v2
//! connection that pushes its state to TheHome when it changes.

pub mod conn;
pub mod device;
pub mod discovery;
pub mod proto;
pub mod status;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thehome_plugin::{async_trait, json, Device, Entity, Entry, Error, Flow, FlowReply, Host, Integration, Map, Value};

use device::DeviceHandle;
use discovery::CastInfo;

pub const DOMAIN: &str = "google_cast";
const SCAN: Duration = Duration::from_secs(4);
/// How often an address the user typed is tried while the device is away.
const MANUAL_RETRY: Duration = Duration::from_secs(30);
/// How often mDNS is asked again.
const REBROWSE: Duration = Duration::from_secs(300);

/// Where a device was found.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Source {
    Mdns,
    /// An address the user typed.
    Manual,
    /// The address mDNS gave last time.
    Remembered,
}

/// Logging to TheHome's log through stderr.
#[doc(hidden)]
pub fn emit(level: &str, message: String) {
    thehome_plugin::log(level, message);
}

#[macro_export]
macro_rules! info {
    ($($t:tt)*) => { $crate::emit("INFO", format!($($t)*)) };
}

#[macro_export]
macro_rules! warn {
    ($($t:tt)*) => { $crate::emit("WARN", format!($($t)*)) };
}

fn device_title(c: &CastInfo) -> String {
    match &c.model {
        Some(m) if *m != c.name => format!("{} ({m}, {})", c.name, c.host),
        _ => format!("{} ({})", c.name, c.host),
    }
}

fn schema(found: &[CastInfo], selected: &[String], known_hosts: &str) -> Value {
    let mut props = Map::new();
    if !found.is_empty() {
        let options: Vec<Value> = found.iter().map(|c| json!({ "const": c.uuid, "title": device_title(c) })).collect();
        props.insert(
            "devices".into(),
            json!({
                "type": "array", "title": "Enheder", "items": { "type": "string", "oneOf": options }, "default": selected,
                "description": "Med alle valgt kommer nye enheder også med, når de dukker op"
            }),
        );
    }
    props.insert(
        "known_hosts".into(),
        json!({
            "type": "string", "title": "Ekstra adresser", "default": known_hosts,
            "description": "IP-adresser eller værtsnavne adskilt af komma, til enheder som mDNS ikke finder, f.eks. på et andet VLAN"
        }),
    );
    json!({ "type": "object", "properties": props })
}

/// Found devices kept in the flow's data between steps.
fn store(found: &[CastInfo]) -> Value {
    found.iter().map(|c| json!({ "uuid": c.uuid, "name": c.name, "model": c.model, "manufacturer": c.manufacturer, "host": c.host, "port": c.port })).collect()
}

fn stored(data: &Map<String, Value>) -> Vec<CastInfo> {
    let Some(list) = data.get("found").and_then(Value::as_array) else { return vec![] };
    list.iter()
        .filter_map(|c| {
            Some(CastInfo {
                uuid: c["uuid"].as_str()?.into(),
                name: c["name"].as_str()?.into(),
                model: c["model"].as_str().map(String::from),
                manufacturer: c["manufacturer"].as_str().map(String::from),
                host: c["host"].as_str().unwrap_or_default().into(),
                port: c["port"].as_u64().and_then(|p| u16::try_from(p).ok()).unwrap_or(8009),
            })
        })
        .collect()
}

fn with_description(mut r: FlowReply, text: &str) -> FlowReply {
    r.result["description"] = json!(text);
    r
}

fn str_list(v: &Value) -> Vec<String> {
    match v {
        Value::Array(a) => a.iter().filter_map(Value::as_str).map(String::from).collect(),
        Value::String(s) => s.split([',', ' ', '\n']).map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect(),
        _ => vec![],
    }
}

/// The addresses in the form, or the first one that is not an address.
fn parse_hosts(v: &Value) -> Result<Vec<String>, String> {
    let list = str_list(v);
    for h in &list {
        if conn::parse_host(h).is_none() {
            return Err(h.clone());
        }
    }
    Ok(list)
}

/// Check the form and turn it into entry data, or show it again with errors.
#[allow(clippy::result_large_err)]
async fn submit(input: &Value, found_ids: &[String], extra: Map<String, Value>, show: impl Fn(Value) -> FlowReply) -> Result<Map<String, Value>, FlowReply> {
    let hosts = match parse_hosts(&input["known_hosts"]) {
        Ok(h) => h,
        Err(h) => {
            info!("ugyldig adresse: {h}");
            return Err(show(json!({ "known_hosts": "invalid_host" })));
        }
    };
    for h in &hosts {
        let (host, port) = conn::parse_host(h).unwrap_or_default();
        if let Err(e) = conn::connect(&host, port).await {
            info!("{h}: {e}");
            return Err(show(json!({ "base": "cannot_connect" })));
        }
    }
    let mut data = extra;
    data.insert("known_hosts".into(), json!(hosts));
    let chosen: Vec<String> = str_list(&input["devices"]).into_iter().filter(|d| found_ids.contains(d)).collect();
    // all of them means new devices too, so nothing is stored
    if !found_ids.is_empty() && !found_ids.iter().all(|d| chosen.contains(d)) {
        data.insert("devices".into(), json!(chosen));
    }
    Ok(data)
}

#[derive(Default)]
pub struct GoogleCast {
    entries: Mutex<HashMap<String, Arc<Runtime>>>,
}

impl GoogleCast {
    fn runtime(&self, entry_id: &str) -> Result<Arc<Runtime>, Error> {
        self.entries.lock().unwrap().get(entry_id).cloned().ok_or_else(|| Error::unavailable("opsætningen kører ikke"))
    }

    /// Devices to offer in a form: a fresh scan plus what the setup knows.
    async fn found(&self, flow: &Flow) -> Vec<CastInfo> {
        let mut found = if flow.data.get("scan") == Some(&json!(false)) { vec![] } else { discovery::scan(SCAN).await };
        let known = flow.entry.as_ref().and_then(|e| self.entries.lock().unwrap().get(&e.entry_id).cloned());
        if let Some(rt) = known {
            for c in rt.mdns_devices().await {
                if !found.iter().any(|f| f.uuid == c.uuid) {
                    found.push(c);
                }
            }
        }
        // devices put in flow.data beforehand, as tests do
        for c in stored(&flow.data) {
            if !found.iter().any(|f| f.uuid == c.uuid) {
                found.push(c);
            }
        }
        found
    }
}

#[async_trait]
impl Integration for GoogleCast {
    async fn config_flow(&self, _host: &Host, flow: Flow, step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        match (step, input) {
            ("zeroconf", None) => {
                let d = flow.discovery.clone().unwrap_or(Value::Null);
                let name = d.pointer("/properties/fn").and_then(Value::as_str).or(d["name"].as_str()).unwrap_or("en Cast-enhed").to_string();
                let r = FlowReply::form("confirm", json!({ "type": "object", "properties": {} })).with_unique_id(DOMAIN);
                Ok(with_description(r, &format!("Fandt {name}. Alle Cast-enheder på netværket bliver tilføjet, også dem der dukker op senere.")))
            }
            ("confirm", Some(_)) => Ok(FlowReply::create("Google Cast", json!({})).with_unique_id(DOMAIN)),
            ("user", None) => {
                let found = self.found(&flow).await;
                let ids: Vec<String> = found.iter().map(|c| c.uuid.clone()).collect();
                let mut data = flow.data.clone();
                data.insert("found".into(), store(&found));
                let r = FlowReply::form("user", schema(&found, &ids, "")).with_data(data).with_unique_id(DOMAIN);
                Ok(if found.is_empty() {
                    with_description(r, "Ingen Cast-enheder fundet på netværket lige nu. Enheder, der dukker op senere, bliver tilføjet.")
                } else {
                    r
                })
            }
            ("user", Some(v)) => {
                let found = stored(&flow.data);
                let ids: Vec<String> = found.iter().map(|c| c.uuid.clone()).collect();
                let mut extra = Map::new();
                if flow.data.get("mdns") == Some(&json!(false)) {
                    extra.insert("mdns".into(), json!(false));
                }
                let typed = v["known_hosts"].as_str().unwrap_or_default().to_string();
                let chosen = str_list(&v["devices"]);
                let show = |errors| FlowReply::form_with_errors("user", schema(&found, &chosen, &typed), errors);
                match submit(&v, &ids, extra, show).await {
                    Ok(data) => Ok(FlowReply::create("Google Cast", Value::Object(data)).with_unique_id(DOMAIN)),
                    Err(form) => Ok(form),
                }
            }
            _ => Ok(FlowReply::abort("unknown_step")),
        }
    }

    async fn options_flow(&self, _host: &Host, flow: Flow, _step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        let Some(entry) = flow.entry.clone() else { return Ok(FlowReply::abort("unknown")) };
        let hosts = str_list(entry.get("known_hosts").unwrap_or(&Value::Null)).join(", ");
        match input {
            None => {
                let found = self.found(&flow).await;
                let ids: Vec<String> = found.iter().map(|c| c.uuid.clone()).collect();
                let current = entry.get("devices").map(str_list).unwrap_or_else(|| ids.clone());
                let mut data = flow.data.clone();
                data.insert("found".into(), store(&found));
                Ok(FlowReply::form("init", schema(&found, &current, &hosts)).with_data(data))
            }
            Some(v) => {
                let found = stored(&flow.data);
                let ids: Vec<String> = found.iter().map(|c| c.uuid.clone()).collect();
                let typed = v["known_hosts"].as_str().unwrap_or_default().to_string();
                let chosen = str_list(&v["devices"]);
                let show = |errors| FlowReply::form_with_errors("init", schema(&found, &chosen, &typed), errors);
                match submit(&v, &ids, Map::new(), show).await {
                    Ok(data) => Ok(FlowReply::create("", Value::Object(data))),
                    Err(form) => Ok(form),
                }
            }
        }
    }

    async fn setup_entry(&self, host: &Host, entry: Entry) -> Result<(), Error> {
        let rt = Runtime::start(host.clone(), &entry);
        if let Some(old) = self.entries.lock().unwrap().insert(entry.entry_id.clone(), rt) {
            old.stop();
        }
        Ok(())
    }

    async fn unload_entry(&self, _host: &Host, entry_id: &str) -> Result<(), Error> {
        if let Some(rt) = self.entries.lock().unwrap().remove(entry_id) {
            rt.stop();
        }
        Ok(())
    }

    async fn entity_service(&self, _host: &Host, entry_id: &str, entity_id: &str, service: &str, data: Map<String, Value>) -> Result<(), Error> {
        let rt = self.runtime(entry_id)?;
        rt.call(entity_id, service, data).await
    }

    async fn diagnostics(&self, _host: &Host, entry: Entry) -> Value {
        let mut d = json!({ "data": entry.data, "options": entry.options });
        if let Ok(rt) = self.runtime(&entry.entry_id) {
            d["devices"] = Value::Array(rt.diagnostics().await);
        }
        d
    }
}

struct Known {
    info: CastInfo,
    manual: bool,
    entity_id: Option<String>,
    handle: Option<DeviceHandle>,
}

/// One running setup.
struct Runtime {
    host: Host,
    entry_id: String,
    /// Chosen devices; `None` is all of them.
    selection: Option<Vec<String>>,
    manual: Vec<(String, u16)>,
    devices: tokio::sync::Mutex<HashMap<String, Known>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    daemon: Mutex<Option<mdns_sd::ServiceDaemon>>,
}

impl Runtime {
    fn start(host: Host, entry: &Entry) -> Arc<Self> {
        let selection = entry.get("devices").map(str_list);
        let manual: Vec<(String, u16)> = str_list(entry.get("known_hosts").unwrap_or(&Value::Null)).iter().filter_map(|h| conn::parse_host(h)).collect();
        let rt = Arc::new(Self {
            host,
            entry_id: entry.entry_id.clone(),
            selection,
            manual: manual.clone(),
            devices: Default::default(),
            tasks: Default::default(),
            daemon: Default::default(),
        });

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(CastInfo, Source)>();
        // first, so mDNS finds them known and only moves them if they moved
        for c in remembered(&rt.memory_file()) {
            let _ = tx.send((c, Source::Remembered));
        }
        if entry.get("mdns") != Some(&json!(false)) {
            let (mtx, mut mrx) = tokio::sync::mpsc::unbounded_channel();
            *rt.daemon.lock().unwrap() = discovery::browse(mtx.clone());
            let tx = tx.clone();
            rt.tasks.lock().unwrap().push(tokio::spawn(async move {
                while let Some(c) = mrx.recv().await {
                    if tx.send((c, Source::Mdns)).is_err() {
                        break;
                    }
                }
            }));
            let me = rt.clone();
            rt.tasks.lock().unwrap().push(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(REBROWSE).await;
                    if let Some(d) = me.daemon.lock().unwrap().as_ref() {
                        discovery::rebrowse(d, mtx.clone());
                    }
                }
            }));
        }
        for (h, p) in manual {
            let tx = tx.clone();
            rt.tasks.lock().unwrap().push(tokio::spawn(async move {
                loop {
                    if let Some(c) = probe(&h, p).await {
                        let _ = tx.send((c, Source::Manual));
                        break;
                    }
                    tokio::time::sleep(MANUAL_RETRY).await;
                }
            }));
        }
        drop(tx);
        let me = rt.clone();
        rt.tasks.lock().unwrap().push(tokio::spawn(async move {
            while let Some((c, source)) = rx.recv().await {
                me.on_found(c, source).await;
            }
        }));
        rt
    }

    fn stop(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        if let Some(d) = self.daemon.lock().unwrap().take() {
            let _ = d.shutdown();
        }
        // the handles stop their connections when dropped
        if let Ok(mut d) = self.devices.try_lock() {
            d.clear();
        }
    }

    fn memory_file(&self) -> std::path::PathBuf {
        self.host.data_dir().join(format!("devices-{}.json", self.entry_id))
    }

    /// Write down where the devices mDNS found are.
    fn remember(&self, devices: &HashMap<String, Known>) {
        let mut found: Vec<CastInfo> = devices.values().filter(|k| !k.manual).map(|k| k.info.clone()).collect();
        found.sort_by(|a, b| a.uuid.cmp(&b.uuid));
        let path = self.memory_file();
        let write = || -> std::io::Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(&path, serde_json::to_vec_pretty(&store(&found))?)
        };
        if let Err(e) = write() {
            warn!("kan ikke gemme enhedernes adresser i {}: {e}", path.display());
        }
    }

    async fn on_found(&self, mut c: CastInfo, source: Source) {
        let manual = source == Source::Manual;
        if !manual {
            // an address the user typed wins over mDNS for the same device
            if self.manual.iter().any(|(h, p)| *h == c.host && *p == c.port) {
                return;
            }
            if self.selection.as_ref().is_some_and(|s| !s.contains(&c.uuid)) {
                return;
            }
        }
        let mut devices = self.devices.lock().await;
        if let Some(k) = devices.get_mut(&c.uuid) {
            if (k.manual && !manual) || source == Source::Remembered {
                return;
            }
            if !replaces(&k.info.host, &c.host) {
                return;
            }
            if (k.info.host.as_str(), k.info.port) != (c.host.as_str(), c.port) {
                info!("{}: ny adresse {}:{}", k.info.name, c.host, c.port);
                k.info.host = c.host.clone();
                k.info.port = c.port;
                if let Some(h) = &k.handle {
                    h.set_addr((c.host, c.port));
                }
                self.remember(&devices);
            }
            return;
        }
        if source == Source::Mdns {
            if let Some((_, v)) = discovery::eureka(&c.host).await {
                let name = c.name.clone();
                discovery::apply_eureka(&mut c, &v);
                // the name from mDNS is the one the user set in Google Home
                c.name = name;
            }
        }
        let how = if source == Source::Remembered { "husker" } else { "fandt" };
        info!("{how} {} ({}) på {}:{}", c.name, c.model.as_deref().unwrap_or("ukendt model"), c.host, c.port);
        let dev = Device {
            identifiers: vec![(DOMAIN.into(), c.uuid.clone())],
            name: c.name.clone(),
            manufacturer: c.manufacturer.clone(),
            model: c.model.clone(),
            ..Default::default()
        };
        let mut known = Known { info: c.clone(), manual, entity_id: None, handle: None };
        match self.host.register_device(&self.entry_id, dev).await {
            Ok(dev_id) => match self.host.add_entity(&self.entry_id, Entity::new("media_player", c.uuid.clone()).device(&dev_id), true).await {
                Ok(Some(entity_id)) => {
                    known.handle = Some(DeviceHandle::spawn(self.host.clone(), entity_id.clone(), c.name.clone(), (c.host.clone(), c.port)));
                    known.entity_id = Some(entity_id);
                }
                Ok(None) => {}
                Err(e) => warn!("{}: {e}", c.name),
            },
            Err(e) => warn!("{}: {e}", c.name),
        }
        devices.insert(c.uuid.clone(), known);
        if source == Source::Mdns {
            self.remember(&devices);
        }
    }

    async fn call(&self, entity_id: &str, service: &str, data: Map<String, Value>) -> Result<(), Error> {
        let devices = self.devices.lock().await;
        let k = devices
            .values()
            .find(|k| k.entity_id.as_deref() == Some(entity_id))
            .ok_or_else(|| Error::new("not_found", format!("{entity_id} findes ikke")))?;
        let caller = k.handle.as_ref().ok_or_else(|| Error::unavailable(format!("{entity_id} er slået fra")))?.caller();
        // don't hold the lock while the device answers
        drop(devices);
        caller.call(service, data).await
    }

    async fn mdns_devices(&self) -> Vec<CastInfo> {
        self.devices.lock().await.values().filter(|k| !k.manual).map(|k| k.info.clone()).collect()
    }

    async fn diagnostics(&self) -> Vec<Value> {
        let devices: Vec<(CastInfo, bool, Option<String>, Option<device::Caller>)> = self
            .devices
            .lock()
            .await
            .values()
            .map(|k| (k.info.clone(), k.manual, k.entity_id.clone(), k.handle.as_ref().map(DeviceHandle::caller)))
            .collect();
        let mut out = vec![];
        for (info, manual, entity_id, caller) in devices {
            let status = match caller {
                Some(c) => c.snapshot().await,
                None => Value::Null,
            };
            out.push(json!({
                "uuid": info.uuid, "name": info.name, "model": info.model, "manufacturer": info.manufacturer,
                "host": info.host, "port": info.port, "manual": manual, "entity_id": entity_id, "status": status,
            }));
        }
        out
    }
}

/// The devices written down last time.
fn remembered(path: &std::path::Path) -> Vec<CastInfo> {
    let Ok(bytes) = std::fs::read(path) else { return vec![] };
    let list: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    stored(&Map::from_iter([("found".to_string(), list)]))
}

/// Whether a newly seen address should replace the current one. mDNS can
/// resolve the AAAA record before the A record, so IPv6 never replaces IPv4.
fn replaces(current: &str, new: &str) -> bool {
    let is_v6 = |h: &str| h.parse::<std::net::IpAddr>().is_ok_and(|a| a.is_ipv6());
    !(is_v6(new) && !is_v6(current))
}

/// A device at an address the user typed: its identity from eureka_info,
/// or from the address when that page is not there but Cast answers.
async fn probe(host: &str, port: u16) -> Option<CastInfo> {
    let mut c = CastInfo { uuid: String::new(), name: host.to_string(), model: None, manufacturer: None, host: host.to_string(), port };
    if let Some((uuid, v)) = discovery::eureka(host).await {
        discovery::apply_eureka(&mut c, &v);
        if let Some(u) = uuid {
            c.uuid = u;
            return Some(c);
        }
    }
    match conn::connect(host, port).await {
        Ok(_) => {
            c.uuid = format!("host_{}_{port}", host.to_lowercase().replace(|ch: char| !ch.is_ascii_alphanumeric(), "_"));
            Some(c)
        }
        Err(e) => {
            info!("{host}:{port}: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_wins() {
        assert!(replaces("2a09::1", "192.168.0.164"));
        assert!(replaces("192.168.0.164", "192.168.0.165"));
        assert!(!replaces("192.168.0.164", "2a09::1"));
        assert!(replaces("fe80::1", "2a09::1"));
    }

    #[test]
    fn form_values() {
        assert_eq!(str_list(&json!("10.0.0.1, tv.lan:8010\n10.0.0.2")), ["10.0.0.1", "tv.lan:8010", "10.0.0.2"]);
        assert_eq!(str_list(&json!(["a", 1, "b"])), ["a", "b"]);
        assert_eq!(parse_hosts(&json!("10.0.0.1, x:y")), Err("x:y".to_string()));
        let found = vec![CastInfo { uuid: "u".into(), name: "Stue".into(), model: Some("Chromecast".into()), manufacturer: None, host: "10.0.0.9".into(), port: 8009 }];
        assert_eq!(stored(&Map::from_iter([("found".to_string(), store(&found))])), found);
    }
}
