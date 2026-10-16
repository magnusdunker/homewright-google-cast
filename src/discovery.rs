//! Finding cast devices: mDNS (`_googlecast._tcp`) and, for addresses the
//! user typed, the device's own `eureka_info` web page.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use serde_json::Value;

pub const SERVICE: &str = "_googlecast._tcp.local.";

#[derive(Clone, Debug, PartialEq)]
pub struct CastInfo {
    /// The cast UUID as 32 lowercase hex digits.
    pub uuid: String,
    pub name: String,
    pub model: Option<String>,
    pub manufacturer: Option<String>,
    pub host: String,
    pub port: u16,
}

/// The UUID in the form mDNS uses: lowercase hex without dashes.
pub fn normalize_uuid(s: &str) -> Option<String> {
    let hex: String = s.chars().filter(|c| *c != '-').collect::<String>().to_lowercase();
    (hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit())).then_some(hex)
}

/// A device from an mDNS record: TXT `id`, `fn` (name) and `md` (model).
pub fn from_txt(props: &HashMap<String, String>, addresses: &[IpAddr], port: u16) -> Option<CastInfo> {
    let get = |k: &str| props.iter().find(|(key, _)| key.eq_ignore_ascii_case(k)).map(|(_, v)| v.trim().to_string()).filter(|v| !v.is_empty());
    let uuid = normalize_uuid(&get("id")?)?;
    // IPv4 first, which every cast device has
    let host = addresses.iter().find(|a| a.is_ipv4()).or(addresses.first())?.to_string();
    let model = get("md");
    Some(CastInfo {
        name: get("fn").or_else(|| model.clone()).unwrap_or_else(|| host.clone()),
        manufacturer: None,
        model,
        uuid,
        host,
        port: if port == 0 { 8009 } else { port },
    })
}

/// Browse mDNS until `found` gets dropped. Each resolved record is sent once
/// per change of address.
pub fn browse(found: tokio::sync::mpsc::UnboundedSender<CastInfo>) -> Option<mdns_sd::ServiceDaemon> {
    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            crate::warn!("mDNS kan ikke starte: {e}");
            return None;
        }
    };
    let rx = match daemon.browse(SERVICE) {
        Ok(rx) => rx,
        Err(e) => {
            crate::warn!("mDNS-søgning fejlede: {e}");
            let _ = daemon.shutdown();
            return None;
        }
    };
    forward(rx, found);
    Some(daemon)
}

/// Ask again: a new query, and the records mDNS already has come again.
/// Devices that missed the first query, or answer only now and then, turn
/// up this way.
pub fn rebrowse(daemon: &mdns_sd::ServiceDaemon, found: tokio::sync::mpsc::UnboundedSender<CastInfo>) {
    let _ = daemon.stop_browse(SERVICE);
    match daemon.browse(SERVICE) {
        Ok(rx) => forward(rx, found),
        Err(e) => crate::warn!("mDNS-søgning fejlede: {e}"),
    }
}

fn forward(rx: mdns_sd::Receiver<mdns_sd::ServiceEvent>, found: tokio::sync::mpsc::UnboundedSender<CastInfo>) {
    tokio::spawn(async move {
        while let Ok(ev) = rx.recv_async().await {
            match ev {
                mdns_sd::ServiceEvent::ServiceResolved(info) => {
                    if let Some(c) = from_service(&info) {
                        if found.send(c).is_err() {
                            break;
                        }
                    }
                }
                mdns_sd::ServiceEvent::SearchStopped(_) => break,
                _ => {}
            }
        }
    });
}

fn from_service(info: &mdns_sd::ServiceInfo) -> Option<CastInfo> {
    let props: HashMap<String, String> = info.get_properties().iter().map(|p| (p.key().to_string(), p.val_str().to_string())).collect();
    let mut addrs: Vec<IpAddr> = info.get_addresses().iter().copied().collect();
    addrs.sort();
    from_txt(&props, &addrs, info.get_port())
}

/// Look for devices for a while, for the setup form.
pub async fn scan(duration: Duration) -> Vec<CastInfo> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let Some(daemon) = browse(tx) else { return vec![] };
    let mut found: Vec<CastInfo> = vec![];
    let deadline = tokio::time::Instant::now() + duration;
    while let Ok(Some(c)) = tokio::time::timeout_at(deadline, rx.recv()).await {
        found.retain(|f| f.uuid != c.uuid);
        found.push(c);
    }
    let _ = daemon.shutdown();
    found.sort_by_key(|a| a.name.to_lowercase());
    found
}

/// Name, UUID, maker and model from `/setup/eureka_info`. Newer devices
/// answer on HTTPS port 8443 with a self-signed certificate, older ones on 8008.
pub async fn eureka(host: &str) -> Option<(Option<String>, Value)> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let h = if host.contains(':') { format!("[{host}]") } else { host.to_string() };
    for url in [format!("https://{h}:8443/setup/eureka_info?params=name,device_info"), format!("http://{h}:8008/setup/eureka_info?params=name,device_info")] {
        let Ok(r) = client.get(&url).send().await else { continue };
        let Ok(r) = r.error_for_status() else { continue };
        let Ok(v) = r.json::<Value>().await else { continue };
        let udn = v["ssdp_udn"].as_str().or(v.pointer("/device_info/ssdp_udn").and_then(Value::as_str)).and_then(normalize_uuid);
        return Some((udn, v));
    }
    None
}

/// Fill in what eureka_info knows.
pub fn apply_eureka(info: &mut CastInfo, v: &Value) {
    if let Some(n) = v["name"].as_str().filter(|s| !s.is_empty()) {
        info.name = n.into();
    }
    if let Some(m) = v.pointer("/device_info/manufacturer").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        info.manufacturer = Some(m.into());
    }
    if let Some(m) = v.pointer("/device_info/model_name").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        info.model = Some(m.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn props(p: &[(&str, &str)]) -> HashMap<String, String> {
        p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn txt_record() {
        let p = props(&[("id", "0123456789ABCDEF0123456789abcdef"), ("fn", "Stue"), ("md", "Google Nest Mini"), ("rs", "")]);
        let addrs: Vec<IpAddr> = vec!["fe80::1".parse().unwrap(), "192.168.1.20".parse().unwrap()];
        let c = from_txt(&p, &addrs, 8009).unwrap();
        assert_eq!(c.uuid, "0123456789abcdef0123456789abcdef");
        assert_eq!(c.name, "Stue");
        assert_eq!(c.model.as_deref(), Some("Google Nest Mini"));
        assert_eq!(c.host, "192.168.1.20");
        assert_eq!(c.port, 8009);
    }

    #[test]
    fn group_and_missing_fields() {
        let p = props(&[("id", "01234567-89ab-cdef-0123-456789abcdef"), ("md", "Google Cast Group")]);
        let c = from_txt(&p, &["10.0.0.2".parse().unwrap()], 32187).unwrap();
        assert_eq!(c.name, "Google Cast Group");
        assert_eq!(c.port, 32187);
        assert!(from_txt(&props(&[("fn", "x")]), &["10.0.0.2".parse().unwrap()], 8009).is_none());
        assert!(from_txt(&props(&[("id", "0123456789abcdef0123456789abcdef")]), &[], 8009).is_none());
    }

    #[test]
    fn eureka_fields() {
        let mut c = CastInfo { uuid: "x".into(), name: "10.0.0.5".into(), model: None, manufacturer: None, host: "10.0.0.5".into(), port: 8009 };
        apply_eureka(&mut c, &json!({ "name": "Køkken", "device_info": { "manufacturer": "Google Inc.", "model_name": "Chromecast" } }));
        assert_eq!(c.name, "Køkken");
        assert_eq!(c.manufacturer.as_deref(), Some("Google Inc."));
        assert_eq!(c.model.as_deref(), Some("Chromecast"));
        assert_eq!(normalize_uuid("01234567-89AB-cdef-0123-456789abcdef").as_deref(), Some("0123456789abcdef0123456789abcdef"));
        assert_eq!(normalize_uuid("nope"), None);
    }
}
