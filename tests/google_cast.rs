//! The plugin driven over a pipe the way TheHome drives it, against a fake
//! cast device on localhost.

mod fake;
mod host;

use std::time::Duration;

use fake::FakeCast;
use host::FakeHost;
use serde_json::{json, Value};
use thehome_google_cast::GoogleCast;

async fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..200 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("ventede forgæves på {what}");
}

fn flow(source: &str, data: Value) -> Value {
    json!({ "flow_id": "f1", "source": source, "data": data })
}

async fn start() -> FakeHost {
    let host = FakeHost::start(GoogleCast::default());
    let dir = std::env::temp_dir().join(format!("google-cast-test-{}", std::process::id()));
    host.call("initialize", json!({ "api_version": 1, "data_dir": dir })).await.unwrap();
    host
}

#[tokio::test]
async fn user_flow_checks_addresses_and_keeps_field_order() {
    let cast = FakeCast::start().await;
    let host = start().await;
    let data = json!({ "scan": false, "mdns": false,
                       "found": [{ "uuid": "aa".repeat(16), "name": "Stue", "host": "10.0.0.20" },
                                 { "uuid": "bb".repeat(16), "name": "Køkken", "host": "10.0.0.21", "model": "Google Nest Mini" }] });
    let r = host.call("config_flow", json!({ "flow": flow("user", data), "step": "user" })).await.unwrap();
    assert_eq!(r["result"]["type"], "form");
    assert_eq!(r["unique_id"], "google_cast");
    let props: Vec<&String> = r["result"]["schema"]["properties"].as_object().unwrap().keys().collect();
    assert_eq!(props, ["devices", "known_hosts"]);
    let options = &r["result"]["schema"]["properties"]["devices"]["items"]["oneOf"];
    assert_eq!(options[1]["title"], "Køkken (Google Nest Mini, 10.0.0.21)");
    assert_eq!(r["result"]["schema"]["properties"]["devices"]["default"], json!(["aa".repeat(16), "bb".repeat(16)]));
    let data = r["data"].clone();

    let step = |input: Value| json!({ "flow": flow("user", data.clone()), "step": "user", "input": input });
    let r = host.call("config_flow", step(json!({ "known_hosts": "ikke en:adresse", "devices": [] }))).await.unwrap();
    assert_eq!(r["result"]["errors"]["known_hosts"], "invalid_host");
    // nothing listens on port 1
    let r = host.call("config_flow", step(json!({ "known_hosts": "127.0.0.1:1" }))).await.unwrap();
    assert_eq!(r["result"]["errors"]["base"], "cannot_connect");

    // all devices: nothing stored, so new devices come along too
    let addr = format!("127.0.0.1:{}", cast.port);
    let r = host.call("config_flow", step(json!({ "known_hosts": addr, "devices": ["aa".repeat(16), "bb".repeat(16)] }))).await.unwrap();
    assert_eq!(r["result"]["type"], "create_entry");
    assert_eq!(r["result"]["title"], "Google Cast");
    assert_eq!(r["result"]["data"], json!({ "mdns": false, "known_hosts": [addr] }));
    // one device chosen
    let r = host.call("config_flow", step(json!({ "known_hosts": "", "devices": ["bb".repeat(16)] }))).await.unwrap();
    assert_eq!(r["result"]["data"], json!({ "mdns": false, "known_hosts": [], "devices": ["bb".repeat(16)] }));
}

#[tokio::test]
async fn zeroconf_flow_asks_once() {
    let host = start().await;
    let disc = json!({ "name": "Google-Nest-Mini-0123", "host": "x.local", "port": 8009, "properties": { "fn": "Soveværelse" } });
    let f = json!({ "flow_id": "f2", "source": "zeroconf", "discovery": disc, "data": {} });
    let r = host.call("config_flow", json!({ "flow": f, "step": "zeroconf" })).await.unwrap();
    assert_eq!(r["result"]["step_id"], "confirm");
    assert_eq!(r["unique_id"], "google_cast");
    assert!(r["result"]["description"].as_str().unwrap().contains("Soveværelse"));
    let r = host.call("config_flow", json!({ "flow": f, "step": "confirm", "input": {} })).await.unwrap();
    assert_eq!(r["result"]["type"], "create_entry");
    assert_eq!(r["result"]["data"], json!({}));
}

#[tokio::test]
async fn options_flow_shows_current_addresses() {
    let host = start().await;
    let entry = json!({ "entry_id": "e9", "domain": "google_cast", "data": { "known_hosts": ["10.0.0.5", "tv.lan:8010"] }, "options": {} });
    let f = json!({ "flow_id": "f3", "source": "options", "data": { "scan": false }, "entry": entry });
    let r = host.call("options_flow", json!({ "flow": f, "step": "options" })).await.unwrap();
    assert_eq!(r["result"]["step_id"], "init");
    assert_eq!(r["result"]["schema"]["properties"]["known_hosts"]["default"], "10.0.0.5, tv.lan:8010");
    assert!(r["result"]["schema"]["properties"].get("devices").is_none());
    let f = json!({ "flow_id": "f3", "source": "options", "data": r["data"], "entry": entry });
    let r = host.call("options_flow", json!({ "flow": f, "step": "init", "input": { "known_hosts": "" } })).await.unwrap();
    assert_eq!(r["result"]["type"], "create_entry");
    assert_eq!(r["result"]["data"], json!({ "known_hosts": [] }));
}

async fn service(host: &FakeHost, entity: &str, service: &str, data: Value) -> Result<Value, Value> {
    host.call("entity/call_service", json!({ "entry_id": "e1", "entity_id": entity, "service": service, "data": data })).await
}

fn attr(host: &FakeHost, uid: &str, key: &str) -> Value {
    host.state_of(uid).map(|(_, a)| a[key].clone()).unwrap_or(Value::Null)
}

#[tokio::test]
async fn controls_a_device() {
    let cast = FakeCast::start().await;
    let host = start().await;
    let addr = format!("127.0.0.1:{}", cast.port);
    let uid = format!("host_127_0_0_1_{}", cast.port);
    let entry = json!({ "entry_id": "e1", "domain": "google_cast", "data": { "mdns": false, "known_hosts": [addr] }, "options": {} });
    host.call("setup_entry", json!({ "entry": entry })).await.unwrap();

    // the device and its media player
    let (state, attrs) = wait_for("tilstand", || host.state_of(&uid)).await;
    assert_eq!(state, "off");
    assert_eq!(attrs["volume_level"], 0.5);
    let id = host.entity(&uid);
    {
        let rec = host.rec.lock().unwrap();
        assert_eq!(rec.devices[0]["identifiers"], json!([["google_cast", uid]]));
        assert_eq!(rec.devices[0]["name"], "127.0.0.1");
        assert_eq!(rec.entities[&id].0["domain"], "media_player");
        assert!(rec.entities[&id].1, "kald skal gå til pluginet");
    }

    // volume
    service(&host, &id, "volume_set", json!({ "volume_level": 0.3 })).await.unwrap();
    wait_for("lydstyrke", || (attr(&host, &uid, "volume_level") == 0.3).then_some(())).await;
    service(&host, &id, "volume_mute", json!({ "is_volume_muted": true })).await.unwrap();
    wait_for("lydløs", || (attr(&host, &uid, "is_volume_muted") == true).then_some(())).await;
    service(&host, &id, "volume_up", json!({})).await.unwrap();
    wait_for("højere", || (attr(&host, &uid, "volume_level") == 0.4).then_some(())).await;
    let e = service(&host, &id, "volume_set", json!({})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "invalid_argument");

    // nothing plays yet
    let e = service(&host, &id, "media_pause", json!({})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "invalid_argument");

    // play_media starts the Default Media Receiver and loads the URL
    service(&host, &id, "play_media", json!({ "media_content_id": "http://nas/sang.mp3", "media_content_type": "music",
                                              "extra": { "title": "Sang", "thumb": "http://nas/cover.jpg" } }))
        .await
        .unwrap();
    let (state, attrs) = wait_for("afspilning", || host.state_of(&uid).filter(|(s, _)| s == "playing")).await;
    assert_eq!(state, "playing");
    assert_eq!(attrs["app_id"], "CC1AD845");
    assert_eq!(attrs["app_name"], "Default Media Receiver");
    assert_eq!(attrs["media_title"], "Sang");
    assert_eq!(attrs["media_content_id"], "http://nas/sang.mp3");
    assert_eq!(attrs["media_content_type"], "music");
    assert_eq!(attrs["entity_picture"], "http://nas/cover.jpg");
    assert!(attrs["media_position_updated_at"].is_string());
    let features = attrs["supported_features"].as_u64().unwrap();
    assert_eq!(features & (1 | 2 | 4 | 8 | 128 | 256 | 512 | 4096 | 16384), 1 | 2 | 4 | 8 | 128 | 256 | 512 | 4096 | 16384);
    let load = &cast.sent("LOAD")[0];
    assert_eq!(load["media"]["contentType"], "audio/mpeg");
    assert_eq!(load["media"]["streamType"], "BUFFERED");
    assert_eq!(load["autoplay"], true);
    assert_eq!(cast.launches(), 1);

    // pause keeps the title although the update leaves the media out
    service(&host, &id, "media_pause", json!({})).await.unwrap();
    let (_, attrs) = wait_for("pause", || host.state_of(&uid).filter(|(s, _)| s == "paused")).await;
    assert_eq!(attrs["media_title"], "Sang");
    service(&host, &id, "media_play_pause", json!({})).await.unwrap();
    wait_for("afspiller igen", || host.state_of(&uid).filter(|(s, _)| s == "playing")).await;
    service(&host, &id, "media_seek", json!({ "seek_position": 42 })).await.unwrap();
    wait_for("position", || (attr(&host, &uid, "media_position") == 42.0).then_some(())).await;
    assert_eq!(cast.sent("SEEK")[0]["mediaSessionId"], 7);

    // the app runs already, so the next play_media only loads
    service(&host, &id, "play_media", json!({ "media_content_id": "http://nas/film.mp4", "media_content_type": "video" })).await.unwrap();
    assert_eq!(cast.launches(), 1);
    assert_eq!(cast.sent("LOAD")[1]["media"]["contentType"], "video/mp4");
    let e = service(&host, &id, "play_media", json!({ "media_content_id": "http://nas/fail.mp4", "media_content_type": "video/mp4" })).await.unwrap_err();
    assert!(e["message"].as_str().unwrap().contains("kunne ikke afspille"), "{e}");

    // off stops the app
    service(&host, &id, "turn_off", json!({})).await.unwrap();
    wait_for("slukket", || host.state_of(&uid).filter(|(s, _)| s == "off")).await;
    assert_eq!(cast.sent("STOP").last().unwrap()["sessionId"], "session-1");
    service(&host, &id, "turn_on", json!({})).await.unwrap();
    wait_for("tændt", || host.state_of(&uid).filter(|(s, _)| s == "idle")).await;

    let e = service(&host, &id, "select_sound_mode", json!({})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "not_supported");

    // diagnostics
    let d = host.call("diagnostics", json!({ "entry": entry })).await.unwrap();
    assert_eq!(d["devices"][0]["status"]["connected"], true);
    assert_eq!(d["devices"][0]["manual"], true);

    // the device reboots: unavailable, then back
    cast.drop_connections();
    wait_for("utilgængelig", || host.state_of(&uid).filter(|(s, _)| s == "unavailable")).await;
    let (state, _) = wait_for("forbundet igen", || host.state_of(&uid).filter(|(s, _)| s != "unavailable")).await;
    assert_eq!(state, "idle");
    service(&host, &id, "volume_set", json!({ "volume_level": 0.6 })).await.unwrap();
    wait_for("lydstyrke efter genforbindelse", || (attr(&host, &uid, "volume_level") == 0.6).then_some(())).await;

    // gone for good: calls fail as unavailable
    drop(cast);
    wait_for("utilgængelig igen", || host.state_of(&uid).filter(|(s, _)| s == "unavailable")).await;
    let e = service(&host, &id, "media_play", json!({})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "unavailable");

    host.call("unload_entry", json!({ "entry_id": "e1" })).await.unwrap();
    let e = service(&host, &id, "media_play", json!({})).await.unwrap_err();
    assert_eq!(e["data"]["kind"], "unavailable");
}

#[tokio::test]
async fn connects_to_a_remembered_address_without_mdns() {
    let cast = FakeCast::start().await;
    let host = start().await;
    let uuid = "bb".repeat(16);
    let dir = std::env::temp_dir().join(format!("google-cast-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let saved = json!([{ "uuid": uuid, "name": "Soveværelse", "model": "Chromecast", "manufacturer": "Google", "host": "127.0.0.1", "port": cast.port }]);
    std::fs::write(dir.join("devices-e-remember.json"), saved.to_string()).unwrap();

    // mDNS is off and no address is typed in: only the remembered one is left
    let entry = json!({ "entry_id": "e-remember", "domain": "google_cast", "data": { "mdns": false }, "options": {} });
    host.call("setup_entry", json!({ "entry": entry })).await.unwrap();
    let (state, _) = wait_for("tilstand", || host.state_of(&uuid)).await;
    assert_eq!(state, "off");
    let rec = host.rec.lock().unwrap();
    assert_eq!(rec.devices[0]["name"], "Soveværelse");
    assert_eq!(rec.devices[0]["model"], "Chromecast");
}
