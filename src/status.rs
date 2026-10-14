//! Turns the receiver's and the media app's status into a media_player
//! state with Home Assistant's attribute names.

use serde_json::{json, Map, Value};

/// The Default Media Receiver, used for play_media and turn_on.
pub const DEFAULT_MEDIA_RECEIVER: &str = "CC1AD845";
/// Backdrop, the ambient screen shown when nothing runs.
pub const IDLE_APP: &str = "E8C28D3C";

// media_player supported_features, as in Home Assistant
pub const PAUSE: u64 = 1;
pub const SEEK: u64 = 2;
pub const VOLUME_SET: u64 = 4;
pub const VOLUME_MUTE: u64 = 8;
pub const PREVIOUS_TRACK: u64 = 16;
pub const NEXT_TRACK: u64 = 32;
pub const TURN_ON: u64 = 128;
pub const TURN_OFF: u64 = 256;
pub const PLAY_MEDIA: u64 = 512;
pub const VOLUME_STEP: u64 = 1024;
pub const STOP: u64 = 4096;
pub const PLAY: u64 = 16384;

// supportedMediaCommands in a media status
const CMD_PAUSE: u64 = 1;
const CMD_SEEK: u64 = 2;
const CMD_QUEUE_NEXT: u64 = 64;
const CMD_QUEUE_PREV: u64 = 128;

/// The running app that matters: the first one that is not the idle screen.
pub fn active_app(receiver: &Value) -> Option<&Value> {
    receiver["applications"]
        .as_array()?
        .iter()
        .find(|a| a["isIdleScreen"] != true && a["appId"].as_str() != Some(IDLE_APP))
}

/// Whether an app speaks the media namespace, so it can be asked for media status.
pub fn has_media(app: &Value) -> bool {
    app["namespaces"]
        .as_array()
        .is_some_and(|ns| ns.iter().any(|n| n["name"] == crate::proto::NS_MEDIA))
}

/// The newest media status from a MEDIA_STATUS message. Updates may leave
/// out `media` when it did not change, so it is kept from `previous` for
/// the same session.
pub fn merge_media(previous: Option<&Value>, message: &Value) -> Option<Value> {
    let mut status = message["status"].as_array()?.first()?.clone();
    if status.get("media").is_none_or(Value::is_null) {
        if let Some(prev) = previous.filter(|p| p["mediaSessionId"] == status["mediaSessionId"]) {
            if let Some(media) = prev.get("media") {
                status["media"] = media.clone();
            }
        }
    }
    Some(status)
}

fn content_type(media: &Value) -> &'static str {
    match media.pointer("/metadata/metadataType").and_then(Value::as_u64) {
        Some(1) => return "movie",
        Some(2) => return "tvshow",
        Some(3) => return "music",
        Some(4) => return "image",
        _ => {}
    }
    match media["contentType"].as_str().unwrap_or_default() {
        t if t.starts_with("audio/") => "music",
        t if t.starts_with("image/") => "image",
        _ => "video",
    }
}

/// State and attributes for the media_player entity. `now` goes into
/// `media_position_updated_at`.
pub fn state_of(receiver: &Value, media: Option<&Value>, now: &str) -> (String, Map<String, Value>) {
    let mut a = Map::new();
    let mut features = TURN_ON | TURN_OFF | PLAY_MEDIA;

    let volume = &receiver["volume"];
    if volume["controlType"] != "fixed" {
        features |= VOLUME_SET | VOLUME_MUTE | VOLUME_STEP;
        if let Some(level) = volume["level"].as_f64() {
            a.insert("volume_level".into(), json!((level * 1000.0).round() / 1000.0));
        }
        if let Some(muted) = volume["muted"].as_bool() {
            a.insert("is_volume_muted".into(), json!(muted));
        }
    }

    let app = active_app(receiver);
    if let Some(app) = app {
        if let Some(id) = app["appId"].as_str() {
            a.insert("app_id".into(), json!(id));
        }
        if let Some(name) = app["displayName"].as_str() {
            a.insert("app_name".into(), json!(name));
        }
    }

    let media = media.filter(|_| app.is_some());
    let mut state = match app {
        Some(_) => "idle",
        None => "off",
    };
    if let Some(m) = media {
        state = match m["playerState"].as_str().unwrap_or_default() {
            "PLAYING" => "playing",
            "PAUSED" => "paused",
            "BUFFERING" | "LOADING" => "buffering",
            _ => "idle",
        };
        let cmds = m["supportedMediaCommands"].as_u64().unwrap_or(0);
        features |= PLAY | STOP;
        if cmds & CMD_PAUSE != 0 {
            features |= PAUSE;
        }
        if cmds & CMD_SEEK != 0 {
            features |= SEEK;
        }
        if cmds & CMD_QUEUE_NEXT != 0 {
            features |= NEXT_TRACK;
        }
        if cmds & CMD_QUEUE_PREV != 0 {
            features |= PREVIOUS_TRACK;
        }

        let info = &m["media"];
        if info.is_object() && state != "idle" {
            let meta = &info["metadata"];
            let text = |v: &Value| v.as_str().filter(|s| !s.is_empty()).map(|s| json!(s));
            if let Some(id) = text(&info["contentId"]) {
                a.insert("media_content_id".into(), id);
            }
            a.insert("media_content_type".into(), json!(content_type(info)));
            for (key, attr) in [
                ("title", "media_title"),
                ("artist", "media_artist"),
                ("albumName", "media_album_name"),
                ("albumArtist", "media_album_artist"),
                ("seriesTitle", "media_series_title"),
            ] {
                if let Some(v) = text(&meta[key]) {
                    a.insert(attr.into(), v);
                }
            }
            if !a.contains_key("media_title") {
                if let Some(v) = text(&meta["subtitle"]) {
                    a.insert("media_title".into(), v);
                }
            }
            for (key, attr) in [("season", "media_season"), ("episode", "media_episode")] {
                if let Some(n) = meta[key].as_u64() {
                    a.insert(attr.into(), json!(n));
                }
            }
            if let Some(url) = meta.pointer("/images/0/url").and_then(Value::as_str) {
                a.insert("entity_picture".into(), json!(url));
            }
            if let Some(d) = info["duration"].as_f64().filter(|d| *d > 0.0) {
                a.insert("media_duration".into(), json!(d));
            }
        }
        if state != "idle" {
            if let Some(pos) = m["currentTime"].as_f64() {
                a.insert("media_position".into(), json!(pos));
                a.insert("media_position_updated_at".into(), json!(now));
            }
        }
    }

    a.insert("supported_features".into(), json!(features));
    (state.to_string(), a)
}

/// A content type for LOAD. Home Assistant passes either a MIME type or a
/// media type like `music`; for the latter, guess from the URL.
pub fn mime_for(url: &str, content_type: &str) -> String {
    if content_type.contains('/') {
        return content_type.to_string();
    }
    let path = url.split(['?', '#']).next().unwrap_or(url).to_lowercase();
    let ext = path.rsplit_once('.').map(|(_, e)| e).unwrap_or_default();
    let by_ext = match ext {
        "mp3" => Some("audio/mpeg"),
        "aac" | "m4a" => Some("audio/mp4"),
        "ogg" | "oga" => Some("audio/ogg"),
        "opus" => Some("audio/opus"),
        "flac" => Some("audio/flac"),
        "wav" => Some("audio/wav"),
        "mp4" | "m4v" => Some("video/mp4"),
        "webm" => Some("video/webm"),
        "mkv" => Some("video/x-matroska"),
        "m3u8" => Some("application/x-mpegURL"),
        "mpd" => Some("application/dash+xml"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    };
    if let Some(m) = by_ext {
        return m.into();
    }
    match content_type {
        "music" | "audio" | "podcast" | "track" => "audio/mpeg",
        "image" => "image/jpeg",
        _ => "video/mp4",
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receiver(app: Option<Value>) -> Value {
        json!({ "volume": { "level": 0.4, "muted": false, "controlType": "attenuation" },
                "applications": app.map(|a| vec![a]).unwrap_or_default() })
    }

    fn spotify() -> Value {
        json!({ "appId": "CC32E753", "displayName": "Spotify", "isIdleScreen": false, "sessionId": "s1", "transportId": "t1",
                "namespaces": [{ "name": NS }, { "name": "urn:x-cast:com.spotify.chromecast.secure.v1" }] })
    }

    const NS: &str = crate::proto::NS_MEDIA;

    #[test]
    fn off_without_app_or_with_backdrop() {
        let (s, a) = state_of(&receiver(None), None, "t");
        assert_eq!(s, "off");
        assert_eq!(a["volume_level"], 0.4);
        assert_eq!(a["is_volume_muted"], false);
        assert_eq!(a["supported_features"], TURN_ON | TURN_OFF | PLAY_MEDIA | VOLUME_SET | VOLUME_MUTE | VOLUME_STEP);
        let backdrop = json!({ "appId": IDLE_APP, "displayName": "Backdrop", "isIdleScreen": true });
        assert_eq!(state_of(&receiver(Some(backdrop)), None, "t").0, "off");
    }

    #[test]
    fn idle_with_app_and_no_media() {
        let (s, a) = state_of(&receiver(Some(spotify())), None, "t");
        assert_eq!(s, "idle");
        assert_eq!(a["app_name"], "Spotify");
        assert_eq!(a["app_id"], "CC32E753");
        assert!(has_media(&spotify()));
    }

    #[test]
    fn playing_music() {
        let msg = json!({ "type": "MEDIA_STATUS", "status": [{
            "mediaSessionId": 3, "playerState": "PLAYING", "currentTime": 12.5, "supportedMediaCommands": 1 | 2 | 64,
            "media": { "contentId": "spotify:track:x", "contentType": "application/x-spotify.track", "duration": 200.0,
                       "metadata": { "metadataType": 3, "title": "Sang", "artist": "Kunstner", "albumName": "Album",
                                     "images": [{ "url": "https://i/1.jpg" }] } } }] });
        let m = merge_media(None, &msg).unwrap();
        let (s, a) = state_of(&receiver(Some(spotify())), Some(&m), "2026-10-14T12:00:00Z");
        assert_eq!(s, "playing");
        assert_eq!(a["media_title"], "Sang");
        assert_eq!(a["media_artist"], "Kunstner");
        assert_eq!(a["media_album_name"], "Album");
        assert_eq!(a["media_content_type"], "music");
        assert_eq!(a["entity_picture"], "https://i/1.jpg");
        assert_eq!(a["media_duration"], 200.0);
        assert_eq!(a["media_position"], 12.5);
        assert_eq!(a["media_position_updated_at"], "2026-10-14T12:00:00Z");
        let f = a["supported_features"].as_u64().unwrap();
        assert_eq!(f & (PLAY | PAUSE | STOP | SEEK | NEXT_TRACK), PLAY | PAUSE | STOP | SEEK | NEXT_TRACK);
        assert_eq!(f & PREVIOUS_TRACK, 0);

        // an update without media keeps the old media for the same session
        let upd = json!({ "status": [{ "mediaSessionId": 3, "playerState": "PAUSED", "currentTime": 20.0 }] });
        let m2 = merge_media(Some(&m), &upd).unwrap();
        let (s, a) = state_of(&receiver(Some(spotify())), Some(&m2), "t");
        assert_eq!(s, "paused");
        assert_eq!(a["media_title"], "Sang");
        assert_eq!(a["media_position"], 20.0);
        // but not for another session
        let other = json!({ "status": [{ "mediaSessionId": 4, "playerState": "BUFFERING" }] });
        let m3 = merge_media(Some(&m), &other).unwrap();
        let (s, a) = state_of(&receiver(Some(spotify())), Some(&m3), "t");
        assert_eq!(s, "buffering");
        assert!(a.get("media_title").is_none());
        assert!(merge_media(Some(&m), &json!({ "status": [] })).is_none());
    }

    #[test]
    fn idle_media_and_fixed_volume() {
        let mut r = receiver(Some(spotify()));
        r["volume"] = json!({ "controlType": "fixed", "level": 1.0 });
        let m = json!({ "mediaSessionId": 1, "playerState": "IDLE", "idleReason": "FINISHED", "media": { "metadata": { "title": "x" } } });
        let (s, a) = state_of(&r, Some(&m), "t");
        assert_eq!(s, "idle");
        assert!(a.get("volume_level").is_none());
        assert!(a.get("media_title").is_none());
        assert_eq!(a["supported_features"].as_u64().unwrap() & VOLUME_SET, 0);
        // media without an app is stale
        assert_eq!(state_of(&receiver(None), Some(&m), "t").0, "off");
    }

    #[test]
    fn generic_video_uses_subtitle() {
        let m = json!({ "mediaSessionId": 1, "playerState": "PLAYING",
                        "media": { "contentType": "video/mp4", "metadata": { "metadataType": 0, "subtitle": "Klip" } } });
        let (_, a) = state_of(&receiver(Some(spotify())), Some(&m), "t");
        assert_eq!(a["media_content_type"], "video");
        assert_eq!(a["media_title"], "Klip");
    }

    #[test]
    fn mime_types() {
        assert_eq!(mime_for("http://x/a.MP3?x=1", "music"), "audio/mpeg");
        assert_eq!(mime_for("http://x/a", "audio/flac"), "audio/flac");
        assert_eq!(mime_for("http://x/live.m3u8", "video"), "application/x-mpegURL");
        assert_eq!(mime_for("http://x/stream", "music"), "audio/mpeg");
        assert_eq!(mime_for("http://x/stream", "video"), "video/mp4");
        assert_eq!(mime_for("http://x/p.png", "image"), "image/png");
    }
}
