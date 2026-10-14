//! The Cast v2 wire format: a `CastMessage` protobuf (cast_channel.proto)
//! behind a 4-byte big-endian length. Only the fields Cast uses are known;
//! others are skipped when reading.

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_MEDIA: &str = "urn:x-cast:com.google.cast.media";

pub const SENDER: &str = "sender-0";
pub const RECEIVER: &str = "receiver-0";

/// Cast refuses messages over 64 KiB; allow some slack when reading.
const MAX_FRAME: usize = 1 << 20;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CastMessage {
    pub source_id: String,
    pub destination_id: String,
    pub namespace: String,
    /// The payload when it is text, which is always JSON in practice.
    pub payload_utf8: Option<String>,
    pub payload_binary: Option<Vec<u8>>,
}

impl CastMessage {
    pub fn json(source: &str, destination: &str, namespace: &str, payload: &Value) -> Self {
        Self {
            source_id: source.into(),
            destination_id: destination.into(),
            namespace: namespace.into(),
            payload_utf8: Some(payload.to_string()),
            payload_binary: None,
        }
    }

    /// The payload as JSON, if it is text and parses.
    pub fn payload(&self) -> Option<Value> {
        self.payload_utf8.as_deref().and_then(|s| serde_json::from_str(s).ok())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64 + self.payload_utf8.as_ref().map_or(0, String::len));
        // protocol_version = CASTV2_1_0 (0)
        put_varint(&mut b, 1 << 3);
        put_varint(&mut b, 0);
        put_bytes(&mut b, 2, self.source_id.as_bytes());
        put_bytes(&mut b, 3, self.destination_id.as_bytes());
        put_bytes(&mut b, 4, self.namespace.as_bytes());
        // payload_type: STRING (0) or BINARY (1)
        put_varint(&mut b, 5 << 3);
        match (&self.payload_utf8, &self.payload_binary) {
            (_, Some(bin)) => {
                put_varint(&mut b, 1);
                put_bytes(&mut b, 7, bin);
            }
            (s, None) => {
                put_varint(&mut b, 0);
                put_bytes(&mut b, 6, s.as_deref().unwrap_or_default().as_bytes());
            }
        }
        b
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, String> {
        let mut m = Self::default();
        while !buf.is_empty() {
            let key = get_varint(&mut buf)?;
            let (field, wire) = (key >> 3, key & 7);
            match wire {
                0 => {
                    get_varint(&mut buf)?;
                }
                2 => {
                    let len = get_varint(&mut buf)? as usize;
                    if len > buf.len() {
                        return Err("felt længere end beskeden".into());
                    }
                    let (data, rest) = buf.split_at(len);
                    buf = rest;
                    let text = || String::from_utf8(data.to_vec()).map_err(|_| format!("felt {field} er ikke UTF-8"));
                    match field {
                        2 => m.source_id = text()?,
                        3 => m.destination_id = text()?,
                        4 => m.namespace = text()?,
                        6 => m.payload_utf8 = Some(text()?),
                        7 => m.payload_binary = Some(data.to_vec()),
                        _ => {}
                    }
                }
                1 => buf = buf.get(8..).ok_or("kort beskeden")?,
                5 => buf = buf.get(4..).ok_or("kort beskeden")?,
                w => return Err(format!("ukendt wire type {w}")),
            }
        }
        Ok(m)
    }
}

fn put_varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push((v as u8) | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

fn put_bytes(b: &mut Vec<u8>, field: u64, data: &[u8]) {
    put_varint(b, (field << 3) | 2);
    put_varint(b, data.len() as u64);
    b.extend_from_slice(data);
}

fn get_varint(buf: &mut &[u8]) -> Result<u64, String> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = buf.split_first().ok_or("varint slutter for tidligt")?;
        *buf = rest;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err("varint for lang".into())
}

pub async fn write_message<W: AsyncWrite + Unpin>(w: &mut W, m: &CastMessage) -> std::io::Result<()> {
    let body = m.encode();
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    w.write_all(&frame).await?;
    w.flush().await
}

pub async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<CastMessage> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("besked på {len} bytes")));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    CastMessage::decode(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip() {
        let m = CastMessage::json(SENDER, RECEIVER, NS_RECEIVER, &json!({ "type": "GET_STATUS", "requestId": 1 }));
        let back = CastMessage::decode(&m.encode()).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.payload().unwrap()["type"], "GET_STATUS");
    }

    #[test]
    fn known_bytes() {
        // encoded by hand from cast_channel.proto
        let m = CastMessage::json("a", "b", "n", &json!({}));
        assert_eq!(m.encode(), [0x08, 0x00, 0x12, 0x01, b'a', 0x1a, 0x01, b'b', 0x22, 0x01, b'n', 0x28, 0x00, 0x32, 0x02, b'{', b'}']);
    }

    #[test]
    fn skips_unknown_fields_and_reads_binary() {
        let mut b = CastMessage { payload_binary: Some(vec![1, 2, 3]), ..Default::default() }.encode();
        // field 9 as fixed64 and field 10 as varint 300
        b.extend_from_slice(&[(9 << 3) | 1, 0, 0, 0, 0, 0, 0, 0, 0, 10 << 3, 0xac, 0x02]);
        let m = CastMessage::decode(&b).unwrap();
        assert_eq!(m.payload_binary, Some(vec![1, 2, 3]));
        assert!(CastMessage::decode(&[0x32, 0x05, b'x']).is_err());
    }

    #[tokio::test]
    async fn framing() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let m = CastMessage::json(SENDER, RECEIVER, NS_HEARTBEAT, &json!({ "type": "PING" }));
        write_message(&mut a, &m).await.unwrap();
        write_message(&mut a, &m).await.unwrap();
        assert_eq!(read_message(&mut b).await.unwrap(), m);
        assert_eq!(read_message(&mut b).await.unwrap(), m);
        drop(a);
        assert!(read_message(&mut b).await.is_err());
    }
}
