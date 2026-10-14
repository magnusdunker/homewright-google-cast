//! TLS to a cast device. Cast devices use self-signed certificates, so the
//! certificate is not checked, the same as every other Cast sender.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

pub type Stream = TlsStream<TcpStream>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug)]
struct AnyCert(Vec<SignatureScheme>);

impl ServerCertVerifier for AnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(&self, _m: &[u8], _c: &CertificateDer<'_>, _d: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(&self, _m: &[u8], _c: &CertificateDer<'_>, _d: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.clone()
    }
}

fn connector() -> TlsConnector {
    static CONNECTOR: OnceLock<TlsConnector> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let schemes = provider.signature_verification_algorithms.supported_schemes();
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("TLS-versioner")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AnyCert(schemes)))
                .with_no_client_auth();
            TlsConnector::from(Arc::new(config))
        })
        .clone()
}

pub async fn connect(host: &str, port: u16) -> std::io::Result<Stream> {
    let io_err = |e: String| std::io::Error::other(e);
    let work = async {
        let tcp = TcpStream::connect((host, port)).await?;
        let _ = tcp.set_nodelay(true);
        let name = match host.parse::<std::net::IpAddr>() {
            Ok(ip) => ServerName::IpAddress(ip.into()),
            Err(_) => ServerName::try_from(host.to_string()).map_err(|e| io_err(e.to_string()))?,
        };
        connector().connect(name, tcp).await
    };
    tokio::time::timeout(CONNECT_TIMEOUT, work).await.map_err(|_| io_err(format!("{host}:{port} svarede ikke")))?
}

/// `host`, `host:port` or `[v6]:port`; the port defaults to 8009.
pub fn parse_host(s: &str) -> Option<(String, u16)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Some(rest) = s.strip_prefix('[') {
        let (h, tail) = rest.split_once(']')?;
        let port = match tail.strip_prefix(':') {
            Some(p) => p.parse().ok()?,
            None if tail.is_empty() => 8009,
            None => return None,
        };
        return Some((h.to_string(), port));
    }
    match s.rsplit_once(':') {
        // a bare IPv6 address has several colons
        Some((h, p)) if !h.contains(':') => Some((h.to_string(), p.parse().ok()?)),
        _ => Some((s.to_string(), 8009)),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_host;

    #[test]
    fn hosts() {
        assert_eq!(parse_host(" 192.168.1.5 "), Some(("192.168.1.5".into(), 8009)));
        assert_eq!(parse_host("tv.lan:8010"), Some(("tv.lan".into(), 8010)));
        assert_eq!(parse_host("fe80::1"), Some(("fe80::1".into(), 8009)));
        assert_eq!(parse_host("[fe80::1]:9000"), Some(("fe80::1".into(), 9000)));
        assert_eq!(parse_host("x:y"), None);
        assert_eq!(parse_host(""), None);
    }
}
