use std::path::PathBuf;

mod tls_certs {
    use super::*;

    pub fn server_cert_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/cert.pem")
    }

    pub fn server_key_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/tls/key.pem")
    }
}

/// Test that the TLS config loader can read our test certificates
#[test]
fn test_tls_config_loads() {
    use std::io::BufReader;

    let cert_path = tls_certs::server_cert_path();
    let key_path = tls_certs::server_key_path();

    assert!(cert_path.exists(), "cert file not found: {}", cert_path.display());
    assert!(key_path.exists(), "key file not found: {}", key_path.display());

    // Read and parse the cert
    let cert_file = std::fs::File::open(&cert_path).expect("failed to open cert");
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse cert");
    assert!(!certs.is_empty(), "no certificates found");

    // Read and parse the key
    let key_file = std::fs::File::open(&key_path).expect("failed to open key");
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .expect("failed to parse key");
    assert!(key.is_some(), "no private key found");
}

/// Test that the TLS config builder creates a valid config with no client auth
#[test]
fn test_tls_config_builds_no_auth() {
    use std::io::BufReader;
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;

    let cert_path = tls_certs::server_cert_path();
    let key_path = tls_certs::server_key_path();

    let cert_file = std::fs::File::open(&cert_path).expect("failed to open cert");
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse cert");

    let key_file = std::fs::File::open(&key_path).expect("failed to open key");
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .expect("failed to parse key")
        .expect("no private key");

    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("failed to build TLS config");

    // Verify the config is valid by creating a TlsAcceptor
    let _acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
}

/// Test full TLS round-trip: start server, connect via TLS, verify response
#[tokio::test]
async fn test_tls_ping() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::process::Command;
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::TlsConnector;

    let tls_port = 16380u16;
    let server_bin = PathBuf::from(env!("CARGO_BIN_EXE_valkey-server"));

    // Start the server with TLS env vars
    let mut child = Command::new(&server_bin)
        .env("TLS_PORT", tls_port.to_string())
        .env("TLS_CERT", tls_certs::server_cert_path())
        .env("TLS_KEY", tls_certs::server_key_path())
        .env("TLS_AUTH_CLIENTS", "no")
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start valkey-server");

    // Wait for server to be ready by attempting TLS connections
    let mut ready = false;
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(NoopVerifier))
        .with_no_client_auth();
    let connector = TlsConnector::from(std::sync::Arc::new(config));

    tokio::time::sleep(Duration::from_secs(3)).await;
    for _ in 0..40 {
        match TcpStream::connect(format!("127.0.0.1:{}", tls_port)).await {
            Ok(tcp) => {
                let name = ServerName::try_from("localhost").expect("invalid server name");
                match tokio::time::timeout(Duration::from_secs(5), connector.clone().connect(name, tcp)).await {
                    Ok(Ok(mut stream)) => {
                        // Server is ready - send a PING command
                        let ping = b"*1\r\n$4\r\nPING\r\n";
                        if stream.write_all(ping).await.is_ok() && stream.flush().await.is_ok() {
                            let mut buf = [0u8; 64];
                            match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
                                Ok(Ok(n)) if n > 0 => {
                                    let resp = String::from_utf8_lossy(&buf[..n]);
                                    // Verify we got a valid RESP response (starts with + or -)
                                    assert!(resp.starts_with('+') || resp.starts_with('-'),
                                        "expected RESP response, got: {:?}", resp);
                                    ready = true;
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if !ready {
        let _ = child.kill().await;
        panic!("server did not start in time or did not respond");
    }

    // Now do the actual test with a fresh connection
    let tcp_stream = TcpStream::connect(format!("127.0.0.1:{}", tls_port))
        .await
        .expect("failed to connect to TLS server");

    let server_name = ServerName::try_from("localhost")
        .expect("invalid server name");

    let mut tls_stream = tokio::time::timeout(
        Duration::from_secs(5),
        connector.connect(server_name, tcp_stream)
    ).await.expect("TLS handshake timed out").expect("TLS handshake failed");

    // Send PING command
    let ping = b"*1\r\n$4\r\nPING\r\n";
    tls_stream.write_all(ping).await.unwrap();
    tls_stream.flush().await.unwrap();

    // Read response
    let mut buf = [0u8; 64];
    let n = tls_stream.read(&mut buf).await.expect("failed to read response");
    let resp = String::from_utf8_lossy(&buf[..n]);

    // The server should respond with a RESP simple string (+PONG) or error
    assert!(resp.starts_with('+') || resp.starts_with('-'),
        "expected RESP response, got: {:?}", resp);

    // Cleanup
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[derive(Debug)]
struct NoopVerifier;

impl rustls::client::danger::ServerCertVerifier for NoopVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
