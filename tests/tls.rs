//! TLS on its own port (`--tls-port`), end to end with a self-signed
//! certificate the client trusts.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tantalum::context::{Context, Settings};
use tantalum::server::{self, TlsAcceptor};
use tantalum::store::Store;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{self, RootCertStore, pki_types::ServerName};

/// A server-side acceptor loaded from PEM files (as `--tls-cert-file` and
/// `--tls-key-file` are), and a client that trusts its certificate.
fn tls_pair(name: &str) -> (TlsAcceptor, TlsConnector) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("tantalum-tls-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert, generated.cert.pem()).unwrap();
    std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
    let acceptor = server::tls_acceptor(&cert, &key).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();

    let mut roots = RootCertStore::empty();
    roots.add(generated.cert.der().clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    (acceptor, TlsConnector::from(Arc::new(config)))
}

fn context(settings: Settings) -> Arc<Context> {
    Arc::new(Context::new(Arc::new(Store::new(16)), settings))
}

/// A server on the shared runtime with a plain and a TLS listener.
async fn serve_shared(ctx: Arc<Context>, acceptor: TlsAcceptor) -> (SocketAddr, SocketAddr) {
    let plain = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addrs = (plain.local_addr().unwrap(), tls.local_addr().unwrap());
    tokio::spawn(server::serve_with(plain, ctx.clone(), None));
    tokio::spawn(server::serve_with(tls, ctx, Some(acceptor)));
    addrs
}

/// The same with per-thread loops (also what serves TLS under io_uring).
fn serve_per_thread(ctx: Arc<Context>, acceptor: TlsAcceptor) -> (SocketAddr, SocketAddr) {
    let plain = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tls = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addrs = (plain.local_addr().unwrap(), tls.local_addr().unwrap());
    std::thread::spawn(move || {
        server::serve_per_thread_with(vec![(plain, None), (tls, Some(acceptor))], ctx, 2)
    });
    addrs
}

async fn connect_tls(
    connector: &TlsConnector,
    addr: SocketAddr,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap()
}

fn req(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn check(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    request: &[u8],
    expected: &[u8],
) {
    stream.write_all(request).await.unwrap();
    stream.flush().await.unwrap();
    let mut got = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut got))
        .await
        .expect("timed out waiting for reply")
        .unwrap();
    assert_eq!(
        got.escape_ascii().to_string(),
        expected.escape_ascii().to_string()
    );
}

/// Reads until the server closes; returns what came.
async fn read_to_end(stream: &mut (impl AsyncRead + Unpin)) -> Vec<u8> {
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut out))
        .await
        .expect("timed out waiting for the server to close");
    out
}

async fn exercise(plain: SocketAddr, tls: SocketAddr, connector: &TlsConnector) {
    let mut c = connect_tls(connector, tls).await;
    check(&mut c, &req(&[b"PING"]), b"+PONG\r\n").await;
    check(&mut c, &req(&[b"SET", b"k", b"over tls"]), b"+OK\r\n").await;
    // The same data on the plain port.
    let mut p = TcpStream::connect(plain).await.unwrap();
    check(&mut p, &req(&[b"GET", b"k"]), b"$8\r\nover tls\r\n").await;
    // A big value (written with writev on plain TCP) and a long pipeline.
    let big = vec![b'x'; 100_000];
    check(&mut c, &req(&[b"SET", b"big", &big]), b"+OK\r\n").await;
    let mut expected = b"$100000\r\n".to_vec();
    expected.extend_from_slice(&big);
    expected.extend_from_slice(b"\r\n");
    check(&mut c, &req(&[b"GET", b"big"]), &expected).await;
    let mut pipeline = Vec::new();
    let mut replies = Vec::new();
    for i in 0..1_000 {
        let v = format!("v{i}");
        pipeline.extend(req(&[b"SET", format!("p{i}").as_bytes(), v.as_bytes()]));
        pipeline.extend(req(&[b"GET", format!("p{i}").as_bytes()]));
        replies.extend_from_slice(b"+OK\r\n");
        replies.extend(format!("${}\r\n{v}\r\n", v.len()).into_bytes());
    }
    check(&mut c, &pipeline, &replies).await;
    // A plain client on the TLS port gets disconnected, not an answer.
    let mut wrong = TcpStream::connect(tls).await.unwrap();
    wrong.write_all(&req(&[b"PING"])).await.unwrap();
    assert!(!read_to_end(&mut wrong).await.starts_with(b"+PONG"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_and_plain_ports_on_the_shared_runtime() {
    let (acceptor, connector) = tls_pair("shared");
    let (plain, tls) = serve_shared(context(Settings::default()), acceptor).await;
    exercise(plain, tls, &connector).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_and_plain_ports_per_thread() {
    let (acceptor, connector) = tls_pair("per-thread");
    let (plain, tls) = serve_per_thread(context(Settings::default()), acceptor);
    tokio::time::sleep(Duration::from_millis(100)).await;
    exercise(plain, tls, &connector).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_connections_time_out_and_shut_down() {
    let (acceptor, connector) = tls_pair("lifecycle");
    let ctx = context(Settings {
        timeout: Some(Duration::from_millis(300)),
        ..Settings::default()
    });
    let (_, tls) = serve_shared(ctx.clone(), acceptor).await;
    // Idle past the timeout: closed.
    let mut idle = connect_tls(&connector, tls).await;
    check(&mut idle, &req(&[b"PING"]), b"+PONG\r\n").await;
    assert_eq!(read_to_end(&mut idle).await, b"");
    // Graceful shutdown closes a TLS connection too.
    let mut c = connect_tls(&connector, tls).await;
    check(&mut c, &req(&[b"PING"]), b"+PONG\r\n").await;
    ctx.shutdown();
    assert_eq!(read_to_end(&mut c).await, b"");
    let drained = tokio::task::spawn_blocking(move || server::drain(&ctx, Duration::from_secs(5)));
    assert_eq!(drained.await.unwrap(), 0);
}

#[test]
fn bad_tls_files_are_reported() {
    let dir = std::env::temp_dir().join(format!("tantalum-tls-{}-bad", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    let missing = server::tls_acceptor(&cert, &key).err().unwrap().to_string();
    assert!(missing.contains("can't read the certificate"), "{missing}");
    let a = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let b = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&cert, a.cert.pem()).unwrap();
    std::fs::write(&key, b.signing_key.serialize_pem()).unwrap();
    let mismatch = server::tls_acceptor(&cert, &key).err().unwrap().to_string();
    assert!(mismatch.contains("doesn't go with the key"), "{mismatch}");
    std::fs::remove_dir_all(&dir).unwrap();
}
