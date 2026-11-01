use rmtt::{Client, ClientOptions};
use std::sync::mpsc;
use std::time::Duration;

/// Cross-language end-to-end probe: the `rmtt` Rust client (crates.io v0.1.0,
/// https://github.com/czqu/rmtt-rust) against the Java rmtt server
/// (rmtt-it E2eServerMain). It connects over TCP, pushes a message upstream
/// and verifies the server's echo comes back downstream. Exit 0 on success.
fn main() {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "18999".to_string());
    let addr = format!("tcp://127.0.0.1:{}", port);

    let mut opts = ClientOptions::default();
    opts.credential = "rust-e2e".to_string();
    opts.heartbeat_seconds = 30;

    let client = Client::connect(&addr, opts).expect("connect failed");
    println!("RUST_CLIENT_CONNECTED");

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    client.set_payload_handler(move |msg| {
        let _ = tx.send(msg.payload().to_vec());
    });

    client.push(b"ping-from-rust").expect("push failed");

    let echo = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("timeout waiting for server echo");
    if echo != b"echo:ping-from-rust" {
        eprintln!("unexpected echo: {:?}", String::from_utf8_lossy(&echo));
        std::process::exit(1);
    }
    println!("RUST_CLIENT_ECHO_OK {}", String::from_utf8_lossy(&echo));
    client.disconnect();

    // Phase 2: regression guards for the blackbox report - a JWT-like credential well above the
    // old 23-byte cap (F2) and a 1 MiB frame above the old 8 KiB cap (F1). The Java server must
    // accept both like rmtt-go / rmtt-rust do.
    let mut opts2 = ClientOptions::default();
    opts2.credential = "j".repeat(300);
    opts2.heartbeat_seconds = 30;
    let client2 = Client::connect(&addr, opts2).expect("long-credential connect failed");
    println!("RUST_LONG_CRED_OK (300-byte credential accepted)");

    let (tx2, rx2) = mpsc::channel::<Vec<u8>>();
    client2.set_payload_handler(move |msg| {
        let _ = tx2.send(msg.payload().to_vec());
    });
    let mut big = Vec::with_capacity(1024 * 1024);
    while big.len() < 1024 * 1024 {
        big.extend_from_slice(b"0123456789abcdef");
    }
    client2.push(&big).expect("1 MiB push failed");
    let mut expect = b"echo:".to_vec();
    expect.extend_from_slice(&big);
    let echo2 = rx2
        .recv_timeout(Duration::from_secs(10))
        .expect("timeout waiting for 1 MiB echo");
    if echo2 != expect {
        eprintln!("1 MiB echo mismatch: got {} bytes", echo2.len());
        std::process::exit(1);
    }
    println!("RUST_LARGE_ECHO_OK (1 MiB round trip)");
    client2.disconnect();
    println!("RUST_CLIENT_E2E_PASS");
}
