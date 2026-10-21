//! Localhost port reservations for RPC tests.
//!
//! Each socket stays bound without a listener for the process lifetime.
//! Tokio RPC listeners reuse the bind at startup and restart. New port
//! allocations bind without address reuse, so they cannot take a held port.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Mutex;
use tokio::net::TcpSocket;

static RESERVED_PORTS: Mutex<Vec<TcpSocket>> = Mutex::new(Vec::new());

// The diagnostic runner assigns a separate locked range to each process.
#[cfg(feature = "c3doubt_trace")]
fn test_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    match std::env::var("C3DOUBT_PORT_BASE") {
        Ok(base) => {
            base.parse::<u16>().unwrap() + NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        }
        Err(_) => 0,
    }
}

#[cfg(not(feature = "c3doubt_trace"))]
fn test_port() -> u16 {
    0
}

/// Returns a localhost port reserved for this process's RPC listeners.
pub fn unique_localhost_port() -> u16 {
    let socket = TcpSocket::new_v4().expect("create port reservation socket");
    socket
        .set_reuseaddr(false)
        .expect("disable reuse during allocation");
    socket
        .bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, test_port()).into())
        .expect("bind ephemeral port");
    let port = socket.local_addr().expect("read ephemeral port").port();
    // The RPC listener uses SO_REUSEADDR. Allow it to share this bind while
    // retaining the reservation through the listener's shutdown window.
    socket
        .set_reuseaddr(true)
        .expect("allow RPC listener reuse");
    RESERVED_PORTS
        .lock()
        .expect("port registry poisoned")
        .push(socket);
    port
}

/// Returns the reserved address in the form `127.0.0.1:<port>`.
pub fn unique_localhost_addr() -> String {
    format!("127.0.0.1:{}", unique_localhost_port())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[tokio::test]
    async fn allocated_port_stays_reserved_between_rpc_listeners() {
        let address = unique_localhost_addr();
        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        drop(listener);
        let competing = tokio::net::TcpSocket::new_v4().unwrap();
        competing.set_reuseaddr(false).unwrap();
        assert_eq!(
            competing.bind(address.parse().unwrap()).unwrap_err().kind(),
            ErrorKind::AddrInUse
        );
        let restarted = tokio::net::TcpListener::bind(&address).await.unwrap();
        assert_eq!(restarted.local_addr().unwrap().to_string(), address);
    }
}
