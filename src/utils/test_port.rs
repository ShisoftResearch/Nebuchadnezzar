//! Process-unique localhost address allocation for tests.
//!
//! Test servers used to bind hardcoded ports; two tests sharing a
//! number and running concurrently made one server fail its bind
//! (`AddrInUse`) and — worse — sent its clients to the *other* test's
//! server, where plane lookups miss and bootstrap stalls until retry
//! exhaustion. Ports here come from the OS ephemeral range (bind to
//! port 0), deduplicated process-wide so no two calls in one test
//! binary ever return the same port.

use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::Mutex;

static CLAIMED_PORTS: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// Returns a localhost port no other call in this process has handed
/// out, currently free according to the OS.
pub fn unique_localhost_port() -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener
            .local_addr()
            .expect("read ephemeral port")
            .port();
        drop(listener);
        let mut claimed = CLAIMED_PORTS.lock().expect("port registry poisoned");
        if claimed.get_or_insert_with(HashSet::new).insert(port) {
            return port;
        }
    }
}

/// `127.0.0.1:<unique port>` — the shape most test servers take.
pub fn unique_localhost_addr() -> String {
    format!("127.0.0.1:{}", unique_localhost_port())
}
