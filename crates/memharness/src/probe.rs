//! Is a BAiD server running locally? (G26)
//!
//! The loaders write straight to the backends, so BAiD's write log never sees the load and a
//! running BAiD could keep serving results it cached before the load. `memharness load`
//! therefore refuses while anything accepts connections on BAiD's ports (default
//! `127.0.0.1:7070` and the admin listener `127.0.0.1:7071`), unless told the caches were
//! flushed.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;

/// BAiD's default listeners, as seen from this machine.
pub const DEFAULT_BAID_ADDRS: [&str; 2] = ["127.0.0.1:7070", "127.0.0.1:7071"];

/// How long to wait for each connection. On Windows a refused local connect can take ~2 s
/// (SYN retries), so this also bounds that.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// The first address in `addrs` that accepts a TCP connection, if any.
pub async fn first_listening(addrs: &[SocketAddr]) -> Option<SocketAddr> {
    for addr in addrs {
        if let Ok(Ok(_)) = tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(addr)).await {
            return Some(*addr);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sees_a_listener_and_ignores_a_closed_port() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let open = l.local_addr().expect("addr");
        // A port that was free a moment ago: bind and drop.
        let closed = {
            let c = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            c.local_addr().expect("addr")
        };
        assert_eq!(first_listening(&[closed]).await, None);
        assert_eq!(first_listening(&[closed, open]).await, Some(open));
    }
}
