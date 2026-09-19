use std::io;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

use interprocess::local_socket::traits::{Listener as _, Stream as _};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::ipc::LocalListener;
use crate::server::client_transport::{self, ServerEvent};

/// Accepts pending thin-client connections and starts their handshake readers.
pub(crate) fn accept_pending_client_connections(
    listener: &LocalListener,
    next_client_id: &mut u64,
    should_quit: &Arc<AtomicBool>,
    server_event_tx: &mpsc::Sender<ServerEvent>,
) -> io::Result<()> {
    loop {
        if should_quit.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok(stream) => {
                let client_id = *next_client_id;
                *next_client_id = next_client_id.saturating_add(1);

                if let Err(err) = stream.set_nonblocking(true) {
                    warn!(err = %err, "failed to set client stream nonblocking");
                    continue;
                }

                let should_quit = should_quit.clone();
                let server_event_tx = server_event_tx.clone();
                std::thread::spawn(move || {
                    if let Err(err) = client_transport::handle_client_handshake(
                        stream,
                        client_id,
                        &server_event_tx,
                        &should_quit,
                    ) {
                        debug!(client_id, err = %err, "client handshake failed");
                    }
                });
            }
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => {
                error!(err = %err, "client listener accept failed");
                break;
            }
        }
    }

    Ok(())
}

/// Drains pending thin-client connections without starting handshakes.
///
/// During live handoff the old server must not let clients sit in the Unix
/// listener backlog waiting for a welcome frame that will never be sent.
pub(crate) fn reject_pending_client_connections(listener: &LocalListener) -> io::Result<()> {
    loop {
        match listener.accept() {
            Ok(_stream) => {}
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => {
                error!(err = %err, "client listener reject failed");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(unix)]
pub(crate) use crate::platform::ClientAcceptWaker;

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn waker_notifies_when_a_connection_is_pending() {
        let dir = std::env::temp_dir().join(format!("herdr-waker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("l.sock");
        let listener = crate::ipc::bind_local_listener(&path).unwrap();
        listener
            .set_nonblocking(interprocess::local_socket::ListenerNonblockingMode::Accept)
            .unwrap();

        let notify = Arc::new(tokio::sync::Notify::new());
        let waker = ClientAcceptWaker::spawn(notify.clone());
        waker.rearm(&listener);

        let idle = tokio::time::timeout(Duration::from_millis(150), notify.notified()).await;
        assert!(idle.is_err(), "nothing is pending yet");

        let _client = crate::ipc::connect_local_stream(&path).unwrap();
        tokio::time::timeout(Duration::from_secs(2), notify.notified())
            .await
            .expect("pending connection must wake the loop");

        // The loop drains the backlog, then re-arms; a fresh connection wakes it again.
        assert!(listener.accept().is_ok());
        waker.rearm(&listener);
        let _second = crate::ipc::connect_local_stream(&path).unwrap();
        tokio::time::timeout(Duration::from_secs(2), notify.notified())
            .await
            .expect("re-armed waker must fire again");

        drop(listener);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn waker_replaces_an_idle_listener_and_stops_without_a_connection() {
        let dir = std::env::temp_dir().join(format!("herdr-waker-replace-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let old = crate::ipc::bind_local_listener(&dir.join("old.sock")).unwrap();
        let new_path = dir.join("new.sock");
        let new = crate::ipc::bind_local_listener(&new_path).unwrap();
        let notify = Arc::new(tokio::sync::Notify::new());
        let waker = ClientAcceptWaker::spawn(notify.clone());
        waker.rearm(&old);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), notify.notified())
                .await
                .is_err()
        );
        waker.rearm(&new);
        drop(old);
        let _client = crate::ipc::connect_local_stream(&new_path).unwrap();
        tokio::time::timeout(Duration::from_secs(2), notify.notified())
            .await
            .unwrap();
        assert!(new.accept().is_ok());
        waker.rearm(&new);
        drop(new);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            drop(waker);
            let _ = done_tx.send(());
        });
        tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
