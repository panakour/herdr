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

/// Wakes the headless loop as soon as a client connection is waiting on the
/// non-blocking listener, instead of leaving it to the idle poll interval.
///
/// The loop re-arms the watcher after each accept pass with the listener it is
/// currently using, so a listener rebuilt after a failed handoff is picked up on
/// the next pass and a stale descriptor only produces one harmless extra wake.
#[cfg(unix)]
pub(crate) struct ClientAcceptWaker {
    rearm_tx: std::sync::mpsc::SyncSender<std::os::fd::RawFd>,
}

#[cfg(unix)]
impl ClientAcceptWaker {
    pub(crate) fn spawn(notify: Arc<tokio::sync::Notify>) -> Self {
        let (rearm_tx, rearm_rx) = std::sync::mpsc::sync_channel::<std::os::fd::RawFd>(1);
        let spawned = std::thread::Builder::new()
            .name("client-accept-waker".into())
            .spawn(move || {
                while let Ok(fd) = rearm_rx.recv() {
                    wait_until_readable(fd);
                    notify.notify_one();
                }
            });
        if let Err(err) = spawned {
            warn!(err = %err, "client accept waker unavailable; falling back to polling");
        }
        Self { rearm_tx }
    }

    /// Watches `listener` for the next pending connection. At most one request
    /// is queued; the loop calls this after every accept pass.
    pub(crate) fn rearm(&self, listener: &LocalListener) {
        use std::os::fd::{AsFd as _, AsRawFd as _};

        let interprocess::local_socket::Listener::UdSocket(inner) = listener;
        let _ = self.rearm_tx.try_send(inner.as_fd().as_raw_fd());
    }
}

#[cfg(unix)]
fn wait_until_readable(fd: std::os::fd::RawFd) {
    loop {
        let mut pollfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: `pollfd` is a valid, exclusively borrowed array of one entry.
        let ready = unsafe { libc::poll(&mut pollfd, 1, -1) };
        if ready < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        // A readable, closed, or invalid descriptor all mean the loop should look.
        return;
    }
}

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
}
