//! Unix listener readiness with owned descriptors and interruptible rearming.
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, OwnedFd};
use std::os::unix::net::UnixDatagram;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Pending {
    listener: Option<OwnedFd>,
    stopping: bool,
}

pub(crate) struct ClientAcceptWaker {
    pending: Arc<Mutex<Pending>>,
    wake: Option<UnixDatagram>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ClientAcceptWaker {
    pub(crate) fn spawn(notify: Arc<tokio::sync::Notify>) -> Self {
        let pending = Arc::new(Mutex::new(Pending::default()));
        let result = (|| -> io::Result<_> {
            let (wake, receiver) = UnixDatagram::pair()?;
            wake.set_nonblocking(true)?;
            receiver.set_nonblocking(true)?;
            let worker_pending = pending.clone();
            let worker = std::thread::Builder::new()
                .name("client-accept-waker".into())
                .spawn(move || watch(receiver, worker_pending, notify))?;
            Ok((wake, worker))
        })();
        match result {
            Ok((wake, worker)) => Self {
                pending,
                wake: Some(wake),
                worker: Some(worker),
            },
            Err(error) => {
                tracing::warn!(%error, "client accept waker unavailable; falling back to polling");
                Self {
                    pending,
                    wake: None,
                    worker: None,
                }
            }
        }
    }

    /// Replace any queued watch with the latest listener and interrupt poll.
    pub(crate) fn rearm(&self, listener: &crate::ipc::LocalListener) {
        if self.worker.is_none() {
            return;
        }
        let interprocess::local_socket::Listener::UdSocket(inner) = listener;
        match inner.as_fd().try_clone_to_owned() {
            Ok(fd) => {
                self.pending
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .listener = Some(fd);
                self.wake();
            }
            Err(error) => tracing::warn!(%error, "could not watch client listener"),
        }
    }

    fn wake(&self) {
        if let Some(wake) = &self.wake {
            // WouldBlock means a wake is already queued; the shared slot always
            // contains the newest descriptor, so no rearm can be lost.
            loop {
                match wake.send(&[1]) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() != io::ErrorKind::WouldBlock => {
                        tracing::warn!(%error, "client listener wake failed");
                    }
                    _ => {}
                }
                break;
            }
        }
    }
}

impl Drop for ClientAcceptWaker {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopping = true;
        self.wake();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn watch(wake: UnixDatagram, pending: Arc<Mutex<Pending>>, notify: Arc<tokio::sync::Notify>) {
    let mut listener: Option<OwnedFd> = None;
    loop {
        {
            let mut pending = pending.lock().unwrap_or_else(|p| p.into_inner());
            if pending.stopping {
                return;
            }
            if let Some(next) = pending.listener.take() {
                listener = Some(next);
            }
        }
        let mut fds = [
            libc::pollfd {
                fd: wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: listener.as_ref().map_or(-1, |fd| fd.as_raw_fd()),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: both descriptors are owned for the entire poll, and fds is
        // an exclusively borrowed array of exactly two initialized entries.
        // Bound shutdown even if the wake channel fails. Normal readiness and
        // rearming remain immediate; this is only a low-frequency fallback.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, 1000) };
        if ready < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
        if fds[0].revents != 0 {
            while wake.recv(&mut [0; 64]).is_ok() {}
            // Apply replacements/shutdown before considering old readiness.
            continue;
        }
        if fds[1].revents != 0 {
            listener = None;
            notify.notify_one();
        }
    }
}
