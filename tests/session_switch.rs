//! Integration tests for moving an attached client between named sessions.

#![cfg(unix)]

pub mod support;
#[path = "support/terminal_screen.rs"]
mod terminal_screen;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use serde_json::Value;
use support::{
    cleanup_test_base, register_runtime_dir, register_spawned_herdr_pid,
    unregister_spawned_herdr_pid, wait_for_socket, wait_until,
};

fn unique_test_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Unix socket paths are short-limited; keep the base short so
    // `sessions/<name>/herdr-client.sock` still fits under it.
    PathBuf::from(format!(
        "/tmp/hsw-{}-{}",
        std::process::id(),
        nanos % 1_000_000_000
    ))
}

fn app_dir_name() -> &'static str {
    if cfg!(debug_assertions) {
        "herdr-dev"
    } else {
        "herdr"
    }
}

struct SpawnedHerdr {
    master: Option<Box<dyn MasterPty + Send>>,
    child: Box<dyn Child + Send + Sync>,
}

impl Drop for SpawnedHerdr {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        drop(self.master.take());

        if let Some(pid) = pid {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                let mut status = 0;
                let result =
                    unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
                if result == pid as libc::pid_t || result == -1 {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            unregister_spawned_herdr_pid(Some(pid));
        }
    }
}

fn test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Session layout under an isolated config home: the default session lives in
/// the config directory and named sessions under `sessions/<name>`.
struct Sessions {
    base: PathBuf,
    config_home: PathBuf,
    runtime_dir: PathBuf,
}

impl Sessions {
    fn new() -> Self {
        let base = unique_test_dir();
        let config_home = base.join("config");
        let runtime_dir = base.join("runtime");
        fs::create_dir_all(config_home.join(app_dir_name())).unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        register_runtime_dir(&runtime_dir);
        fs::write(
            config_home.join(app_dir_name()).join("config.toml"),
            "onboarding = false\n\n[ui]\nsidebar_start_collapsed = true\nsidebar_collapsed_mode = \"hidden\"\nhide_tab_bar_when_single_tab = true\npane_scrollbars = false\n",
        )
        .unwrap();
        Self {
            base,
            config_home,
            runtime_dir,
        }
    }

    fn data_dir(&self, session: Option<&str>) -> PathBuf {
        let config_dir = self.config_home.join(app_dir_name());
        match session {
            Some(name) => config_dir.join("sessions").join(name),
            None => config_dir,
        }
    }

    fn api_socket(&self, session: Option<&str>) -> PathBuf {
        self.data_dir(session).join("herdr.sock")
    }

    fn client_socket(&self, session: Option<&str>) -> PathBuf {
        self.data_dir(session).join("herdr-client.sock")
    }

    fn command(&self, args: &[&str], session: Option<&str>) -> CommandBuilder {
        let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
        cmd.args(args);
        cmd.env("HERDR_DISABLE_SOUND", "1");
        cmd.env("XDG_CONFIG_HOME", &self.config_home);
        cmd.env("XDG_STATE_HOME", self.runtime_dir.join("state"));
        cmd.env("XDG_RUNTIME_DIR", &self.runtime_dir);
        cmd.env(
            "HERDR_CONFIG_PATH",
            self.config_home.join(app_dir_name()).join("config.toml"),
        );
        cmd.env("SHELL", "/bin/sh");
        cmd.env("TERM", "xterm-256color");
        cmd.env_remove("HERDR_SOCKET_PATH");
        cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
        cmd.env_remove("HERDR_ENV");
        match session {
            Some(name) => cmd.env("HERDR_SESSION", name),
            None => cmd.env_remove("HERDR_SESSION"),
        }
        cmd
    }

    fn spawn(&self, args: &[&str], session: Option<&str>) -> SpawnedHerdr {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let child = pair
            .slave
            .spawn_command(self.command(args, session))
            .unwrap();
        register_spawned_herdr_pid(child.process_id());
        drop(pair.slave);
        SpawnedHerdr {
            master: Some(pair.master),
            child,
        }
    }

    fn spawn_server(&self, session: Option<&str>) -> SpawnedHerdr {
        let server = self.spawn(&["server"], session);
        wait_for_socket(&self.api_socket(session), Duration::from_secs(10));
        wait_for_socket(&self.client_socket(session), Duration::from_secs(10));
        server
    }

    /// Opens a workspace in `session` whose shell prints `marker`, so a client
    /// surface from that session is recognizable on screen.
    fn seed_marker(&self, session: Option<&str>, marker: &str) {
        let api = self.api_socket(session);
        let created = send_json_request(
            &api,
            &serde_json::json!({
                "id": "workspace", "method": "workspace.create",
                "params": {"cwd": &self.base, "focus": true, "label": marker},
            })
            .to_string(),
        );
        let pane_id = created["result"]["root_pane"]["pane_id"]
            .as_str()
            .unwrap_or_else(|| panic!("workspace.create failed: {created}"));
        let response = send_json_request(
            &api,
            &serde_json::json!({
                "id": "seed", "method": "pane.send_input",
                "params": {"pane_id": pane_id, "text": format!("printf '{marker}\\n'"), "keys": ["Enter"]},
            })
            .to_string(),
        );
        assert_eq!(response["result"]["type"], "ok", "{response}");
    }

    fn cleanup(self) {
        cleanup_test_base(&self.base);
    }
}

fn send_json_request(socket_path: &Path, request: &str) -> Value {
    let mut stream = UnixStream::connect(socket_path).expect("should connect to API socket");
    writeln!(stream, "{}", request).unwrap();
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    serde_json::from_str(&response).expect("response should be valid JSON")
}

fn switch_client(api: &Path, session: &str, client_id: Option<u64>) -> Value {
    send_json_request(
        api,
        &serde_json::json!({
            "id": "switch", "method": "client.session.switch",
            "params": {"session": session, "client_id": client_id},
        })
        .to_string(),
    )
}

/// `client.window_title.set` reports whether the server currently has a
/// foreground client, which is the cheapest attached-client probe over the API.
fn has_foreground_client(api: &Path) -> bool {
    let response = send_json_request(
        api,
        r#"{"id":"probe","method":"client.window_title.set","params":{"title":"probe"}}"#,
    );
    response["result"]["reason"] == "set"
}

type SharedOutput = Arc<Mutex<Vec<u8>>>;

fn spawn_pty_drain(mut reader: Box<dyn Read + Send>) -> SharedOutput {
    let output: SharedOutput = Arc::new(Mutex::new(Vec::new()));
    let thread_output = output.clone();
    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => thread_output
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend_from_slice(&buf[..n]),
            }
        }
    });
    output
}

fn screen_text(output: &SharedOutput) -> String {
    let bytes = output.lock().unwrap_or_else(|p| p.into_inner()).clone();
    terminal_screen::text(&bytes, 80, 24)
}

fn wait_for_screen(output: &SharedOutput, marker: &str, what: &str) {
    assert!(
        wait_until(Duration::from_secs(15), Duration::from_millis(25), || {
            screen_text(output).contains(marker)
        }),
        "{what}; screen:\n{}",
        screen_text(output)
    );
}

fn attach_client(sessions: &Sessions) -> (SpawnedHerdr, SharedOutput) {
    let client = sessions.spawn(&["client"], None);
    let output = spawn_pty_drain(client.master.as_ref().unwrap().try_clone_reader().unwrap());
    (client, output)
}

#[test]
fn switching_moves_only_the_requested_client_to_a_running_session() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let default_api = sessions.api_socket(None);
    let work_api = sessions.api_socket(Some("work"));

    let _default_server = sessions.spawn_server(None);
    let _work_server = sessions.spawn_server(Some("work"));
    sessions.seed_marker(None, "DEFAULT_SESSION_FRAME");
    sessions.seed_marker(Some("work"), "WORK_SESSION_FRAME");

    let (client, output) = attach_client(&sessions);
    wait_for_screen(
        &output,
        "DEFAULT_SESSION_FRAME",
        "client must attach to the default session",
    );
    let mut input = client.master.as_ref().unwrap().take_writer().unwrap();
    input
        .write_all(b"export SWITCH_STATE=retained; printf 'STATE_%s\\n' ready\r")
        .unwrap();
    wait_for_screen(
        &output,
        "STATE_ready",
        "source shell state must be initialized",
    );
    let (bystander, bystander_output) = attach_client(&sessions);
    wait_for_screen(
        &bystander_output,
        "DEFAULT_SESSION_FRAME",
        "second client must attach too",
    );
    assert!(has_foreground_client(&default_api));
    assert!(!has_foreground_client(&work_api));

    // Typing in the first client makes it the foreground client, which is what
    // a request without an explicit client id targets.
    input.write_all(b"\x7f").unwrap();
    thread::sleep(Duration::from_millis(300));
    let response = switch_client(&default_api, "work", None);
    assert_eq!(
        response["result"]["type"], "client_session_switch",
        "{response}"
    );
    assert_eq!(response["result"]["accepted"], true, "{response}");
    assert_eq!(response["result"]["reason"], "requested", "{response}");
    assert!(response["result"]["client_id"].is_u64(), "{response}");
    assert_eq!(response["result"]["session"], "work", "{response}");

    wait_for_screen(
        &output,
        "WORK_SESSION_FRAME",
        "switched client must show the work session",
    );
    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(50), || {
            has_foreground_client(&work_api)
        }),
        "work session must gain the switched client"
    );
    assert!(
        has_foreground_client(&default_api),
        "the bystander client must remain attached to the default session"
    );
    assert!(
        !screen_text(&bystander_output).contains("WORK_SESSION_FRAME"),
        "the bystander client must not switch"
    );

    // Requests for the session a server already owns are rejected, as are unknown clients.
    let response = switch_client(&work_api, "work", None);
    assert_eq!(response["error"]["code"], "same_session", "{response}");
    let response = switch_client(&default_api, "work", Some(99));
    assert_eq!(response["error"]["code"], "client_not_found", "{response}");

    // Return to the original session and prove its shell state and input survive.
    let response = switch_client(&work_api, "default", None);
    assert_eq!(response["result"]["accepted"], true, "{response}");
    wait_for_screen(
        &output,
        "DEFAULT_SESSION_FRAME",
        "client must return to its source",
    );
    // The surface can be painted before the presentation-effects fence opens
    // input. Retry the harmless probe until the normal activation is complete.
    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(100), || {
            input
                .write_all(b"printf 'ROUND_%s\\n' \"$SWITCH_STATE\"\r")
                .unwrap();
            screen_text(&output).contains("ROUND_retained")
        }),
        "original shell must survive and accept input"
    );

    drop(bystander);
    drop(client);
    sessions.cleanup();
}

#[test]
fn failed_target_start_keeps_the_source_attached_and_interactive() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let _server = sessions.spawn_server(None);
    sessions.seed_marker(None, "SOURCE_BEFORE_FAILURE");
    let (client, output) = attach_client(&sessions);
    wait_for_screen(&output, "SOURCE_BEFORE_FAILURE", "source must be attached");
    // A regular file prevents the target daemon from creating its session directory.
    let target = sessions.data_dir(Some("blocked"));
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "not a directory").unwrap();
    let response = switch_client(&sessions.api_socket(None), "blocked", None);
    assert_eq!(response["result"]["accepted"], true, "{response}");
    let mut input = client.master.as_ref().unwrap().take_writer().unwrap();
    input
        .write_all(b"printf 'DURING_%s\\n' preparation\r")
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(2), Duration::from_millis(25), || {
            screen_text(&output).contains("DURING_preparation")
        }),
        "source must remain responsive while startup is pending"
    );
    wait_for_screen(
        &output,
        "cannot switch to session blocked",
        "startup failure must be visible",
    );
    assert!(has_foreground_client(&sessions.api_socket(None)));
    input.write_all(b"printf 'AFTER_%s\\n' failure\r").unwrap();
    wait_for_screen(
        &output,
        "AFTER_failure",
        "source must accept input after failure",
    );
    drop(client);
    sessions.cleanup();
}

#[test]
fn popup_switch_uses_its_invoking_client_after_foreground_changes() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let invoked = sessions.base.join("invoked");
    let release = sessions.base.join("release");
    let config_path = sessions
        .config_home
        .join(app_dir_name())
        .join("config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(&config_path, format!("{config}\n[keys]\nprefix = \"ctrl+a\"\n\n[[keys.command]]\nkey = \"prefix+p\"\ntype = \"popup\"\ncommand = '''printf '%s' \"$HERDR_CLIENT_ID\" > '{}'; while [ ! -f '{}' ]; do sleep 0.02; done; \"$HERDR_BIN_PATH\" session switch work'''\n", invoked.display(), release.display())).unwrap();
    let _source = sessions.spawn_server(None);
    let _target = sessions.spawn_server(Some("work"));
    sessions.seed_marker(None, "PICKER_SOURCE");
    sessions.seed_marker(Some("work"), "PICKER_TARGET");
    let (client, output) = attach_client(&sessions);
    wait_for_screen(&output, "PICKER_SOURCE", "picker client must attach");
    let (bystander, bystander_output) = attach_client(&sessions);
    wait_for_screen(&bystander_output, "PICKER_SOURCE", "bystander must attach");
    client
        .master
        .as_ref()
        .unwrap()
        .take_writer()
        .unwrap()
        .write_all(b"\x01p")
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(25), || {
            fs::read_to_string(&invoked)
                .ok()
                .is_some_and(|id| id.parse::<u64>().is_ok())
        }),
        "popup must receive the invoking client id"
    );
    bystander
        .master
        .as_ref()
        .unwrap()
        .take_writer()
        .unwrap()
        .write_all(b"\x7f")
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    fs::write(release, "go").unwrap();
    wait_for_screen(
        &output,
        "PICKER_TARGET",
        "popup must switch its original client",
    );
    assert!(has_foreground_client(&sessions.api_socket(None)));
    assert!(!screen_text(&bystander_output).contains("PICKER_TARGET"));
    drop(bystander);
    drop(client);
    sessions.cleanup();
}

#[test]
fn stalled_target_handshake_keeps_source_input_and_times_out() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let _source = sessions.spawn_server(None);
    let _target = sessions.spawn_server(Some("stalled"));
    sessions.seed_marker(None, "HANDSHAKE_SOURCE");
    let (client, output) = attach_client(&sessions);
    wait_for_screen(&output, "HANDSHAKE_SOURCE", "source must attach");
    // Keep the target status API alive, but replace its client listener with
    // one that accepts Hello and never sends Welcome.
    let socket = sessions.client_socket(Some("stalled"));
    fs::rename(&socket, socket.with_extension("original")).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let (hello_tx, hello_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        // The readiness probe connects and immediately closes.
        let (probe, _) = listener.accept().unwrap();
        drop(probe);
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut hello = [0; 1];
        stream.read_exact(&mut hello).unwrap();
        hello_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(Duration::from_secs(15));
    });
    let response = switch_client(&sessions.api_socket(None), "stalled", None);
    assert_eq!(response["result"]["accepted"], true);
    hello_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut input = client.master.as_ref().unwrap().take_writer().unwrap();
    input
        .write_all(b"printf 'HANDSHAKE_%s\\n' responsive\r")
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(2), Duration::from_millis(25), || {
            screen_text(&output).contains("HANDSHAKE_responsive")
        }),
        "input must not wait for the handshake timeout"
    );
    wait_for_screen(
        &output,
        "cannot switch to session stalled",
        "handshake must time out visibly",
    );
    assert!(has_foreground_client(&sessions.api_socket(None)));
    input
        .write_all(b"printf 'TIMEOUT_%s\\n' recovered\r")
        .unwrap();
    wait_for_screen(
        &output,
        "TIMEOUT_recovered",
        "source must remain usable after timeout",
    );
    let _ = release_tx.send(());
    worker.join().unwrap();
    drop(client);
    sessions.cleanup();
}

#[test]
fn transient_target_status_failure_is_retried() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let _source = sessions.spawn_server(None);
    let _target = sessions.spawn_server(Some("retry"));
    sessions.seed_marker(None, "RETRY_SOURCE");
    sessions.seed_marker(Some("retry"), "RETRY_TARGET");
    let (client, output) = attach_client(&sessions);
    wait_for_screen(&output, "RETRY_SOURCE", "source must attach");
    let api = sessions.api_socket(Some("retry"));
    let original = api.with_extension("original");
    fs::rename(&api, &original).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&api).unwrap();
    let worker = thread::spawn(move || {
        for attempt in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut request)
                .unwrap();
            if attempt == 0 {
                continue;
            } // Empty response during a listener replacement.
            let reply = send_json_request(&original, &request);
            writeln!(stream, "{reply}").unwrap();
        }
    });
    let response = switch_client(&sessions.api_socket(None), "retry", None);
    assert_eq!(response["result"]["accepted"], true);
    wait_for_screen(
        &output,
        "RETRY_TARGET",
        "temporary status failure must be retried",
    );
    worker.join().unwrap();
    drop(client);
    sessions.cleanup();
}

#[test]
fn switching_to_a_stopped_session_starts_its_server() {
    let _lock = test_lock();
    let sessions = Sessions::new();
    let default_api = sessions.api_socket(None);
    let side_api = sessions.api_socket(Some("side"));

    let _default_server = sessions.spawn_server(None);
    sessions.seed_marker(None, "DEFAULT_SESSION_FRAME");
    let (client, output) = attach_client(&sessions);
    wait_for_screen(
        &output,
        "DEFAULT_SESSION_FRAME",
        "client must attach to the default session",
    );
    assert!(!side_api.exists());

    // The CLI targets the server behind HERDR_SOCKET_PATH, as a custom command
    // would, and without HERDR_CLIENT_ID it moves the foreground client.
    let output_cli = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["session", "switch", "side", "--cwd"])
        .arg(&sessions.base)
        .env("XDG_CONFIG_HOME", &sessions.config_home)
        .env("XDG_STATE_HOME", sessions.runtime_dir.join("state"))
        .env("XDG_RUNTIME_DIR", &sessions.runtime_dir)
        .env("HERDR_SOCKET_PATH", &default_api)
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_CLIENT_ID")
        .output()
        .unwrap();
    assert!(
        output_cli.status.success(),
        "{}",
        String::from_utf8_lossy(&output_cli.stderr)
    );
    let response: Value = serde_json::from_slice(&output_cli.stdout).unwrap();
    assert_eq!(response["result"]["accepted"], true, "{response}");
    assert!(response["result"]["client_id"].is_u64(), "{response}");

    wait_for_socket(&side_api, Duration::from_secs(15));
    assert!(
        wait_until(Duration::from_secs(15), Duration::from_millis(50), || {
            has_foreground_client(&side_api)
        }),
        "the side session's new server must gain the switched client"
    );
    assert!(
        wait_until(Duration::from_secs(5), Duration::from_millis(50), || {
            !has_foreground_client(&default_api)
        }),
        "the default session must lose its only client"
    );
    // The server started by the switch seeded its first workspace from --cwd.
    let panes = send_json_request(
        &side_api,
        r#"{"id":"panes","method":"pane.list","params":{}}"#,
    );
    let expected_cwd = fs::canonicalize(&sessions.base).unwrap();
    assert_eq!(
        panes["result"]["panes"][0]["cwd"]
            .as_str()
            .map(PathBuf::from),
        Some(expected_cwd),
        "{panes}"
    );
    sessions.seed_marker(Some("side"), "SIDE_SESSION_FRAME");
    wait_for_screen(
        &output,
        "SIDE_SESSION_FRAME",
        "switched client must render the side session",
    );

    // The client started this server as a detached daemon, so stop it explicitly.
    drop(client);
    let response = send_json_request(
        &side_api,
        r#"{"id":"stop","method":"server.stop","params":{}}"#,
    );
    assert_eq!(response["result"]["type"], "ok", "{response}");
    assert!(
        wait_until(Duration::from_secs(10), Duration::from_millis(50), || {
            UnixStream::connect(&side_api).is_err()
        }),
        "side session server must stop"
    );
    sessions.cleanup();
}
