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
    client
        .master
        .as_ref()
        .unwrap()
        .take_writer()
        .unwrap()
        .write_all(b"\x7f")
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    let response = switch_client(&default_api, "work", None);
    assert_eq!(
        response["result"]["type"], "client_session_switch",
        "{response}"
    );
    assert_eq!(response["result"]["switched"], true, "{response}");
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

    drop(bystander);
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
    assert_eq!(response["result"]["switched"], true, "{response}");
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
