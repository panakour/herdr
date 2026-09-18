use super::*;

const DETACH_FLUSH_TIMEOUT: Duration = Duration::from_millis(250);
/// A fresh server accepts clients within tens of milliseconds. Waiting here keeps
/// the first reconnect attempt from missing it and falling into retry backoff.
const STARTED_SERVER_READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Moves this client-owned shell from the current Local session to `session`.
///
/// The old server only asked; the client owns the reconnect. Local keeps its
/// endpoint identity, so the existing reconnect and activation path brings the
/// new session's snapshot and surface up exactly like a replaced Local server.
#[allow(clippy::too_many_arguments)]
pub(super) fn begin_local_session_switch(
    state: &mut ClientState,
    endpoints: &mut endpoint::EndpointRegistry,
    endpoint_commands: &mut endpoint_commands::EndpointCommands,
    supervisors: &mut endpoint::EndpointSupervisors,
    pending_activation: &mut Option<endpoint::PendingEndpointActivation>,
    endpoint_id: &endpoint::ClientEndpointId,
    generation: u64,
    session: &str,
    startup_cwd: Option<&str>,
    now: std::time::Instant,
) -> Result<String, String> {
    if !endpoint_id.is_local() {
        return Err("only the Local endpoint can switch sessions".into());
    }
    if state.shell.is_none() {
        return Err("direct terminal attaches cannot switch sessions".into());
    }
    if handshake::is_remote_client_process() {
        return Err("remote clients cannot switch local sessions".into());
    }
    let target = crate::session::switch_active_session(session)?;
    let label = crate::session::display_name(target.as_deref()).to_owned();
    let socket_path = client_socket_path();
    let startup_cwd = startup_cwd
        .map(crate::worktree::expand_tilde_path)
        .filter(|path| path.is_dir());
    match crate::server::autodetect::spawn_server_daemon_if_needed(
        &socket_path,
        startup_cwd.as_deref(),
    ) {
        Ok(true) => {
            info!(session = %label, "started server for requested session");
            if let Err(error) = crate::server::autodetect::wait_for_server_socket(
                &socket_path,
                STARTED_SERVER_READY_TIMEOUT,
            ) {
                warn!(%error, session = %label, "started server is not ready yet; reconnecting");
            }
        }
        Ok(false) => {}
        Err(error) => {
            warn!(%error, session = %label, "failed to start server for requested session")
        }
    }

    // Leave the old server as a clean detach, then repoint Local at the new socket.
    if matches!(
        endpoints.send_to(endpoint_id, &ClientMessage::Detach),
        endpoint::EndpointSendOutcome::Sent
    ) {
        let _ = endpoints.flush_to(endpoint_id, now + DETACH_FLUSH_TIMEOUT);
    }
    handle_endpoint_disconnect(
        state,
        endpoints,
        endpoint_commands,
        supervisors,
        pending_activation,
        endpoint_id,
        generation,
        now,
        &format!("is switching to session {label}"),
    );
    endpoints.disconnect(endpoint_id);
    supervisors.add_local(socket_path, None, now);
    if let Some(shell) = state.shell.as_mut() {
        let endpoint_label = if target.is_some() {
            label.clone()
        } else {
            "Local".into()
        };
        shell.set_local_endpoint_label(endpoint_label);
        shell.set_endpoint_status(endpoint_id, endpoint::ClientEndpointStatus::Connecting);
    }
    Ok(label)
}
