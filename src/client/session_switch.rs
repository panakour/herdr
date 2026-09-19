use super::*;

/// Preparation owns no live endpoint state. Failed or stale preparations simply
/// drop their inactive connection, leaving the source session usable.
pub(super) struct PreparedSwitch {
    pub session: String,
    pub source_generation: u64,
    pub generation: u64,
    pub result: Result<endpoint::EndpointSupervisorEvent, String>,
}

pub(super) fn prepare(
    session: String,
    cwd: Option<String>,
    source_generation: u64,
    generation: u64,
    options: endpoint::EndpointConnectOptions,
    event_tx: tokio::sync::mpsc::Sender<ClientLoopEvent>,
) {
    tokio::spawn(async move {
        let target_session = session.clone();
        let result = tokio::task::spawn_blocking(move || {
            let target = crate::session::parse_target_name(&target_session)?;
            let cwd = cwd
                .map(|cwd| {
                    let path = crate::worktree::expand_tilde_path(&cwd);
                    let path = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
                    if !path.is_dir() {
                        return Err(format!("{} is not a directory", path.display()));
                    }
                    Ok(path)
                })
                .transpose()?;
            crate::server::autodetect::prepare_session_server(target.as_deref(), cwd.as_deref())
                .map_err(|error| error.to_string())?;
            endpoint::prepare_local_connection(
                crate::session::client_socket_path_for(target.as_deref()),
                options,
                generation,
            )
            .map_err(|error| error.to_string())
        })
        .await
        .unwrap_or_else(|error| Err(format!("session preparation task failed: {error}")));
        let _ = event_tx
            .send(ClientLoopEvent::SessionSwitchPrepared(Box::new(
                PreparedSwitch {
                    session,
                    source_generation,
                    generation,
                    result,
                },
            )))
            .await;
    });
}

/// Commit only after the target has accepted an inactive shell handshake.
// These arguments are the existing client-loop state holders; the transaction
// deliberately does not introduce a second owner for them.
#[allow(clippy::too_many_arguments)]
pub(super) fn commit(
    state: &mut ClientState,
    endpoints: &mut endpoint::EndpointRegistry,
    endpoint_commands: &mut endpoint_commands::EndpointCommands,
    supervisors: &mut endpoint::EndpointSupervisors,
    pending_activation: &mut Option<endpoint::PendingEndpointActivation>,
    session: &str,
    source_generation: u64,
    generation: u64,
) -> Result<bool, String> {
    let endpoint_id = endpoint::ClientEndpointId::Local;
    if !endpoints.accepts(&endpoint_id, source_generation) || pending_activation.is_some() {
        return Err("source connection changed while preparing the session; try again".into());
    }
    let target = crate::session::switch_active_session(session)?;
    let now = std::time::Instant::now();
    endpoints.detach_in_background(&endpoint_id);
    let local_was_active = handle_endpoint_disconnect(
        state,
        endpoints,
        endpoint_commands,
        supervisors,
        pending_activation,
        &endpoint_id,
        source_generation,
        now,
        &format!("is switching to session {session}"),
    );
    supervisors.add_local(
        crate::session::client_socket_path_for(target.as_deref()),
        Some(generation),
        now,
    );
    if let Some(shell) = state.shell.as_mut() {
        shell.set_local_endpoint_label(target.unwrap_or_else(|| "Local".into()));
        shell.set_endpoint_status(&endpoint_id, endpoint::ClientEndpointStatus::Connecting);
    }
    Ok(local_was_active)
}
