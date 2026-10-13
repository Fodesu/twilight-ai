use super::effect::Effect;
use super::event::{AppEvent, UiAction};
use super::state::{AppState, ConnectionPhase, ObservationPhase, SessionView};
use super::{commands, streaming};
use chat_client::{config::HostConfig, ClientError, ErrorKind};
use chat_protocol::Capabilities;

pub fn update(state: &mut AppState, event: AppEvent) -> Vec<Effect> {
    if !matches!(event, AppEvent::Tick) {
        state.revision += 1;
    }
    match event {
        AppEvent::Ui(action) => ui(state, action),
        AppEvent::Connected { epoch, result } => connected(state, epoch, result),
        AppEvent::Snapshot { epoch, result } => streaming::snapshot(state, epoch, result),
        AppEvent::StreamOpened { epoch } => stream_opened(state, epoch),
        AppEvent::Server { epoch, event } => streaming::server(state, epoch, event),
        AppEvent::StreamFailed { epoch, error } => stream_failed(state, epoch, error),
        AppEvent::CommandStarted { command } => commands::started(state, command),
        AppEvent::CommandResult { id, status } => commands::resolved(state, &id, status),
        AppEvent::RetryConnection { epoch } => retry_connection(state, epoch),
        AppEvent::RetryStream { epoch } => retry_stream(state, epoch),
        AppEvent::SettingsLoaded { config, error } => settings_loaded(state, config, error),
        AppEvent::Tick => tick(state),
    }
}

fn settings_loaded(
    state: &mut AppState,
    config: Option<HostConfig>,
    error: Option<String>,
) -> Vec<Effect> {
    if let Some(error) = error {
        state.connection.phase = ConnectionPhase::Incompatible;
        state.ui.notice = Some(error);
    }
    config
        .map(|config| configure(state, config))
        .unwrap_or_default()
}

fn ui(state: &mut AppState, action: UiAction) -> Vec<Effect> {
    consume_local_command(state, &action);
    match action {
        UiAction::Edit(text) => edit(state, text),
        UiAction::Send => return commands::submit(state),
        UiAction::Stop => return commands::control(state, super::effect::Control::Stop),
        UiAction::Resume => return commands::control(state, super::effect::Control::Resume),
        UiAction::Configure(config) => return configure(state, config),
        UiAction::OpenHost => state.ui.host_editor = true,
        UiAction::CloseHost => state.ui.host_editor = false,
        UiAction::Disconnect => return disconnect(state),
        UiAction::Refresh => state.session.dirty = true,
        UiAction::ToggleEvents => state.ui.events = !state.ui.events,
        UiAction::Quit => state.quit = true,
    }
    Vec::new()
}

fn consume_local_command(state: &mut AppState, action: &UiAction) {
    let literal = match action {
        UiAction::OpenHost => Some("/host"),
        UiAction::Disconnect => Some("/disconnect"),
        UiAction::ToggleEvents => Some("/events"),
        UiAction::Refresh => Some("/refresh"),
        _ => None,
    };
    if literal == Some(state.ui.draft.trim()) {
        edit(state, String::new());
    }
}

fn edit(state: &mut AppState, text: String) {
    state.ui.draft = text;
    state.ui.draft_revision += 1;
}

fn configure(state: &mut AppState, mut config: HostConfig) -> Vec<Effect> {
    if let Err(error) = config.validate() {
        state.ui.notice = Some(error.to_string());
        return Vec::new();
    }
    state.connection.epoch += 1;
    let epoch = state.connection.epoch;
    if config.session.is_none() {
        config.session = Some(format!("chat-{}-{epoch}", state.instance));
    }
    state.connection.config = config.clone();
    state.connection.phase = ConnectionPhase::Connecting;
    state.connection.capabilities = None;
    state.observation = ObservationPhase::Idle;
    state.session = SessionView::default();
    state.ui.host_editor = false;
    state.ui.notice = Some("Connecting Host".into());
    vec![Effect::Connect { epoch, config }]
}

fn connected(
    state: &mut AppState,
    epoch: u64,
    result: Result<Capabilities, ClientError>,
) -> Vec<Effect> {
    if epoch != state.connection.epoch {
        return Vec::new();
    }
    match result {
        Ok(capabilities) => {
            state.connection.capabilities = Some(capabilities);
            state.connection.phase = ConnectionPhase::Ready;
            state.session.watch_needed = true;
            state.ui.notice = Some("Host ready; commands available".into());
            request_snapshot(state)
        }
        Err(error) => connection_failed(state, epoch, error),
    }
}

fn connection_failed(state: &mut AppState, epoch: u64, error: ClientError) -> Vec<Effect> {
    state.ui.notice = Some(error.message);
    if matches!(
        error.kind,
        ErrorKind::Incompatible | ErrorKind::Unauthorized | ErrorKind::Protocol
    ) {
        state.connection.phase = ConnectionPhase::Incompatible;
        return Vec::new();
    }
    state.connection.phase = ConnectionPhase::Unavailable;
    vec![Effect::After {
        milliseconds: 1500,
        event: Box::new(AppEvent::RetryConnection { epoch }),
    }]
}

fn retry_connection(state: &mut AppState, epoch: u64) -> Vec<Effect> {
    if epoch != state.connection.epoch || state.connection.phase != ConnectionPhase::Unavailable {
        return Vec::new();
    }
    state.connection.phase = ConnectionPhase::Connecting;
    vec![Effect::Connect {
        epoch,
        config: state.connection.config.clone(),
    }]
}

fn disconnect(state: &mut AppState) -> Vec<Effect> {
    state.connection.epoch += 1;
    state.connection.phase = ConnectionPhase::Unconfigured;
    state.connection.capabilities = None;
    state.observation = ObservationPhase::Idle;
    state.session.active_effects.clear();
    state.session.settling_effects.clear();
    state.ui.notice = Some("Disconnected; /host selects a connection".into());
    vec![Effect::Disconnect]
}

fn stream_opened(state: &mut AppState, epoch: u64) -> Vec<Effect> {
    if epoch == state.connection.epoch {
        state.observation = ObservationPhase::Live;
    }
    Vec::new()
}

fn stream_failed(state: &mut AppState, epoch: u64, error: ClientError) -> Vec<Effect> {
    if epoch != state.connection.epoch {
        return Vec::new();
    }
    state.observation = ObservationPhase::Retrying;
    streaming::reset_transient(state);
    state.log(format!("Event stream: {}", error.message));
    vec![Effect::After {
        milliseconds: 1000,
        event: Box::new(AppEvent::RetryStream { epoch }),
    }]
}

fn retry_stream(state: &mut AppState, epoch: u64) -> Vec<Effect> {
    if epoch != state.connection.epoch || state.connection.phase != ConnectionPhase::Ready {
        return Vec::new();
    }
    state.session.watch_needed = true;
    request_snapshot(state)
}

pub(super) fn request_snapshot(state: &mut AppState) -> Vec<Effect> {
    if state.session.snapshot_inflight {
        return Vec::new();
    }
    state.session.snapshot_inflight = true;
    vec![Effect::Snapshot {
        epoch: state.connection.epoch,
    }]
}

fn tick(state: &mut AppState) -> Vec<Effect> {
    let mut effects = commands::reconcile_pending(state);
    if state.connection.phase != ConnectionPhase::Ready {
        return effects;
    }
    let poll =
        !state.session.loaded || state.session.dirty || state.observation != ObservationPhase::Live;
    if poll {
        effects.extend(request_snapshot(state));
    }
    effects
}

#[cfg(test)]
mod tests {
    use super::*;
    use chat_protocol::*;

    fn ready() -> AppState {
        let mut state = AppState::new("instance".into());
        update(
            &mut state,
            AppEvent::Ui(UiAction::Configure(HostConfig::default())),
        );
        update(
            &mut state,
            AppEvent::Connected {
                epoch: 1,
                result: Ok(Capabilities {
                    version: VERSION,
                    submit: true,
                    stop: true,
                    resume: true,
                    approvals: false,
                }),
            },
        );
        state.session.snapshot_inflight = false;
        state
    }

    fn draft(state: &mut AppState, text: &str) {
        update(state, AppEvent::Ui(UiAction::Edit(text.into())));
    }

    fn act(state: &mut AppState, action: UiAction) -> Vec<Effect> {
        update(state, AppEvent::Ui(action))
    }

    fn resolve(state: &mut AppState, status: CommandStatus) {
        let id = state.command_order.last().unwrap().clone();
        update(state, AppEvent::CommandResult { id, status });
    }

    fn key() -> EffectKey {
        EffectKey {
            run_id: "run".into(),
            effect_id: "effect".into(),
        }
    }

    fn descriptor() -> ActiveEffect {
        ActiveEffect {
            key: key(),
            kind: EffectKind::Model,
            label: "model".into(),
        }
    }

    fn frame(state: &mut AppState, generation: i64, sequence: u64, text: &str) {
        let event = ServerEvent::Progress {
            session: state.connection.config.session.clone().unwrap(),
            key: key(),
            generation,
            sequence,
            update: ProgressKind::TextDelta,
            text: text.into(),
        };
        update(state, AppEvent::Server { epoch: 1, event });
    }

    fn commit(state: &mut AppState, index: u32, started: Vec<ActiveEffect>, ended: Vec<EffectKey>) {
        let event = ServerEvent::Committed {
            session: state.connection.config.session.clone().unwrap(),
            cursor: Cursor { commit: 5, index },
            effects_started: started,
            effects_ended: ended,
            ended_runs: Vec::new(),
        };
        update(state, AppEvent::Server { epoch: 1, event });
    }

    fn snapshot(state: &mut AppState, next_commit: u64) {
        let value = Snapshot {
            version: VERSION,
            session: state.connection.config.session.clone().unwrap(),
            next_commit,
            ..Default::default()
        };
        update(
            state,
            AppEvent::Snapshot {
                epoch: 1,
                result: Ok(value),
            },
        );
    }

    #[test]
    fn durable_receipt_clears_matching_draft_only() {
        let mut state = ready();
        draft(&mut state, "first");
        act(&mut state, UiAction::Send);
        assert_eq!(state.ui.draft, "first");
        resolve(&mut state, CommandStatus::Accepted);
        assert_eq!(state.ui.draft, "");
        draft(&mut state, "second");
        resolve(&mut state, CommandStatus::Applied);
        assert_eq!(state.ui.draft, "second");
    }

    #[test]
    fn newer_draft_survives_earlier_receipt() {
        let mut state = ready();
        draft(&mut state, "first");
        act(&mut state, UiAction::Send);
        draft(&mut state, "second");
        resolve(&mut state, CommandStatus::Applied);
        assert_eq!(state.ui.draft, "second");
    }

    #[test]
    fn unknown_is_read_only_reconciled_with_stable_ids() {
        let mut state = ready();
        draft(&mut state, "uncertain");
        act(&mut state, UiAction::Send);
        resolve(&mut state, CommandStatus::Unknown("lost response".into()));
        let id = state.command_order[0].clone();
        let json = serde_json::to_value(&state.commands[&id].command).unwrap();
        assert_eq!(json["payload"]["inputId"], "input-instance-1");
        assert_eq!(state.ui.draft, "uncertain");
        assert!(act(&mut state, UiAction::Send).is_empty());
        let effects = update(&mut state, AppEvent::Tick);
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, Effect::Reconcile { id: value, .. } if value == &id)));
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::Post { .. })));
        resolve(&mut state, CommandStatus::Applied);
        assert_eq!(state.ui.draft, "");
        assert!(state.session.turns.is_empty());
    }

    #[test]
    fn progress_is_temporary_and_never_advances_cursor() {
        let mut state = ready();
        commit(&mut state, 0, vec![descriptor()], Vec::new());
        let before = state.session.cursor;
        frame(&mut state, 0, 0, "hello");
        frame(&mut state, 0, 0, "duplicate");
        assert_eq!(state.session.active_effects[&key()].text, "hello");
        assert_eq!(state.session.cursor, before);
        assert!(state.session.transcript.is_empty());
    }

    #[test]
    fn settled_effect_ignores_late_progress() {
        let mut state = ready();
        commit(&mut state, 0, vec![descriptor()], Vec::new());
        frame(&mut state, 1, 1, "temporary");
        commit(&mut state, 1, Vec::new(), vec![key()]);
        assert_eq!(state.session.settling_effects[&key()].text, "temporary");
        frame(&mut state, 2, 3, "late");
        assert!(state.session.active_effects.is_empty());
        assert_eq!(state.session.settling_effects[&key()].text, "temporary");
    }

    #[test]
    fn stale_snapshot_cannot_rewind_observed_state() {
        let mut state = ready();
        commit(&mut state, 0, vec![descriptor()], Vec::new());
        snapshot(&mut state, 5);
        assert!(!state.session.loaded);
        assert!(state.session.dirty);
        assert!(state.session.active_effects.contains_key(&key()));
    }

    #[test]
    fn completed_effect_remains_visible_until_snapshot_consolidates_it() {
        let mut state = ready();
        commit(&mut state, 0, vec![descriptor()], Vec::new());
        frame(&mut state, 1, 1, "final answer");
        commit(&mut state, 1, Vec::new(), vec![key()]);
        assert!(state.session.active_effects.is_empty());
        assert_eq!(state.session.settling_effects[&key()].text, "final answer");
        snapshot(&mut state, 7);
        assert!(state.session.settling_effects.is_empty());
    }
}
