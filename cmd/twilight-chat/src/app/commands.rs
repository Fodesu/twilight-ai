use super::effect::{Control, Effect};
use super::state::{AppState, ConnectionPhase, PendingCommand, SubmissionPhase};
use chat_protocol::{Command, CommandBody, CommandStatus};

pub fn submit(state: &mut AppState) -> Vec<Effect> {
    let text = state.ui.draft.clone();
    if text.trim().is_empty() {
        return Vec::new();
    }
    if !state.can_submit() {
        return notice(state, "Configure a ready Host with /host; draft retained");
    }
    if same_draft_pending(state) {
        return notice(
            state,
            "Submission awaiting confirmation; reconciling its command ID",
        );
    }
    let id = next_id(state);
    let body = CommandBody::Submit {
        input_id: format!("input-{id}"),
        text,
    };
    let command = Command {
        id: id.clone(),
        body,
    };
    insert(state, command.clone(), Some(state.ui.draft_revision));
    state.ui.notice = Some(format!("Submitting {id}"));
    vec![Effect::Post {
        epoch: state.connection.epoch,
        command,
    }]
}

fn same_draft_pending(state: &AppState) -> bool {
    state.commands.values().any(|pending| {
        pending.epoch == state.connection.epoch
            && pending.draft_revision == Some(state.ui.draft_revision)
            && !pending.phase.terminal()
    })
}

pub fn control(state: &mut AppState, intent: Control) -> Vec<Effect> {
    if state.connection.phase != ConnectionPhase::Ready {
        return notice(state, "Connect a Host with /host first");
    }
    let supported = state
        .connection
        .capabilities
        .as_ref()
        .is_some_and(|caps| match intent {
            Control::Stop => caps.stop,
            Control::Resume => caps.resume,
        });
    if !supported {
        return notice(state, "Host does not support this command");
    }
    let literal = match intent {
        Control::Stop => "/stop",
        Control::Resume => "/resume",
    };
    let draft_revision = (state.ui.draft == literal).then_some(state.ui.draft_revision);
    if draft_revision.is_some() && same_draft_pending(state) {
        return notice(
            state,
            "Control awaiting confirmation; reconciling its command ID",
        );
    }
    let id = next_id(state);
    let body = match intent {
        Control::Stop => CommandBody::Stop {
            turn_id: String::new(),
            reason: "chat user stop".into(),
        },
        Control::Resume => CommandBody::Resume { turn_id: None },
    };
    insert(
        state,
        Command {
            id: id.clone(),
            body,
        },
        draft_revision,
    );
    vec![Effect::Control {
        epoch: state.connection.epoch,
        id,
        intent,
    }]
}

fn notice(state: &mut AppState, text: &str) -> Vec<Effect> {
    state.ui.notice = Some(text.into());
    Vec::new()
}

fn next_id(state: &mut AppState) -> String {
    state.next_command += 1;
    format!("{}-{}", state.instance, state.next_command)
}

fn insert(state: &mut AppState, command: Command, draft_revision: Option<u64>) {
    let id = command.id.clone();
    let pending = PendingCommand {
        command,
        epoch: state.connection.epoch,
        draft_revision,
        phase: SubmissionPhase::Queued,
        reconciliation_inflight: false,
    };
    state.commands.insert(id.clone(), pending);
    state.command_order.push(id);
    prune_resolved(state);
}

fn prune_resolved(state: &mut AppState) {
    if state.command_order.len() <= 64 {
        return;
    }
    let expired = state
        .command_order
        .iter()
        .find(|id| command_resolved(state, id))
        .cloned();
    if let Some(id) = expired {
        state.commands.remove(&id);
        state.command_order.retain(|key| key != &id);
    }
}

fn command_resolved(state: &AppState, id: &str) -> bool {
    state
        .commands
        .get(id)
        .is_some_and(|command| command.phase.terminal())
}

pub fn started(state: &mut AppState, command: Command) -> Vec<Effect> {
    let Some(pending) = state.commands.get_mut(&command.id) else {
        return Vec::new();
    };
    if pending.phase != SubmissionPhase::Queued {
        return Vec::new();
    }
    pending.phase = SubmissionPhase::Submitting;
    pending.command = command;
    Vec::new()
}

pub fn resolved(state: &mut AppState, id: &str, status: CommandStatus) -> Vec<Effect> {
    let Some(pending) = state.commands.get_mut(id) else {
        return Vec::new();
    };
    if pending.phase.terminal() {
        return Vec::new();
    }
    pending.reconciliation_inflight = false;
    pending.phase = match &status {
        CommandStatus::Accepted => SubmissionPhase::Accepted,
        CommandStatus::Applied => SubmissionPhase::Applied,
        CommandStatus::Rejected(reason) => SubmissionPhase::Rejected(reason.clone()),
        CommandStatus::Unknown(reason) => SubmissionPhase::Unknown(reason.clone()),
    };
    let current = pending.epoch == state.connection.epoch;
    let clear = current
        && matches!(status, CommandStatus::Accepted | CommandStatus::Applied)
        && pending.draft_revision == Some(state.ui.draft_revision)
        && pending.text() == Some(state.ui.draft.as_str());
    if clear {
        state.ui.draft.clear();
        state.ui.draft_revision += 1;
    }
    if current {
        state.session.dirty = true;
        state.ui.notice = Some(outcome(id, &status));
    }
    state.log(outcome(id, &status));
    Vec::new()
}

fn outcome(id: &str, status: &CommandStatus) -> String {
    match status {
        CommandStatus::Accepted => format!("Command {id} accepted; awaiting application"),
        CommandStatus::Applied => format!("Command {id} applied; Turn outcome follows"),
        CommandStatus::Rejected(reason) => format!("Command {id} rejected: {reason}"),
        CommandStatus::Unknown(reason) => {
            format!("Command {id} uncertain: {reason}; read-only reconciliation")
        }
    }
}

pub fn reconcile_pending(state: &mut AppState) -> Vec<Effect> {
    let mut effects = Vec::new();
    for (id, pending) in &mut state.commands {
        let posting = matches!(
            pending.phase,
            SubmissionPhase::Queued | SubmissionPhase::Submitting
        );
        if pending.phase.terminal() || pending.reconciliation_inflight || posting {
            continue;
        }
        pending.reconciliation_inflight = true;
        effects.push(Effect::Reconcile {
            epoch: pending.epoch,
            id: id.clone(),
        });
    }
    effects
}
