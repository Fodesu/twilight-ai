use std::collections::{HashMap, HashSet};

use chat_protocol::{CommandBody, EffectKind, Message, MessageKind, Turn, TurnStatus};

use crate::app::state::{AppState, PendingCommand, SubmissionPhase};

#[derive(Clone, Debug)]
pub(crate) struct LiveEffect {
    pub(crate) kind: EffectKind,
    pub(crate) label: String,
    pub(crate) text: String,
}

#[derive(Clone, Debug)]
pub(crate) struct LiveTurn {
    pub(crate) id: String,
    pub(crate) messages: Vec<Message>,
    pub(crate) effects: Vec<LiveEffect>,
}

#[derive(Clone, Debug)]
pub(crate) struct CommittedTurn {
    pub(crate) id: String,
    pub(crate) messages: Vec<Message>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Presentation {
    pub(crate) pending: Vec<Message>,
    // The projection may retain more than one non-terminal source while a snapshot catches up,
    // but the terminal has exactly one mutable live owner. Rendering always consumes `active()`.
    pub(crate) live: Vec<LiveTurn>,
    pub(crate) committed: Vec<CommittedTurn>,
}

impl Presentation {
    pub(crate) fn active(&self) -> Option<&LiveTurn> {
        self.live.last()
    }
}

pub(crate) fn build(state: &AppState) -> Presentation {
    let turn_for_input = input_turns(state);
    let mut messages_by_turn = HashMap::<String, Vec<Message>>::new();
    let mut known_message_ids = HashSet::new();

    for message in &state.session.transcript {
        let turn_id = message_turn_id(message, &turn_for_input);
        if turn_id.is_empty() {
            known_message_ids.insert(message_key(message));
            continue;
        }
        add_message(
            &mut messages_by_turn,
            &mut known_message_ids,
            turn_id,
            message.clone(),
        );
    }
    state.session.turns.iter().for_each(|turn| {
        add_turn_items(turn, &mut messages_by_turn, &mut known_message_ids);
    });

    let mut presentation = Presentation::default();
    state.session.turns.iter().for_each(|turn| {
        let turn = make_turn(turn, &mut messages_by_turn);
        if is_terminal(turn.status) {
            presentation.committed.push(turn.committed);
        } else {
            presentation.live.push(turn.live);
        }
    });

    for (turn_id, mut messages) in messages_by_turn {
        sort_messages(&mut messages);
        presentation.live.push(LiveTurn {
            id: turn_id,
            messages,
            effects: Vec::new(),
        });
    }

    attach_effects(state, &mut presentation);
    presentation.pending = pending_messages(state, &known_message_ids, &turn_for_input);
    presentation
}

pub(crate) fn turn_key(turn: &CommittedTurn) -> String {
    if !turn.id.is_empty() {
        return format!("turn:{}", turn.id);
    }
    turn.messages
        .first()
        .map(message_key)
        .unwrap_or_else(|| "turn:empty".into())
}

struct TurnProjection {
    status: TurnStatus,
    committed: CommittedTurn,
    live: LiveTurn,
}

fn add_turn_items(
    turn: &Turn,
    messages_by_turn: &mut HashMap<String, Vec<Message>>,
    known_message_ids: &mut HashSet<String>,
) {
    turn.items.iter().for_each(|message| {
        let mut message = message.clone();
        if message.turn_id.is_empty() {
            message.turn_id = turn.id.clone();
        }
        add_message(
            messages_by_turn,
            known_message_ids,
            turn.id.clone(),
            message,
        );
    });
}

fn make_turn(turn: &Turn, messages_by_turn: &mut HashMap<String, Vec<Message>>) -> TurnProjection {
    let mut messages = messages_by_turn.remove(&turn.id).unwrap_or_default();
    sort_messages(&mut messages);
    TurnProjection {
        status: turn.status,
        committed: CommittedTurn {
            id: turn.id.clone(),
            messages: messages.clone(),
        },
        live: LiveTurn {
            id: turn.id.clone(),
            messages,
            effects: Vec::new(),
        },
    }
}

fn turn_input_pairs(turn: &Turn) -> impl Iterator<Item = (String, String)> + '_ {
    turn.input_ids
        .iter()
        .map(move |input_id| (input_id.clone(), turn.id.clone()))
}

fn input_turns(state: &AppState) -> HashMap<String, String> {
    state
        .session
        .turns
        .iter()
        .flat_map(turn_input_pairs)
        .collect()
}

fn message_turn_id(message: &Message, turn_for_input: &HashMap<String, String>) -> String {
    if !message.turn_id.is_empty() {
        return message.turn_id.clone();
    }
    turn_for_input.get(&message.id).cloned().unwrap_or_default()
}

fn add_message(
    messages_by_turn: &mut HashMap<String, Vec<Message>>,
    known_message_ids: &mut HashSet<String>,
    turn_id: String,
    message: Message,
) {
    let key = message_key(&message);
    if !known_message_ids.insert(key) {
        return;
    }
    messages_by_turn.entry(turn_id).or_default().push(message);
}

fn sort_messages(messages: &mut [Message]) {
    messages.sort_by_key(|message| {
        (
            message.position.commit,
            message.position.index,
            message_key(message),
        )
    });
}

fn live_run_indexes(state: &AppState, presentation: &Presentation) -> HashMap<String, usize> {
    let mut by_run = HashMap::new();
    if let Some(index) = presentation.live.len().checked_sub(1) {
        add_turn_runs(state, &presentation.live[index], index, &mut by_run);
    }
    by_run
}

fn add_turn_runs(
    state: &AppState,
    turn: &LiveTurn,
    index: usize,
    by_run: &mut HashMap<String, usize>,
) {
    turn.messages
        .iter()
        .filter(|message| !message.run_id.is_empty())
        .for_each(|message| {
            by_run.insert(message.run_id.clone(), index);
        });
    state
        .session
        .turns
        .iter()
        .find(|source| source.id == turn.id)
        .map(|source| by_run.insert(source.run_id.clone(), index));
}

fn attach_effect(
    effect: &crate::app::state::LiveEffect,
    presentation: &mut Presentation,
    by_run: &HashMap<String, usize>,
) {
    let view = LiveEffect {
        kind: effect.descriptor.kind,
        label: effect.descriptor.label.clone(),
        text: effect.text.clone(),
    };
    if let Some(&index) = by_run.get(&effect.descriptor.key.run_id) {
        if !has_durable_output(&presentation.live[index], &effect.descriptor.key.run_id) {
            presentation.live[index].effects.push(view);
        }
        return;
    }
    if let Some(index) = presentation.live.len().checked_sub(1) {
        presentation.live[index].effects.push(view);
        return;
    }
    presentation.live.push(LiveTurn {
        id: format!("effect:{}", effect.descriptor.key.run_id),
        messages: Vec::new(),
        effects: vec![view],
    });
}

fn attach_effects(state: &AppState, presentation: &mut Presentation) {
    let by_run = live_run_indexes(state, presentation);
    state
        .session
        .active_effects
        .values()
        .for_each(|effect| attach_effect(effect, presentation, &by_run));
    // A committed event ends the server-side effect before the consistent
    // snapshot containing its durable message arrives. Keep that last value in
    // the same live tail so completion is a handoff, not a blank frame.
    state
        .session
        .settling_effects
        .values()
        .for_each(|effect| attach_effect(effect, presentation, &by_run));
}

fn has_durable_output(turn: &LiveTurn, run_id: &str) -> bool {
    turn.messages.iter().any(|message| {
        matches!(
            message.kind,
            MessageKind::Assistant | MessageKind::ToolResult
        ) && message.run_id == run_id
    })
}

fn pending_messages(
    state: &AppState,
    known_message_ids: &HashSet<String>,
    turn_for_input: &HashMap<String, String>,
) -> Vec<Message> {
    let mut pending = Vec::new();
    let mut seen = known_message_ids.clone();
    for message in state
        .session
        .transcript
        .iter()
        .filter(|message| is_pending_input(message) && !input_assigned(message, turn_for_input))
    {
        if seen.insert(message_key(message)) {
            pending.push(message.clone());
        }
    }
    for message in state
        .session
        .queued
        .iter()
        .chain(state.session.unassigned.iter())
        .filter(|message| is_pending_input(message) && !input_assigned(message, turn_for_input))
    {
        if seen.insert(message_key(message)) {
            pending.push(message.clone());
        }
    }
    for command in state
        .command_order
        .iter()
        .filter_map(|id| state.commands.get(id))
        .filter_map(|command| pending_command_message(state, command, turn_for_input))
    {
        if seen.insert(message_key(&command)) {
            pending.push(command);
        }
    }
    pending
}

fn pending_command_message(
    state: &AppState,
    command: &PendingCommand,
    turn_for_input: &HashMap<String, String>,
) -> Option<Message> {
    let CommandBody::Submit { input_id, text } = &command.command.body else {
        return None;
    };
    if !matches!(
        command.phase,
        SubmissionPhase::Queued
            | SubmissionPhase::Submitting
            | SubmissionPhase::Accepted
            | SubmissionPhase::Applied
    ) || turn_for_input.contains_key(input_id)
        || state
            .session
            .transcript
            .iter()
            .any(|message| message.id == *input_id)
    {
        return None;
    }
    Some(Message {
        id: input_id.clone(),
        kind: MessageKind::Input,
        status: "submitted".into(),
        text: text.clone(),
        ..Message::default()
    })
}

fn input_assigned(message: &Message, turn_for_input: &HashMap<String, String>) -> bool {
    !message.turn_id.is_empty() || turn_for_input.contains_key(&message.id)
}

fn is_pending_input(message: &Message) -> bool {
    message.kind == MessageKind::Input && matches!(message.status.as_str(), "submitted" | "pending")
}

fn is_terminal(status: TurnStatus) -> bool {
    matches!(
        status,
        TurnStatus::Completed | TurnStatus::Failed | TurnStatus::Stopped | TurnStatus::Superseded
    )
}

fn message_key(message: &Message) -> String {
    if !message.id.is_empty() {
        return message.id.clone();
    }
    format!(
        "@{}:{}:{:?}:{}:{}",
        message.position.commit,
        message.position.index,
        message.kind,
        message.turn_id,
        message.run_id
    )
}
