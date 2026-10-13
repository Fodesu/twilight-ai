use super::effect::Effect;
use super::state::{AppState, LiveEffect, ObservationPhase};
use chat_client::ClientError;
use chat_protocol::{ProgressKind, ServerEvent, Snapshot};
use std::collections::HashMap;

pub fn snapshot(
    state: &mut AppState,
    epoch: u64,
    result: Result<Snapshot, ClientError>,
) -> Vec<Effect> {
    if epoch != state.connection.epoch {
        return Vec::new();
    }
    state.session.snapshot_inflight = false;
    let snapshot = match result {
        Ok(value) => value,
        Err(error) => {
            state.session.error = Some(error.message);
            return Vec::new();
        }
    };
    if snapshot.session != state.connection.config.session.clone().unwrap_or_default() {
        return Vec::new();
    }
    if stale(state, &snapshot) {
        state.session.dirty = true;
        return Vec::new();
    }
    state.session.error = None;
    state.session.transcript = snapshot.transcript;
    state.session.turns = snapshot.turns;
    state.session.queued = snapshot.queued;
    state.session.unassigned = snapshot.unassigned;
    state.session.approvals = snapshot.approvals;
    merge_effects(state, snapshot.active_effects);
    // The durable projection is now the authority for every effect that was
    // waiting for completion. Drop the retained streaming tail only after the
    // transcript and turn projection have crossed the same snapshot barrier.
    state.session.settling_effects.clear();
    state.session.next_commit = snapshot.next_commit;
    state.session.loaded = true;
    state.session.dirty = false;
    state.session.revision += 1;
    if !state.session.watch_needed {
        return Vec::new();
    }
    state.session.watch_needed = false;
    state.observation = ObservationPhase::Connecting;
    let from = state
        .session
        .cursor
        .map(|cursor| cursor.commit)
        .unwrap_or(snapshot.next_commit);
    vec![Effect::Watch { epoch, from }]
}

fn stale(state: &AppState, snapshot: &Snapshot) -> bool {
    snapshot.next_commit < state.session.next_commit
        || state
            .session
            .cursor
            .is_some_and(|cursor| cursor.commit >= snapshot.next_commit)
}

fn merge_effects(state: &mut AppState, active: Vec<chat_protocol::ActiveEffect>) {
    let mut previous = std::mem::take(&mut state.session.active_effects);
    let mut current = HashMap::new();
    for descriptor in active {
        let key = descriptor.key.clone();
        let mut effect = previous
            .remove(&key)
            .unwrap_or_else(|| LiveEffect::new(descriptor.clone()));
        effect.descriptor = descriptor;
        current.insert(key, effect);
    }
    state.session.active_effects = current;
}

pub fn server(state: &mut AppState, epoch: u64, event: ServerEvent) -> Vec<Effect> {
    if epoch != state.connection.epoch
        || Some(event.session()) != state.connection.config.session.as_deref()
    {
        return Vec::new();
    }
    match event {
        ServerEvent::Committed {
            cursor,
            effects_started,
            effects_ended,
            ended_runs,
            ..
        } => {
            committed(state, cursor, effects_started, effects_ended, ended_runs);
        }
        ServerEvent::Progress {
            key,
            generation,
            sequence,
            update,
            text,
            ..
        } => {
            progress(state, key, generation, sequence, update, &text);
        }
    }
    Vec::new()
}

fn committed(
    state: &mut AppState,
    cursor: chat_protocol::Cursor,
    started: Vec<chat_protocol::ActiveEffect>,
    ended: Vec<chat_protocol::EffectKey>,
    ended_runs: Vec<String>,
) {
    if state.session.cursor.is_some_and(|last| cursor <= last) {
        return;
    }
    state.session.cursor = Some(cursor);
    state.log(format!("Committed {}.{}", cursor.commit, cursor.index));
    // A snapshot already contains these older replayed facts. They may advance
    // the replay cursor, but cannot resurrect an effect absent in that snapshot.
    if cursor.commit < state.session.next_commit {
        return;
    }
    state.session.dirty = true;
    for descriptor in started {
        let key = descriptor.key.clone();
        state
            .session
            .active_effects
            .entry(key)
            .or_insert_with(|| LiveEffect::new(descriptor));
    }
    for key in ended {
        move_to_settling(state, &key);
    }
    let ended_keys = state
        .session
        .active_effects
        .keys()
        .filter(|key| ended_runs.contains(&key.run_id))
        .cloned()
        .collect::<Vec<_>>();
    for key in ended_keys {
        move_to_settling(state, &key);
    }
}

fn move_to_settling(state: &mut AppState, key: &chat_protocol::EffectKey) {
    let Some(effect) = state.session.active_effects.remove(key) else {
        return;
    };
    state.session.settling_effects.insert(key.clone(), effect);
}

fn progress(
    state: &mut AppState,
    key: chat_protocol::EffectKey,
    generation: i64,
    sequence: u64,
    kind: ProgressKind,
    text: &str,
) {
    let Some(effect) = state.session.active_effects.get_mut(&key) else {
        return;
    };
    if obsolete(effect, generation, sequence) {
        return;
    }
    if effect.generation != Some(generation) || kind == ProgressKind::Reset {
        effect.text.clear();
    }
    effect.generation = Some(generation);
    effect.sequence = Some(sequence);
    if matches!(kind, ProgressKind::TextDelta | ProgressKind::ToolProgress) {
        append_bounded(&mut effect.text, text);
    }
}

fn obsolete(effect: &LiveEffect, generation: i64, sequence: u64) -> bool {
    let Some(current) = effect.generation else {
        return false;
    };
    generation < current
        || (generation == current && effect.sequence.is_some_and(|last| sequence <= last))
}

fn append_bounded(output: &mut String, text: &str) {
    let mut length = text
        .len()
        .min((256 * 1024usize).saturating_sub(output.len()));
    while !text.is_char_boundary(length) {
        length -= 1;
    }
    output.push_str(&text[..length]);
}

pub fn reset_transient(state: &mut AppState) {
    for effect in state.session.active_effects.values_mut() {
        effect.text.clear();
        effect.generation = None;
        effect.sequence = None;
    }
}
