//! Scripted actions use the same reducer, effect runner and command tracking as TUI.
use crate::app::{
    event::UiAction,
    state::{ConnectionPhase, SubmissionPhase},
    Application,
};
use anyhow::{bail, Context, Result};
use chat_protocol::{CommandBody, Snapshot, TurnStatus, VERSION};
use std::time::{Duration, Instant};

pub struct Script {
    pub send: Vec<String>,
    pub resume: bool,
    pub stop: bool,
    pub seconds: u64,
}

pub async fn run(app: &mut Application, script: Script) -> Result<()> {
    let actions = !script.send.is_empty() || script.resume || script.stop;
    wait_ready(app, script.seconds, !actions).await?;
    for text in script.send {
        send(app, text, script.seconds).await?;
    }
    if script.resume {
        control(app, UiAction::Resume, script.seconds).await?;
    }
    if script.stop {
        control(app, UiAction::Stop, script.seconds).await?;
    }
    print_snapshot(app)
}

async fn advance(app: &mut Application) {
    app.pump(Duration::from_millis(100)).await;
    app.tick();
}

async fn wait_ready(app: &mut Application, seconds: u64, history_required: bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while app.state.connection.phase != ConnectionPhase::Ready
        || (history_required && !app.state.session.loaded)
    {
        if app.state.connection.phase == ConnectionPhase::Incompatible {
            let notice = connection_notice(app);
            bail!("{notice}");
        }
        check_deadline(app, deadline)?;
        advance(app).await;
    }
    Ok(())
}

fn connection_notice(app: &Application) -> &str {
    app.state
        .ui
        .notice
        .as_deref()
        .unwrap_or("Host incompatible")
}

async fn send(app: &mut Application, text: String, seconds: u64) -> Result<()> {
    app.action(UiAction::Edit(text));
    let id = dispatch(app, UiAction::Send)?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let pending = app
            .state
            .commands
            .get(&id)
            .context("Submission record missing")?;
        if let SubmissionPhase::Rejected(reason) = &pending.phase {
            bail!("{id} rejected: {reason}");
        }
        if settled(app, &id)? {
            return Ok(());
        }
        check_deadline(app, deadline)?;
        advance(app).await;
    }
}

fn dispatch(app: &mut Application, action: UiAction) -> Result<String> {
    let before = app.state.next_command;
    app.action(action);
    if app.state.next_command == before {
        let notice = app
            .state
            .ui
            .notice
            .as_deref()
            .unwrap_or("Command unavailable");
        bail!("{notice}");
    }
    app.state
        .command_order
        .last()
        .cloned()
        .context("Command record missing")
}

fn settled(app: &Application, id: &str) -> Result<bool> {
    let pending = app
        .state
        .commands
        .get(id)
        .context("Command record missing")?;
    if pending.phase != SubmissionPhase::Applied {
        return Ok(false);
    }
    let input_id = match &pending.command.body {
        CommandBody::Submit { input_id, .. } | CommandBody::Steer { input_id, .. } => input_id,
        _ => return Ok(false),
    };
    let input = app
        .state
        .session
        .transcript
        .iter()
        .find(|message| &message.id == input_id && message.status == "delivered");
    let Some(input) = input else {
        return Ok(false);
    };
    let turn = app
        .state
        .session
        .turns
        .iter()
        .find(|turn| turn.id == input.turn_id);
    let Some(turn) = turn else {
        return Ok(false);
    };
    match turn.status {
        TurnStatus::Active => Ok(false),
        TurnStatus::Completed | TurnStatus::Stopped => Ok(true),
        TurnStatus::Failed | TurnStatus::Superseded => {
            bail!("Turn ended with {:?}: {}", turn.status, turn.reason)
        }
    }
}

async fn control(app: &mut Application, action: UiAction, seconds: u64) -> Result<()> {
    let before = app.state.session.revision;
    let id = dispatch(app, action)?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let pending = app
            .state
            .commands
            .get(&id)
            .context("Command record missing")?;
        if let SubmissionPhase::Rejected(reason) = &pending.phase {
            bail!("{id} rejected: {reason}");
        }
        let refreshed = app.state.session.revision > before;
        let settled = app.state.session.active_turn().is_none() && !has_pending_input(app);
        if pending.phase == SubmissionPhase::Applied && refreshed && settled {
            return Ok(());
        }
        check_deadline(app, deadline)?;
        advance(app).await;
    }
}

fn has_pending_input(app: &Application) -> bool {
    app.state
        .session
        .transcript
        .iter()
        .any(|message| message.status == "submitted")
}

fn check_deadline(app: &Application, deadline: Instant) -> Result<()> {
    if Instant::now() < deadline {
        return Ok(());
    }
    let unresolved: Vec<_> = app
        .state
        .commands
        .values()
        .filter(|command| !command.phase.terminal())
        .map(|command| command.command.id.as_str())
        .collect();
    bail!(
        "wait timed out; Host keeps Turn state; unresolved command IDs: {}",
        unresolved.join(", ")
    )
}

fn print_snapshot(app: &Application) -> Result<()> {
    let view = &app.state.session;
    let snapshot = Snapshot {
        version: VERSION,
        session: app
            .state
            .connection
            .config
            .session
            .clone()
            .unwrap_or_default(),
        next_commit: view.next_commit,
        transcript: view.transcript.clone(),
        turns: view.turns.clone(),
        queued: view.queued.clone(),
        unassigned: view.unassigned.clone(),
        active_effects: view
            .active_effects
            .values()
            .map(|effect| effect.descriptor.clone())
            .collect(),
        approvals: view.approvals.clone(),
    };
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}
