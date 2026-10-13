use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::Line,
    widgets::{Clear, Paragraph},
};

use super::{bounded_text, theme, Frame};
use crate::app::state::{AppState, ConnectionPhase, ObservationPhase, SubmissionPhase};

const HEIGHT: u16 = 5;

pub(super) fn height(_state: &AppState, _width: u16) -> u16 {
    HEIGHT
}

pub(super) fn render(frame: &mut Frame<'_>, state: &AppState, area: Rect) {
    let surface = Style::default().bg(theme::BASE);
    frame.render_widget(Clear, area);
    let lines = lines(state, area.width);
    frame.render_widget(Paragraph::new(lines).style(surface), area);
}

fn lines(state: &AppState, width: u16) -> Vec<Line<'static>> {
    vec![
        Line::styled(primary(state), Style::default().fg(theme::BLUE)),
        status_line(
            turn_notice(state).or_else(|| state.ui.notice.clone()),
            "Notice",
            width,
            theme::YELLOW,
        ),
        status_line(
            state.session.error.clone(),
            "Synchronization error",
            width,
            theme::RED,
        ),
        status_line(operation(state), "Operation", width, theme::YELLOW),
        Line::styled(session_header(state), Style::default().fg(theme::MUTED)),
    ]
}

fn status_line(text: Option<String>, label: &str, width: u16, color: Color) -> Line<'static> {
    let Some(text) = text else {
        return Line::raw("");
    };
    let prefix = format!("{label}: ");
    let available = usize::from(width).saturating_sub(prefix.chars().count());
    let body = compact(&bounded_text(&text, 2000), available);
    Line::styled(prefix + &body, Style::default().fg(color))
}

fn compact(text: &str, limit: usize) -> String {
    let count = text.chars().count();
    if count <= limit {
        return text.to_owned();
    }
    if limit <= 1 {
        return "…".chars().take(limit).collect();
    }
    let head = (limit - 1) / 2;
    let tail = limit - head - 1;
    let start = text.chars().take(head).collect::<String>();
    let end = text
        .chars()
        .skip(count.saturating_sub(tail))
        .collect::<String>();
    format!("{start}…{end}")
}

fn primary(state: &AppState) -> String {
    let connection = match state.connection.phase {
        ConnectionPhase::Unconfigured => "Offline (unconfigured)",
        ConnectionPhase::Connecting => "Connecting",
        ConnectionPhase::Ready => "Connected",
        ConnectionPhase::Unavailable => "Offline (unavailable)",
        ConnectionPhase::Incompatible => "Offline (incompatible protocol)",
    };
    let observation = match state.observation {
        ObservationPhase::Idle => "idle",
        ObservationPhase::Connecting => "connecting",
        ObservationPhase::Live => "live",
        ObservationPhase::Retrying if state.connection.phase == ConnectionPhase::Ready => {
            "retrying; command channel remains usable"
        }
        ObservationPhase::Retrying => "retrying",
    };
    format!("{connection} | Observation: {observation}")
}

fn operation(state: &AppState) -> Option<String> {
    let commands = state
        .command_order
        .iter()
        .rev()
        .filter_map(|id| state.commands.get(id))
        .filter(|command| command.epoch == state.connection.epoch)
        .collect::<Vec<_>>();
    let problematic = commands
        .iter()
        .find(|command| matches!(command.phase, SubmissionPhase::Unknown(_)));
    let command = problematic.copied().or_else(|| commands.first().copied())?;
    let phase = match &command.phase {
        SubmissionPhase::Queued => "Queued (not submitted)".to_owned(),
        SubmissionPhase::Submitting => "Submitting (not committed)".to_owned(),
        SubmissionPhase::Accepted => "Accepted; awaiting committed history".to_owned(),
        SubmissionPhase::Applied => "Applied".to_owned(),
        SubmissionPhase::Rejected(reason) => format!("Rejected: {reason}"),
        SubmissionPhase::Unknown(reason) => format!("Outcome unknown: {reason}"),
    };
    Some(phase)
}

fn turn_notice(state: &AppState) -> Option<String> {
    let turn = state.session.turns.last()?;
    if turn.status != chat_protocol::TurnStatus::Failed {
        return None;
    }
    Some(format!("Turn failed: {}", bounded_text(&turn.reason, 2000)))
}

fn turn_status(turn: &chat_protocol::Turn) -> &'static str {
    match turn.status {
        chat_protocol::TurnStatus::Active => "active",
        chat_protocol::TurnStatus::Completed => "completed",
        chat_protocol::TurnStatus::Stopped => "stopped",
        chat_protocol::TurnStatus::Failed => "failed",
        chat_protocol::TurnStatus::Superseded => "superseded",
    }
}

fn session_header(state: &AppState) -> String {
    let session = state
        .connection
        .config
        .session
        .as_deref()
        .unwrap_or("new session");
    let session = bounded_text(session, 24).replace('\n', " ");
    let turn = state
        .session
        .turns
        .last()
        .map(turn_status)
        .unwrap_or("idle");
    format!("Session: {session} | Turn: {turn}")
}
