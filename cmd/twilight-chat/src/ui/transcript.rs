use std::collections::{HashMap, HashSet};

use chat_protocol::{EffectKind, Message, MessageKind, ToolCall};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
};

use super::{presentation, text_lines, theme, wrap_lines, Frame};
use crate::app::state::AppState;

// Keep one terminal column empty at the right edge.  The width used by ratatui's
// Unicode measurement is not guaranteed to match the terminal's width for every
// grapheme (notably ambiguous-width characters and emoji).  Without this guard a
// live line can be physically wrapped by the terminal before the same line is
// moved to native scrollback, which makes the live and committed turns jump by a
// column/row at completion.
const TRANSCRIPT_OUTER_INSET: u16 = 2;
const TRANSCRIPT_RIGHT_GUARD: u16 = 1;

pub(crate) fn transcript_width(area_width: u16) -> u16 {
    area_width
        .saturating_sub(TRANSCRIPT_OUTER_INSET + TRANSCRIPT_OUTER_INSET + TRANSCRIPT_RIGHT_GUARD)
}

struct TranscriptBlock {
    lines: Vec<Line<'static>>,
    background: Color,
}

pub(crate) fn history_turn_lines(
    state: &AppState,
    width: u16,
    keys: &[String],
) -> Vec<Line<'static>> {
    history_turn_lines_excluding(state, width, keys, &HashSet::new())
}

pub(crate) fn history_turn_lines_excluding(
    state: &AppState,
    width: u16,
    keys: &[String],
    excluded_inputs: &HashSet<String>,
) -> Vec<Line<'static>> {
    let wanted = keys.iter().map(String::as_str).collect::<HashSet<_>>();
    let presentation = presentation::build(state);
    let calls = call_catalog(&state.session.transcript);
    presentation
        .committed
        .iter()
        .filter(|turn| wanted.contains(presentation::turn_key(turn).as_str()))
        .flat_map(|turn| history_messages_for_turn(turn, width, &calls, excluded_inputs))
        .collect()
}

pub(crate) fn live_height_with_history(
    state: &AppState,
    width: u16,
    precommitted_inputs: &HashSet<String>,
) -> u16 {
    live_blocks_with_history(state, width, precommitted_inputs)
        .iter()
        .map(|block| block.lines.len().saturating_add(2))
        .sum::<usize>()
        .min(usize::from(u16::MAX)) as u16
}

pub(super) fn render_live_with_history(
    frame: &mut Frame<'_>,
    state: &AppState,
    area: Rect,
    precommitted_inputs: &HashSet<String>,
) {
    if area.height == 0 {
        return;
    }
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(theme::BASE)),
        area,
    );
    let width = transcript_width(area.width);
    let blocks = live_blocks_with_history(state, width, precommitted_inputs);
    let total = blocks
        .iter()
        .map(|block| block.lines.len().saturating_add(2))
        .sum::<usize>();
    let mut cursor = usize::from(area.height).saturating_sub(total);
    for block in blocks {
        let height = block.lines.len() + 2;
        render_block(
            frame,
            &block,
            area,
            cursor,
            cursor + height,
            0,
            usize::from(area.height),
        );
        cursor += height;
    }
}

fn history_messages_for_turn(
    turn: &presentation::CommittedTurn,
    width: u16,
    calls: &CallCatalog<'_>,
    excluded_inputs: &HashSet<String>,
) -> Vec<Line<'static>> {
    turn.messages
        .iter()
        .filter(|message| !excluded_inputs.contains(&message.id))
        .flat_map(|message| message_history_lines(message, width, calls))
        .collect()
}

fn message_history_lines(
    message: &Message,
    width: u16,
    calls: &CallCatalog<'_>,
) -> Vec<Line<'static>> {
    let content_width = transcript_width(width);
    message_blocks(message, content_width, calls)
        .into_iter()
        .flat_map(|block| history_block_lines(block, width))
        .collect()
}

fn history_block_lines(block: TranscriptBlock, width: u16) -> Vec<Line<'static>> {
    let line_width = width.saturating_sub(TRANSCRIPT_RIGHT_GUARD);
    let mut lines = vec![background_line(line_width, block.background)];
    lines.extend(
        block
            .lines
            .into_iter()
            .map(|line| inset_line(line, block.background)),
    );
    lines.push(background_line(line_width, block.background));
    lines
}

fn live_turn_blocks(
    turn: &presentation::LiveTurn,
    width: u16,
    calls: &CallCatalog<'_>,
    precommitted_inputs: &HashSet<String>,
) -> Vec<TranscriptBlock> {
    turn.messages
        .iter()
        .filter(|message| !precommitted_inputs.contains(&message.id))
        .flat_map(|message| message_blocks(message, width, calls))
        .collect()
}

fn live_effect_blocks(turn: &presentation::LiveTurn, width: u16) -> Vec<TranscriptBlock> {
    turn.effects
        .iter()
        .filter(|effect| effect.kind == EffectKind::Tool || !effect.text.is_empty())
        .map(|effect| live_block(effect, width))
        .collect()
}

fn live_blocks_with_history(
    state: &AppState,
    width: u16,
    precommitted_inputs: &HashSet<String>,
) -> Vec<TranscriptBlock> {
    let presentation = presentation::build(state);
    let calls = call_catalog(&state.session.transcript);
    let mut blocks = presentation
        .active()
        .into_iter()
        .flat_map(|turn| live_turn_blocks(turn, width, &calls, precommitted_inputs))
        .collect::<Vec<_>>();
    blocks.extend(
        presentation
            .pending
            .iter()
            .flat_map(|message| message_blocks(message, width, &calls)),
    );
    if let Some(turn) = presentation.active() {
        blocks.extend(live_effect_blocks(turn, width));
    }
    blocks
}

fn live_block(effect: &presentation::LiveEffect, width: u16) -> TranscriptBlock {
    if effect.kind == EffectKind::Model {
        let message = Message {
            kind: MessageKind::Assistant,
            text: effect.text.clone(),
            ..Message::default()
        };
        return TranscriptBlock {
            lines: styled_message(&message, width),
            background: Color::Reset,
        };
    }
    let label = super::bounded_text(&effect.label, 160);
    let mut lines = wrap_lines(
        super::text_lines(
            &format!("Tool: {label}"),
            Style::default().fg(Color::Yellow),
        ),
        width,
    );
    let output = wrap_lines(
        super::text_lines(&effect.text, Style::default().fg(Color::Yellow)),
        width,
    );
    if let Some(line) = output.into_iter().next_back() {
        lines.push(line);
    }
    TranscriptBlock {
        lines,
        background: theme::BASE,
    }
}

fn render_block(
    frame: &mut Frame<'_>,
    block: &TranscriptBlock,
    area: Rect,
    start: usize,
    end: usize,
    offset: usize,
    viewport: usize,
) {
    let visible_start = start.max(offset);
    let visible_end = end.min(offset + viewport);
    if visible_start >= visible_end {
        return;
    }
    let y = area.y + (visible_start - offset) as u16;
    let height = (visible_end - visible_start) as u16;
    let background = Style::default().bg(block.background);
    frame.render_widget(
        Paragraph::new("").style(background),
        Rect::new(area.x, y, area.width, height),
    );
    let content_start = start + 1;
    let content_end = content_start + block.lines.len();
    let first = visible_start.max(content_start);
    let last = visible_end.min(content_end);
    if first >= last {
        return;
    }
    let lines = block.lines[first - content_start..last - content_start].to_vec();
    let text_y = area.y + (first - offset) as u16;
    let text_area = Rect::new(
        area.x + TRANSCRIPT_OUTER_INSET,
        text_y,
        transcript_width(area.width),
        (last - first) as u16,
    );
    frame.render_widget(Paragraph::new(lines).style(background), text_area);
}

fn message_blocks(message: &Message, width: u16, calls: &CallCatalog<'_>) -> Vec<TranscriptBlock> {
    if message.kind == MessageKind::ToolResult {
        let call = calls
            .get(&(message.run_id.as_str(), message.call_id.as_str()))
            .copied()
            .flatten();
        return vec![result_block(message, call, width)];
    }
    let mut blocks = vec![TranscriptBlock {
        lines: styled_message(message, width),
        background: message_background(message.kind),
    }];
    for call in &message.calls {
        blocks.push(invocation_block(call, width));
    }
    blocks
}

fn result_block(message: &Message, call: Option<&ToolCall>, width: u16) -> TranscriptBlock {
    TranscriptBlock {
        lines: color_block(
            super::tool_cell::result(message, call, width.saturating_sub(2)),
            theme::BASE,
        ),
        background: theme::BASE,
    }
}

fn invocation_block(call: &ToolCall, width: u16) -> TranscriptBlock {
    TranscriptBlock {
        lines: color_block(
            super::tool_cell::invocation(call, width.saturating_sub(2)),
            theme::BASE,
        ),
        background: theme::BASE,
    }
}

fn message_background(kind: MessageKind) -> Color {
    match kind {
        MessageKind::Input => theme::USER,
        MessageKind::Summary | MessageKind::Assistant => Color::Reset,
        MessageKind::ToolResult => theme::BASE,
    }
}

type CallCatalog<'a> = HashMap<(&'a str, &'a str), Option<&'a ToolCall>>;

fn call_catalog(transcript: &[Message]) -> CallCatalog<'_> {
    let mut calls = HashMap::new();
    for message in transcript {
        for call in &message.calls {
            remember_call(message, call, &mut calls);
        }
    }
    calls
}

fn remember_call<'a>(message: &'a Message, call: &'a ToolCall, calls: &mut CallCatalog<'a>) {
    if message.run_id.is_empty() || call.call_id.is_empty() {
        return;
    }
    // Provider IDs can repeat across Runs. Ambiguous IDs must not mislabel a result.
    calls
        .entry((&message.run_id, &call.call_id))
        .and_modify(|value| *value = None)
        .or_insert(Some(call));
}

fn message_lines(message: &Message, width: u16) -> Vec<Line<'static>> {
    match message.kind {
        MessageKind::Assistant | MessageKind::Summary => {
            super::markdown::lines(&message.text, width)
        }
        MessageKind::Input | MessageKind::ToolResult => text_lines(&message.text, Style::default()),
    }
}

fn styled_message(message: &Message, width: u16) -> Vec<Line<'static>> {
    let indent = if width >= 2 { 2 } else { 0 };
    let body = wrap_lines(
        message_lines(message, width.saturating_sub(indent)),
        width.saturating_sub(indent),
    );
    body.into_iter()
        .enumerate()
        .map(|(index, line)| message_row(message.kind, &message.status, line, index, width, indent))
        .collect()
}

fn background_line(width: u16, background: Color) -> Line<'static> {
    Line::styled(
        " ".repeat(usize::from(width)),
        Style::default().bg(background),
    )
}

fn inset_line(mut line: Line<'static>, background: Color) -> Line<'static> {
    let padding = Style::default().bg(background);
    let inset = " ".repeat(usize::from(TRANSCRIPT_OUTER_INSET));
    line.spans.insert(0, Span::styled(inset.clone(), padding));
    line.spans.push(Span::styled(inset, padding));
    line
}

fn color_block(lines: Vec<Line<'static>>, background: Color) -> Vec<Line<'static>> {
    lines
        .into_iter()
        .map(|mut line| {
            line.style = line.style.bg(background);
            line
        })
        .collect()
}

fn message_row(
    kind: MessageKind,
    status: &str,
    mut line: Line<'static>,
    index: usize,
    width: u16,
    indent: u16,
) -> Line<'static> {
    let user = kind == MessageKind::Input;
    let style = if user {
        Style::default()
            .fg(theme::MUTED)
            .bg(message_background(kind))
    } else if kind == MessageKind::Summary {
        Style::default()
            .fg(theme::MUTED)
            .bg(message_background(kind))
            .add_modifier(Modifier::DIM)
    } else {
        Style::default()
            .fg(theme::TEXT)
            .bg(message_background(kind))
    };
    let (marker, color) = if user {
        input_marker(status)
    } else {
        ("• ", theme::GREEN)
    };
    let marker = if index == 0 { marker } else { "  " };
    let prefix = if indent == 0 { "" } else { marker };
    let prefix = Span::styled(prefix, style.fg(color).add_modifier(Modifier::BOLD));
    line.spans.insert(0, prefix);
    let padding = usize::from(width).saturating_sub(line.width());
    line.spans.push(Span::styled(" ".repeat(padding), style));
    line.style = style;
    for span in &mut line.spans {
        span.style = span.style.bg(message_background(kind));
    }
    line
}

fn input_marker(status: &str) -> (&'static str, Color) {
    match status {
        "submitted" | "pending" => ("… ", theme::YELLOW),
        "rejected" | "withdrawn" => ("× ", theme::RED),
        _ => ("› ", theme::BLUE),
    }
}
