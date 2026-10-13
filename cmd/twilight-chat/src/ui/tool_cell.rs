// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Adapted from openai/codex rust-v0.20.0, 45aed5edd139c38132de044596fdd55e47236657,
// codex-rs/tui/src/history_cell.rs (new_completed_exec_command).
// MODIFIED: typed presentation inputs only; preserve BOTH stdout/stderr and all output;
// no execution inference, ANSI passthrough, I/O, or silent line truncation.
// Upstream license/notice: ../../licenses/codex/.

use chat_protocol::{Message, ToolCall};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};
use serde_json::{Map, Value};

use super::{safe_text, text_lines, wrap_lines};

pub(super) fn invocation(call: &ToolCall, width: u16) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled("Tool call · ", Style::default().fg(Color::Magenta)),
        Span::raw(safe_text(&call.name)),
    ])];
    if !append_shell_command(call, width, &mut lines) {
        lines.extend(output_lines(&pretty_json(&call.input), width));
    }
    wrap_lines(lines, width)
}

fn append_shell_command(call: &ToolCall, width: u16, lines: &mut Vec<Line<'static>>) -> bool {
    let Some(command) = shell_command(call) else {
        return false;
    };
    lines.extend(output_lines(&format!("$ {command}"), width));
    let extras = without_keys(call.input.as_object().unwrap(), &["command"]);
    if !extras.is_empty() {
        lines.extend(output_lines(&pretty_json(&Value::Object(extras)), width));
    }
    true
}

fn shell_command(call: &ToolCall) -> Option<&str> {
    if call.name != "shell" {
        return None;
    }
    call.input.get("command")?.as_str()
}

pub(super) fn result(message: &Message, call: Option<&ToolCall>, width: u16) -> Vec<Line<'static>> {
    let shell = shell_result(message, call);
    let mut lines = vec![result_title(message, call, shell.as_ref())];
    if !message.superseded_by.is_empty() {
        lines.extend(text_lines(
            &format!("Superseded by {}", message.superseded_by),
            Style::default().fg(Color::DarkGray),
        ));
    }
    match shell {
        Some(value) => append_shell_output(&value, width, &mut lines),
        None => lines.extend(output_lines(&message.text, width)),
    }
    wrap_lines(lines, width)
}

fn result_title(
    message: &Message,
    call: Option<&ToolCall>,
    shell: Option<&Value>,
) -> Line<'static> {
    let status = if message.status.is_empty() {
        "unspecified"
    } else {
        &message.status
    };
    let mut spans = vec![
        Span::styled("Tool result · ", Style::default().fg(Color::Magenta)),
        Span::styled(safe_text(status), status_style(status)),
    ];
    if let Some(exit) = shell
        .and_then(|value| value.get("exitCode"))
        .and_then(Value::as_i64)
    {
        let color = if exit == 0 { Color::Green } else { Color::Red };
        spans.push(Span::styled(
            format!(" · exit {exit}"),
            Style::default().fg(color),
        ));
    }
    if let Some(call) = call {
        spans.push(Span::raw(format!(" · {}", safe_text(&call.name))));
    }
    Line::from(spans)
}

fn status_style(status: &str) -> Style {
    let color = match status {
        "success" => Color::Green,
        "error" => Color::Red,
        _ => Color::Yellow,
    };
    Style::default().fg(color)
}

fn shell_result(message: &Message, call: Option<&ToolCall>) -> Option<Value> {
    if call?.name != "shell" {
        return None;
    }
    let value: Value = serde_json::from_str(&message.text).ok()?;
    value.get("exitCode")?.as_i64()?;
    value.get("stdout")?.as_str()?;
    value.get("stderr")?.as_str()?;
    Some(value)
}

fn append_shell_output(value: &Value, width: u16, lines: &mut Vec<Line<'static>>) {
    for channel in ["stdout", "stderr"] {
        let text = value[channel].as_str().unwrap_or_default();
        if text.is_empty() {
            continue;
        }
        lines.push(Line::styled(channel, Style::default().fg(Color::DarkGray)));
        lines.extend(output_lines(text, width));
    }
    let extras = without_keys(
        value.as_object().unwrap(),
        &["stdout", "stderr", "exitCode"],
    );
    if !extras.is_empty() {
        lines.extend(output_lines(&pretty_json(&Value::Object(extras)), width));
    }
}

fn without_keys(object: &Map<String, Value>, keys: &[&str]) -> Map<String, Value> {
    object
        .iter()
        .filter(|(key, _)| !keys.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn output_lines(text: &str, width: u16) -> Vec<Line<'static>> {
    let prefix_width = if width > 4 { 4 } else { 0 };
    let body_width = width.saturating_sub(prefix_width);
    let body = if super::diff::is_diff(text) {
        super::diff::lines(text, body_width)
    } else {
        wrap_lines(text_lines(text, Style::default()), body_width)
    };
    body.into_iter()
        .enumerate()
        .map(|(index, line)| output_line(line, index, prefix_width))
        .collect()
}

// Codex's output first-row connector and dim continuation, with no output dropped.
fn output_line(mut line: Line<'static>, index: usize, prefix_width: u16) -> Line<'static> {
    let prefix = if index == 0 { "  ⎿ " } else { "    " };
    if prefix_width != 0 {
        line.spans.insert(0, Span::raw(prefix));
    }
    // Preserve colored Diff spans; only otherwise-unstyled output is muted.
    line.spans.iter_mut().for_each(mute_plain_span);
    line
}

fn mute_plain_span(span: &mut Span<'_>) {
    if span.style.fg.is_none() {
        span.style = span.style.add_modifier(Modifier::DIM);
    }
}
