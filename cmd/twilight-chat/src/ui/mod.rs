//! Presentation-only components for the unidirectional application loop.
mod composer;
mod diff;
pub mod host_form;
mod markdown;
pub(crate) mod presentation;
mod queue;
mod status;
pub(crate) mod textarea;
mod tool_cell;
pub(crate) mod transcript;

use std::collections::HashSet;

use crossterm::event::{KeyEvent, KeyModifiers};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders},
};

use crate::app::state::AppState;
use crate::custom_terminal::Frame;
use host_form::HostForm;

pub(super) mod theme {
    use ratatui::style::Color;

    // Tool cells, status and unhighlighted surfaces inherit the terminal theme.
    // Do not paint Catppuccin's dark base over Kitty's configured background.
    pub const BASE: Color = Color::Reset;
    pub const USER: Color = Color::Rgb(49, 50, 68);
    pub const TEXT: Color = Color::Rgb(205, 214, 244);
    pub const MUTED: Color = Color::Rgb(166, 173, 200);
    pub const BLUE: Color = Color::Rgb(137, 180, 250);
    pub const GREEN: Color = Color::Rgb(166, 227, 161);
    pub const YELLOW: Color = Color::Rgb(249, 226, 175);
    pub const RED: Color = Color::Rgb(243, 139, 168);
}

#[derive(Default)]
pub struct RenderCaches;

pub fn render(
    frame: &mut Frame<'_>,
    state: &AppState,
    form: Option<&HostForm>,
    caches: &mut RenderCaches,
) {
    let mut editor = textarea::TextArea::default();
    editor.reset(&state.ui.draft);
    render_with_composer(frame, state, form, caches, &mut editor);
}

pub(crate) fn render_with_composer(
    frame: &mut Frame<'_>,
    state: &AppState,
    form: Option<&HostForm>,
    caches: &mut RenderCaches,
    editor: &mut textarea::TextArea,
) {
    render_with_composer_hidden(frame, state, form, caches, editor, &HashSet::new());
}

pub(crate) fn render_with_composer_hidden(
    frame: &mut Frame<'_>,
    state: &AppState,
    form: Option<&HostForm>,
    _caches: &mut RenderCaches,
    editor: &mut textarea::TextArea,
    precommitted_inputs: &HashSet<String>,
) {
    let areas = sections(frame.area(), state, editor);
    if state.ui.host_editor {
        if let Some(form) = form {
            form.render(frame, areas[0]);
        }
        status::render(frame, state, areas[1]);
        return;
    }
    transcript::render_live_with_history(frame, state, areas[0], precommitted_inputs);
    composer::render(frame, state, editor, areas[1]);
}

pub(crate) fn desired_height(
    width: u16,
    max_height: u16,
    state: &AppState,
    editor: &mut textarea::TextArea,
) -> u16 {
    desired_height_with_history(width, max_height, state, editor, &HashSet::new())
}

pub(crate) fn desired_height_with_history(
    width: u16,
    max_height: u16,
    state: &AppState,
    editor: &mut textarea::TextArea,
    precommitted_inputs: &HashSet<String>,
) -> u16 {
    if state.ui.host_editor {
        return max_height;
    }
    let area = Rect::new(0, 0, width, max_height);
    let composer = composer::pane_height(state, editor, area);
    let live = transcript::live_height_with_history(
        state,
        transcript::transcript_width(width),
        precommitted_inputs,
    );
    composer
        .saturating_add(live)
        .min(max_height)
        .max(composer)
        .max(1)
}

fn sections(area: Rect, state: &AppState, editor: &mut textarea::TextArea) -> Vec<Rect> {
    if state.ui.host_editor {
        let constraints = [
            Constraint::Min(0),
            Constraint::Length(status::height(state, area.width)),
        ];
        return Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area)
            .to_vec();
    }
    let composer = composer::pane_height(state, editor, area);
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(composer)])
        .split(area)
        .to_vec()
}

pub(super) fn bordered(title: &'static str) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

pub(crate) fn printable_key(key: &KeyEvent, c: char) -> bool {
    !key.modifiers
        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        && !c.is_control()
}

/// Normalize platform paste line endings and remove terminal control codes.
pub(crate) fn paste_text(text: &str) -> String {
    text.replace("\r\n", "\n")
        .replace('\r', "\n")
        .chars()
        .filter_map(paste_char)
        .collect()
}

fn paste_char(c: char) -> Option<char> {
    match c {
        '\n' => Some('\n'),
        '\t' => Some(' '),
        c if c.is_control() => None,
        c => Some(c),
    }
}

pub(super) fn safe_text(text: &str) -> String {
    text.chars().map(safe_char).collect()
}

fn safe_char(c: char) -> char {
    match c {
        '\n' => '\n',
        '\t' => ' ',
        c if c.is_control() => '\u{fffd}',
        c => c,
    }
}

pub(super) fn bounded_text(text: &str, limit: usize) -> String {
    safe_text(&text.chars().take(limit).collect::<String>())
}

pub(super) fn text_lines(text: &str, style: Style) -> Vec<Line<'static>> {
    safe_text(text)
        .split('\n')
        .map(|line| Line::styled(line.to_owned(), style))
        .collect()
}

/// Wrap in terminal columns using ratatui's graphemes, not UTF-8 byte offsets.
pub(super) fn wrap_lines(lines: Vec<Line<'static>>, width: u16) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let mut wrapped = Vec::new();
    for line in lines {
        wrap_line(line, usize::from(width), &mut wrapped);
    }
    wrapped
}

fn wrap_line(line: Line<'static>, width: usize, wrapped: &mut Vec<Line<'static>>) {
    let mut spans = Vec::new();
    let mut columns = 0;
    for grapheme in line.styled_graphemes(Style::default()) {
        let (symbol, size) = fitting_symbol(grapheme.symbol, width);
        if columns + size > width {
            wrapped.push(Line::from(std::mem::take(&mut spans)));
            columns = 0;
        }
        spans.push(Span::styled(symbol.to_owned(), grapheme.style));
        columns += size;
    }
    wrapped.push(Line::from(spans));
}

fn fitting_symbol(symbol: &str, width: usize) -> (&str, usize) {
    let size = Line::from(symbol).width();
    if size > width {
        ("�", 1)
    } else {
        (symbol, size)
    }
}
