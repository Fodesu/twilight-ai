use ratatui::{
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
};

use super::{queue, status, textarea::TextArea, theme, Frame};
use crate::app::state::{AppState, ConnectionPhase};

/// Reserve a cursor row and grow to at most one third of the terminal.
pub(super) fn height(editor: &mut TextArea, area: Rect) -> u16 {
    if area.height < 3 {
        return area.height;
    }
    let max_height = (area.height / 3).clamp(3, 10);
    let rows = editor.line_count(text_area(area).width).min(8) as u16;
    (rows + 2).min(max_height)
}

fn has_rules(area: Rect) -> bool {
    area.height >= 3 && area.width >= 6
}

fn inner_area(area: Rect) -> Rect {
    if has_rules(area) {
        Rect::new(
            area.x + 1,
            area.y + 1,
            area.width.saturating_sub(2),
            area.height.saturating_sub(2),
        )
    } else {
        area
    }
}

fn text_area(area: Rect) -> Rect {
    let inner = inner_area(area);
    let padding = if inner.width >= 4 { 2 } else { 0 };
    Rect::new(
        inner.x + padding,
        inner.y,
        inner.width.saturating_sub(padding),
        inner.height,
    )
}

fn editor_area(area: Rect, editor: &mut TextArea) -> Rect {
    let base = text_area(area);
    let rows = editor
        .line_count(base.width)
        .min(usize::from(base.height))
        .max(1) as u16;
    Rect::new(base.x, base.bottom().saturating_sub(rows), base.width, rows)
}

pub(super) fn pane_height(state: &AppState, editor: &mut TextArea, area: Rect) -> u16 {
    let queue = queue::height(state).min(area.height);
    let status = status::height(state, area.width).min(area.height.saturating_sub(queue));
    let input_area = Rect::new(
        area.x,
        area.y,
        area.width,
        area.height.saturating_sub(queue + status),
    );
    (queue + height(editor, input_area) + status).min(area.height)
}

pub(super) fn render(frame: &mut Frame<'_>, state: &AppState, editor: &mut TextArea, area: Rect) {
    let queue_height = queue::height(state).min(area.height);
    let status_height =
        status::height(state, area.width).min(area.height.saturating_sub(queue_height));
    let input_area = Rect::new(
        area.x,
        area.y + queue_height,
        area.width,
        area.height.saturating_sub(queue_height + status_height),
    );
    if queue_height > 0 {
        queue::render(
            frame,
            state,
            Rect::new(area.x, area.y, area.width, queue_height),
        );
    }
    if input_area.height > 0 {
        let text = editor_area(input_area, editor);
        let (lines, cursor) = editor.view(text);
        render_surface(frame, state, editor, input_area, text.height);
        let prompt_y = cursor.map_or(text.y, |(_, y)| y);
        render_prompt(frame, text, prompt_y);
        let surface = surface_style(state);
        if editor.text().is_empty() {
            let placeholder = placeholder(state);
            let style = surface.fg(theme::MUTED);
            frame.render_widget(Paragraph::new(placeholder).style(style), text);
        } else {
            frame.render_widget(Paragraph::new(lines).style(surface), text);
        }
        if let Some(cursor) = cursor {
            frame.set_cursor_position(cursor);
        }
    }
    status::render(
        frame,
        state,
        Rect::new(
            area.x,
            area.bottom().saturating_sub(status_height),
            area.width,
            status_height,
        ),
    );
}

fn render_surface(
    frame: &mut Frame<'_>,
    state: &AppState,
    editor: &TextArea,
    area: Rect,
    rows: u16,
) {
    let surface = surface_style(state);
    frame.render_widget(Paragraph::new("").style(surface), area);
    if !has_rules(area) {
        return;
    }
    let (start, total) = editor.row_extent();
    if total <= usize::from(rows) {
        return;
    }
    let end = (start + usize::from(rows)).min(total);
    let range = format!(" {}–{end}/{total} ", start + 1);
    let hint = Rect::new(area.x, area.bottom().saturating_sub(1), area.width, 1);
    let style = Style::default()
        .fg(theme::MUTED)
        .bg(surface.bg.unwrap_or(theme::BASE));
    let line = Line::from(Span::styled(range, style)).alignment(Alignment::Right);
    frame.render_widget(Paragraph::new(line), hint);
}

fn surface_style(state: &AppState) -> Style {
    let background = if state.connection.phase == ConnectionPhase::Ready {
        theme::USER
    } else {
        theme::BASE
    };
    Style::default().bg(background)
}

fn render_prompt(frame: &mut Frame<'_>, area: Rect, y: u16) {
    let inner = inner_area(area);
    if inner.width < 4 || inner.height == 0 {
        return;
    }
    let prompt = Paragraph::new("›").style(Style::default().fg(theme::BLUE));
    let x = inner.x.saturating_sub(2);
    frame.render_widget(prompt, Rect::new(x, y, 1, 1));
}

fn placeholder(state: &AppState) -> &'static str {
    if state.connection.phase == ConnectionPhase::Ready {
        "Type a message…"
    } else {
        "Draft…"
    }
}
