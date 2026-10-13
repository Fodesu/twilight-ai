use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::Paragraph,
};

use super::{presentation, theme, Frame};
use crate::app::state::AppState;

pub(super) fn height(_state: &AppState) -> u16 {
    // Pending inputs are rendered once in the transcript presentation.
    0
}

pub(super) fn render(frame: &mut Frame<'_>, state: &AppState, area: Rect) {
    if area.height == 0 {
        return;
    }
    frame.render_widget(
        Paragraph::new(queue_line(state)).style(Style::default().bg(theme::USER)),
        area,
    );
}

fn queue_line(state: &AppState) -> Line<'static> {
    let pending = presentation::build(state).pending;
    let text = pending
        .iter()
        .take(3)
        .map(|message| format!("{:?}", super::bounded_text(&message.text, 32)))
        .collect::<Vec<_>>()
        .join(" · ");
    let mut spans = vec![Span::styled("  ↳ ", queue_style(theme::YELLOW))];
    spans.push(Span::styled(
        format!("{} queued", pending.len()),
        queue_style(theme::MUTED),
    ));
    if !text.is_empty() {
        spans.push(Span::styled("  ", queue_style(theme::MUTED)));
        spans.push(Span::styled(text, queue_style(theme::TEXT)));
    }
    Line::from(spans)
}

fn queue_style(fg: ratatui::style::Color) -> Style {
    Style::default().fg(fg).bg(theme::USER)
}
