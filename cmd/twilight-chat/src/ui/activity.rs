use chat_protocol::EffectKind;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::Line,
    widgets::Paragraph,
};

use super::{bounded_text, text_lines, theme, wrap_lines, Frame};
use crate::app::state::{AppState, LiveEffect};

pub(super) fn height(state: &AppState) -> u16 {
    state
        .session
        .active_effects
        .values()
        .take(3)
        .map(effect_height)
        .sum()
}

fn effect_height(effect: &LiveEffect) -> u16 {
    match effect.descriptor.kind {
        EffectKind::Model => 1,
        EffectKind::Tool => 2,
    }
}

pub(super) fn render(frame: &mut Frame<'_>, state: &AppState, area: Rect) {
    if area.height == 0 {
        return;
    }
    let surface = Style::default().bg(theme::BASE);
    frame.render_widget(Paragraph::new("").style(surface), area);
    let inner = Rect::new(
        area.x + 2,
        area.y,
        area.width.saturating_sub(4),
        area.height,
    );
    let mut effects = state.session.active_effects.values().collect::<Vec<_>>();
    effects.sort_by_key(|effect| {
        (
            &effect.descriptor.key.run_id,
            &effect.descriptor.key.effect_id,
        )
    });
    let lines = effects
        .into_iter()
        .flat_map(|effect| effect_lines(effect, inner.width))
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines).style(surface), inner);
}

fn effect_lines(effect: &LiveEffect, width: u16) -> Vec<Line<'static>> {
    let kind = match effect.descriptor.kind {
        EffectKind::Model => "Model",
        EffectKind::Tool => "Tool",
    };
    let label = bounded_text(&effect.descriptor.label, 160);
    let style = Style::default().fg(Color::Yellow);
    let labels = wrap_lines(text_lines(&format!("{kind}: {label}"), style), width);
    if effect.descriptor.kind == EffectKind::Model {
        return labels.into_iter().take(1).collect();
    }
    let tail = effect.text.chars().rev().take(2000).collect::<Vec<_>>();
    let text = tail.into_iter().rev().collect::<String>();
    let progress = wrap_lines(text_lines(&text, style), width);
    let mut lines = labels.into_iter().take(1).collect::<Vec<_>>();
    lines.extend(super::tail(&progress, 1));
    lines
}
