//! A pure local editor; validation never opens files or contacts a Host.
use chat_client::config::{HostConfig, HostLaunch};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::Line,
    widgets::{Clear, Paragraph},
};

use super::{bordered, bounded_text, printable_key, safe_text, text_lines, wrap_lines, Frame};
use crate::app::event::UiAction;

const LABELS: [&str; 4] = [
    "URL",
    "Session (empty: new)",
    "Host binary path (optional)",
    "Host config path (optional)",
];

pub struct HostForm {
    values: [String; 4],
    selected: usize,
    error: Option<String>,
}

impl HostForm {
    pub fn new(config: &HostConfig) -> Self {
        let launch = config.launch.as_ref();
        let values = [
            config.url.clone(),
            config.session.clone().unwrap_or_default(),
            path_text(launch.map(|launch| launch.binary.as_path())),
            path_text(launch.map(|launch| launch.config.as_path())),
        ];
        Self {
            values,
            selected: 0,
            error: None,
        }
    }

    pub(crate) fn handle_key(&mut self, key: KeyEvent) -> Option<UiAction> {
        match key.code {
            KeyCode::Esc => return Some(UiAction::CloseHost),
            KeyCode::Enter => return self.submit(),
            KeyCode::Tab if key.modifiers.contains(KeyModifiers::SHIFT) => self.select(3),
            KeyCode::Tab | KeyCode::Down => self.select(1),
            KeyCode::BackTab | KeyCode::Up => self.select(3),
            KeyCode::Backspace => self.backspace(),
            KeyCode::Char(c) if printable_key(&key, c) => self.paste(&c.to_string()),
            _ => {}
        }
        None
    }

    pub(crate) fn paste(&mut self, text: &str) {
        let text = super::paste_text(text).replace('\n', " ");
        self.values[self.selected].push_str(&text);
        self.error = None;
    }

    fn backspace(&mut self) {
        self.values[self.selected].pop();
        self.error = None;
    }

    fn select(&mut self, step: usize) {
        self.selected = (self.selected + step) % 4;
    }

    fn submit(&mut self) -> Option<UiAction> {
        match self.config() {
            Ok(config) => Some(UiAction::Configure(config)),
            Err(error) => self.reject(error),
        }
    }

    fn reject(&mut self, error: anyhow::Error) -> Option<UiAction> {
        self.error = Some(safe_text(&format!("{error:#}")));
        None
    }

    fn config(&self) -> anyhow::Result<HostConfig> {
        let config = HostConfig {
            url: self.values[0].trim().to_owned(),
            session: optional_text(&self.values[1]),
            launch: optional_launch(&self.values[2], &self.values[3])?,
        };
        config.validate()?;
        Ok(config)
    }

    pub(super) fn render(&self, frame: &mut Frame<'_>, available: Rect) {
        let area = form_area(available);
        let block = bordered("Host configuration");
        let inner = block.inner(area);
        let compact = inner.height < 10;
        let lines = self.lines(inner.width, compact);
        frame.render_widget(Clear, area);
        frame.render_widget(Paragraph::new(lines).block(block), area);
        self.render_cursor(frame, inner, compact);
    }

    fn lines(&self, width: u16, compact: bool) -> Vec<Line<'static>> {
        let fields = if compact {
            vec![self.selected]
        } else {
            (0..4).collect()
        };
        let mut lines = fields
            .into_iter()
            .map(|field| self.field_line(field, width))
            .collect::<Vec<_>>();
        let error = self.error.as_deref().unwrap_or("");
        let mut errors = wrap_lines(
            text_lines(&bounded_text(error, 1000), Style::default().fg(Color::Red)),
            width,
        );
        errors.truncate(3);
        errors.resize(3, Line::raw(""));
        lines.extend(errors);
        lines.push(Line::raw("Tab/Shift-Tab or ↑/↓ fields"));
        lines.push(Line::raw("Enter connects · Esc cancels · Ctrl-C exits"));
        lines
    }

    fn field_line(&self, field: usize, width: u16) -> Line<'static> {
        let marker = if field == self.selected { ">" } else { " " };
        let prefix = format!("{marker} {}: ", LABELS[field]);
        let room = usize::from(width).saturating_sub(Line::from(prefix.as_str()).width() + 1);
        let value = input_tail(&self.values[field], room);
        let color = if field == self.selected {
            Color::Cyan
        } else {
            Color::White
        };
        Line::styled(format!("{prefix}{value}"), Style::default().fg(color))
    }

    fn render_cursor(&self, frame: &mut Frame<'_>, inner: Rect, compact: bool) {
        let row = if compact { 0 } else { self.selected as u16 };
        if inner.width == 0 || row >= inner.height {
            return;
        }
        let columns = self.field_line(self.selected, inner.width).width();
        let x = inner.x + columns.min(usize::from(inner.width - 1)) as u16;
        frame.set_cursor_position((x, inner.y + row));
    }
}

fn optional_launch(binary: &str, config: &str) -> anyhow::Result<Option<HostLaunch>> {
    match (optional_text(binary), optional_text(config)) {
        (None, None) => Ok(None),
        (Some(binary), Some(config)) => Ok(Some(HostLaunch {
            binary: binary.into(),
            config: config.into(),
        })),
        _ => anyhow::bail!("Host binary and config paths must both be filled or both empty"),
    }
}

fn optional_text(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn path_text(path: Option<&std::path::Path>) -> String {
    path.map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn input_tail(text: &str, width: usize) -> String {
    let safe = safe_text(text).replace('\n', " ");
    let mut columns = 0;
    let mut start = safe.len();
    for (index, c) in safe.char_indices().rev() {
        let size = Line::from(c.to_string()).width();
        if columns + size > width {
            break;
        }
        columns += size;
        start = index;
    }
    safe[start..].to_owned()
}

fn form_area(area: Rect) -> Rect {
    let width = area.width.min(76);
    let height = area.height.min(14);
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}
