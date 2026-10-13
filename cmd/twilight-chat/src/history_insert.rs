//! Native history insertion using Codex's full-screen strategy.
//! The mutable viewport is cleared before scrolling so its composer/tail never
//! leaks into native history. Final rows are written at the old viewport top;
//! the new viewport follows them, rather than being pinned to the screen bottom.
use std::{fmt, io, io::Write};

use crate::custom_terminal::Terminal;
use crossterm::{
    cursor::MoveTo,
    queue,
    style::{
        Color as CColor, Colors, Print, SetAttribute, SetBackgroundColor, SetColors,
        SetForegroundColor,
    },
    terminal::{Clear, ClearType},
    Command,
};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier},
    text::{Line, Span},
};

pub(crate) fn insert_history_lines<B: ratatui::backend::Backend + Write>(
    terminal: &mut Terminal<B>,
    lines: Vec<Line>,
) -> io::Result<()> {
    let screen = terminal.size()?;
    let previous = terminal.viewport_area;
    let cursor = terminal.last_known_cursor_pos;
    let area = write_history_lines(terminal.backend_mut(), previous, screen.height, lines)?;
    queue!(terminal.backend_mut(), MoveTo(cursor.x, cursor.y))?;
    terminal.set_viewport_area(area);
    terminal.invalidate();
    Ok(())
}

fn write_history_lines(
    writer: &mut impl Write,
    mut area: Rect,
    screen_height: u16,
    lines: Vec<Line>,
) -> io::Result<Rect> {
    if lines.is_empty() || screen_height == 0 || area.width == 0 {
        return Ok(area);
    }
    // Upstream owns wrapping. Never silently let a row occupy extra physical
    // lines while bookkeeping continues to count it as one.
    if lines
        .iter()
        .any(|line| line.width() > usize::from(area.width))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "history row exceeds viewport width",
        ));
    }
    let row_count = lines.len();
    queue!(
        writer,
        ResetScrollRegion,
        MoveTo(0, area.y),
        Clear(ClearType::FromCursorDown)
    )?;
    for (index, line) in lines.into_iter().enumerate() {
        if index > 0 {
            queue!(writer, Print("\r\n"))?;
        }
        queue!(writer, Clear(ClearType::UntilNewLine))?;
        let spans = line
            .spans
            .iter()
            .map(|span| Span {
                content: span.content.clone(),
                style: line.style.patch(span.style),
            })
            .collect::<Vec<_>>();
        write_spans(writer, spans.iter())?;
    }
    // The first newline moves off the final history row. These rows reserve
    // space for the viewport and scroll naturally only when the screen fills.
    for _ in 0..area.height {
        queue!(writer, Print("\r\n"), Clear(ClearType::UntilNewLine))?;
    }
    area.y = usize::from(area.y)
        .saturating_add(row_count)
        .min(usize::from(screen_height.saturating_sub(area.height))) as u16;
    Ok(area)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetScrollRegion;

impl Command for ResetScrollRegion {
    fn write_ansi(&self, f: &mut impl fmt::Write) -> fmt::Result {
        write!(f, "\x1b[r")
    }
    #[cfg(windows)]
    fn execute_winapi(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ANSI scroll region required",
        ))
    }
    #[cfg(windows)]
    fn is_ansi_code_supported(&self) -> bool {
        true
    }
}

struct ModifierDiff {
    from: Modifier,
    to: Modifier,
}

impl ModifierDiff {
    fn queue(self, w: &mut impl Write) -> io::Result<()> {
        use crossterm::style::Attribute as A;
        let removed = self.from - self.to;
        if removed.intersects(Modifier::BOLD | Modifier::DIM) {
            queue!(w, SetAttribute(A::NormalIntensity))?;
            if self.to.contains(Modifier::BOLD) {
                queue!(w, SetAttribute(A::Bold))?;
            }
            if self.to.contains(Modifier::DIM) {
                queue!(w, SetAttribute(A::Dim))?;
            }
        }
        for (modifier, attribute) in [
            (Modifier::REVERSED, A::NoReverse),
            (Modifier::ITALIC, A::NoItalic),
            (Modifier::UNDERLINED, A::NoUnderline),
            (Modifier::CROSSED_OUT, A::NotCrossedOut),
        ] {
            if removed.contains(modifier) {
                queue!(w, SetAttribute(attribute))?;
            }
        }
        if removed.intersects(Modifier::SLOW_BLINK | Modifier::RAPID_BLINK) {
            queue!(w, SetAttribute(A::NoBlink))?;
        }
        let added = self.to - self.from;
        for (modifier, attribute) in [
            (Modifier::REVERSED, A::Reverse),
            (Modifier::BOLD, A::Bold),
            (Modifier::DIM, A::Dim),
            (Modifier::ITALIC, A::Italic),
            (Modifier::UNDERLINED, A::Underlined),
            (Modifier::CROSSED_OUT, A::CrossedOut),
            (Modifier::SLOW_BLINK, A::SlowBlink),
            (Modifier::RAPID_BLINK, A::RapidBlink),
        ] {
            if added.contains(modifier) {
                queue!(w, SetAttribute(attribute))?;
            }
        }
        Ok(())
    }
}

fn write_spans<'a>(
    writer: &mut impl Write,
    content: impl Iterator<Item = &'a Span<'a>>,
) -> io::Result<()> {
    let mut fg = Color::Reset;
    let mut bg = Color::Reset;
    let mut last_modifier = Modifier::empty();
    for span in content {
        let modifier = span.style.add_modifier - span.style.sub_modifier;
        if modifier != last_modifier {
            ModifierDiff {
                from: last_modifier,
                to: modifier,
            }
            .queue(writer)?;
            last_modifier = modifier;
        }
        let next_fg = span.style.fg.unwrap_or(Color::Reset);
        let next_bg = span.style.bg.unwrap_or(Color::Reset);
        if next_fg != fg || next_bg != bg {
            queue!(
                writer,
                SetColors(Colors::new(next_fg.into(), next_bg.into()))
            )?;
            fg = next_fg;
            bg = next_bg;
        }
        queue!(writer, Print(&span.content))?;
    }
    queue!(
        writer,
        SetForegroundColor(CColor::Reset),
        SetBackgroundColor(CColor::Reset),
        SetAttribute(crossterm::style::Attribute::Reset)
    )
}
