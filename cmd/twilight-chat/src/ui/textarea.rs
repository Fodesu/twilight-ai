// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// MODIFIED: adapted from OpenAI Codex rust-v0.20.0, commit
// 45aed5edd139c38132de044596fdd55e47236657,
// codex-rs/tui/src/bottom_pane/textarea.rs. Editing, word/line movement,
// preferred-column navigation and effective_scroll are copied/adapted here.
// Extended grapheme boundaries and safe, whitespace-preserving cell wrapping
// replace upstream GraphemeCursor fallbacks and unsafe textwrap pointer ranges.
// Visual row metrics support the auto-growing Twilight composer.

use std::ops::Range;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{layout::Rect, text::Line};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Default)]
pub(crate) struct TextArea {
    text: String,
    cursor_pos: usize,
    preferred_col: Option<usize>,
    revision: u64,
    wrap_cache: Option<WrapCache>,
    width: u16,
    scroll: usize,
}

struct WrapCache {
    revision: u64,
    width: u16,
    rows: Vec<Row>,
}

impl WrapCache {
    fn new(text: &str, revision: u64, width: u16) -> Self {
        Self {
            revision,
            width,
            rows: wrap_cells(text, usize::from(width)),
        }
    }
}

#[derive(Default)]
struct Row {
    range: Range<usize>,
    cells: Vec<Cell>,
    columns: usize,
}

impl Row {
    fn at(pos: usize) -> Self {
        Self {
            range: pos..pos,
            ..Self::default()
        }
    }
}

struct Cell {
    range: Range<usize>,
    symbol: String,
    column: usize,
    width: usize,
}

impl TextArea {
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn reset(&mut self, text: &str) {
        self.text = text.to_owned();
        self.cursor_pos = self.text.len();
        self.changed();
        self.scroll = 0;
    }

    fn changed(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.wrap_cache = None;
        self.preferred_col = None;
    }

    pub(crate) fn insert_str(&mut self, text: &str) {
        self.insert_str_at(self.cursor_pos, text);
    }

    fn insert_str_at(&mut self, pos: usize, text: &str) {
        if text.is_empty() {
            return;
        }
        let pos = self.floor_boundary(pos);
        self.text.insert_str(pos, text);
        if pos <= self.cursor_pos {
            self.cursor_pos += text.len();
        }
        self.cursor_pos = self.ceil_boundary(self.cursor_pos);
        self.changed();
    }

    fn replace_range(&mut self, range: Range<usize>, text: &str) {
        assert!(range.start <= range.end);
        let start = self.floor_boundary(range.start);
        let end = self.ceil_boundary(range.end);
        let removed_len = end - start;
        let inserted_len = text.len();
        if removed_len == 0 && inserted_len == 0 {
            return;
        }
        self.text.replace_range(start..end, text);
        // Update the cursor position to account for the edit.
        self.cursor_pos = if self.cursor_pos < start {
            self.cursor_pos
        } else if self.cursor_pos <= end {
            start + inserted_len
        } else {
            self.cursor_pos - removed_len + inserted_len
        };
        // Deletion/insertion may join previously separate graphemes (e.g. ZWJ).
        self.cursor_pos = self.ceil_boundary(self.cursor_pos);
        self.changed();
    }

    fn floor_boundary(&self, pos: usize) -> usize {
        self.text
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .chain(std::iter::once(self.text.len()))
            .take_while(|&i| i <= pos)
            .last()
            .unwrap_or(0)
    }

    fn ceil_boundary(&self, pos: usize) -> usize {
        self.text
            .grapheme_indices(true)
            .map(|(i, _)| i)
            .find(|&i| i >= pos)
            .unwrap_or(self.text.len())
    }

    fn set_cursor(&mut self, pos: usize) {
        self.cursor_pos = self.floor_boundary(pos.min(self.text.len()));
        self.preferred_col = None;
    }

    fn previous_boundary(&self) -> usize {
        self.floor_boundary(self.cursor_pos.saturating_sub(1))
    }

    fn next_boundary(&self) -> usize {
        self.ceil_boundary(self.cursor_pos.saturating_add(1))
    }

    fn beginning_of_line(&self, pos: usize) -> usize {
        self.text[..pos].rfind('\n').map(|i| i + 1).unwrap_or(0)
    }

    fn beginning_of_current_line(&self) -> usize {
        self.beginning_of_line(self.cursor_pos)
    }

    fn end_of_line(&self, pos: usize) -> usize {
        self.text[pos..]
            .find('\n')
            .map(|i| i + pos)
            .unwrap_or(self.text.len())
    }

    fn end_of_current_line(&self) -> usize {
        self.end_of_line(self.cursor_pos)
    }

    fn beginning_of_previous_word(&self) -> usize {
        let Some(first_non_ws) = self.text[..self.cursor_pos].rfind(|c: char| !c.is_whitespace())
        else {
            return 0;
        };
        let pos = self.text[..first_non_ws]
            .char_indices()
            .rfind(|(_, c)| c.is_whitespace())
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        self.floor_boundary(pos)
    }

    fn end_of_next_word(&self) -> usize {
        let Some(first_non_ws) = self.text[self.cursor_pos..].find(|c: char| !c.is_whitespace())
        else {
            return self.text.len();
        };
        let word_start = self.cursor_pos + first_non_ws;
        let pos = self.text[word_start..]
            .find(|c: char| c.is_whitespace())
            .map(|i| word_start + i)
            .unwrap_or(self.text.len());
        self.ceil_boundary(pos)
    }

    pub(crate) fn input(&mut self, event: KeyEvent) {
        let mods = event.modifiers;
        let word = mods.intersects(KeyModifiers::ALT | KeyModifiers::CONTROL);
        match event.code {
            KeyCode::Left if word => self.set_cursor(self.beginning_of_previous_word()),
            KeyCode::Right if word => self.set_cursor(self.end_of_next_word()),
            KeyCode::Left => self.set_cursor(self.previous_boundary()),
            KeyCode::Right => self.set_cursor(self.next_boundary()),
            KeyCode::Up => self.move_vertical(false),
            KeyCode::Down => self.move_vertical(true),
            KeyCode::Home => self.move_cursor_to_beginning_of_line(false),
            KeyCode::End => self.move_cursor_to_end_of_line(false),
            KeyCode::Backspace if word => self.delete_backward_word(),
            KeyCode::Backspace => self.replace_range(self.previous_boundary()..self.cursor_pos, ""),
            KeyCode::Delete => self.delete_forward(word),
            KeyCode::Char(c) => self.character_key(c, mods),
            _ => {}
        }
    }

    fn character_key(&mut self, c: char, mods: KeyModifiers) {
        let key = KeyEvent::new(KeyCode::Char(c), mods);
        if super::printable_key(&key, c) {
            self.insert_str(&c.to_string());
            return;
        }
        match (c, mods) {
            ('b', KeyModifiers::ALT) => self.set_cursor(self.beginning_of_previous_word()),
            ('f', KeyModifiers::ALT) => self.set_cursor(self.end_of_next_word()),
            ('w', KeyModifiers::CONTROL) => self.delete_backward_word(),
            ('d', KeyModifiers::CONTROL) => self.delete_forward(false),
            ('u', KeyModifiers::CONTROL) => self.kill_to_beginning_of_line(),
            ('k', KeyModifiers::CONTROL) => self.kill_to_end_of_line(),
            ('a', KeyModifiers::CONTROL) => self.move_cursor_to_beginning_of_line(true),
            ('e', KeyModifiers::CONTROL) => self.move_cursor_to_end_of_line(true),
            ('j' | 'm', KeyModifiers::CONTROL) => self.insert_str("\n"),
            _ => {}
        }
    }

    fn delete_forward(&mut self, word: bool) {
        let end = if word {
            self.end_of_next_word()
        } else {
            self.next_boundary()
        };
        self.replace_range(self.cursor_pos..end, "");
    }

    fn delete_backward_word(&mut self) {
        self.replace_range(self.beginning_of_previous_word()..self.cursor_pos, "");
    }

    fn kill_to_end_of_line(&mut self) {
        let eol = self.end_of_current_line();
        if self.cursor_pos != eol {
            self.replace_range(self.cursor_pos..eol, "");
        } else if eol < self.text.len() {
            self.replace_range(self.cursor_pos..eol + 1, "");
        }
    }

    fn kill_to_beginning_of_line(&mut self) {
        let bol = self.beginning_of_current_line();
        if self.cursor_pos != bol {
            self.replace_range(bol..self.cursor_pos, "");
        } else if bol > 0 {
            self.replace_range(bol - 1..bol, "");
        }
    }

    fn move_cursor_to_beginning_of_line(&mut self, move_up_at_bol: bool) {
        let bol = self.beginning_of_current_line();
        if move_up_at_bol && self.cursor_pos == bol {
            self.set_cursor(self.beginning_of_line(self.cursor_pos.saturating_sub(1)));
        } else {
            self.set_cursor(bol);
        }
    }

    fn move_cursor_to_end_of_line(&mut self, move_down_at_eol: bool) {
        let eol = self.end_of_current_line();
        if move_down_at_eol && self.cursor_pos == eol {
            let next_pos = self.cursor_pos.saturating_add(1).min(self.text.len());
            self.set_cursor(self.end_of_line(next_pos));
        } else {
            self.set_cursor(eol);
        }
    }

    fn wrapped_line_index_by_start(rows: &[Row], pos: usize) -> usize {
        rows.partition_point(|r| r.range.start <= pos)
            .saturating_sub(1)
    }

    fn move_vertical(&mut self, down: bool) {
        let cursor = self.cursor_pos;
        let preferred = self.preferred_col;
        let rows = self.wrapped_rows(self.width.max(1));
        let idx = Self::wrapped_line_index_by_start(rows, cursor);
        let col = row_column(&rows[idx], cursor);
        let target_col = preferred.unwrap_or(col);
        let target = if down { idx + 1 } else { idx.saturating_sub(1) };
        if target == idx || target >= rows.len() {
            self.cursor_pos = if down { self.text.len() } else { 0 };
            self.preferred_col = None;
            return;
        }
        let pos = row_position(rows, target, target_col);
        self.cursor_pos = pos;
        self.preferred_col = Some(target_col);
    }

    fn wrapped_rows(&mut self, width: u16) -> &[Row] {
        self.width = width.max(1);
        let fresh = self
            .wrap_cache
            .as_ref()
            .is_some_and(|cache| cache.width == self.width && cache.revision == self.revision);
        if !fresh {
            let cache = WrapCache::new(&self.text, self.revision, self.width);
            self.wrap_cache = Some(cache);
        }
        match &self.wrap_cache {
            Some(cache) => &cache.rows,
            None => unreachable!("wrap cache was initialized above"),
        }
    }

    /// Copied/adapted Codex invariant: cursor visible; no scroll when text fits.
    fn effective_scroll(
        height: usize,
        rows: &[Row],
        cursor: usize,
        current_scroll: usize,
    ) -> usize {
        let total_lines = rows.len();
        if height >= total_lines {
            return 0;
        }
        let cursor_line_idx = Self::wrapped_line_index_by_start(rows, cursor);
        let max_scroll = total_lines.saturating_sub(height);
        let mut scroll = current_scroll.min(max_scroll);
        if cursor_line_idx < scroll {
            scroll = cursor_line_idx;
        } else if cursor_line_idx >= scroll + height {
            scroll = cursor_line_idx + 1 - height;
        }
        scroll
    }

    pub(crate) fn line_count(&mut self, width: u16) -> usize {
        self.wrapped_rows(width.saturating_sub(1).max(1)).len()
    }

    pub(crate) fn row_extent(&self) -> (usize, usize) {
        let total = self.wrap_cache.as_ref().map_or(1, |cache| cache.rows.len());
        (self.scroll, total)
    }

    pub(crate) fn view(&mut self, area: Rect) -> (Vec<Line<'static>>, Option<(u16, u16)>) {
        let cursor = self.cursor_pos;
        let scroll = self.scroll;
        let width = area.width.saturating_sub(1).max(1);
        let rows = self.wrapped_rows(width);
        if area.width == 0 || area.height == 0 {
            return (Vec::new(), None);
        }
        let scroll = Self::effective_scroll(usize::from(area.height), rows, cursor, scroll);
        let idx = Self::wrapped_line_index_by_start(rows, cursor);
        let column = row_column(&rows[idx], cursor).min(usize::from(area.width - 1));
        let position = (area.x + column as u16, area.y + (idx - scroll) as u16);
        let lines = rows
            .iter()
            .skip(scroll)
            .take(usize::from(area.height))
            .map(row_line)
            .collect();
        self.scroll = scroll;
        (lines, Some(position))
    }
}

fn row_column(row: &Row, pos: usize) -> usize {
    row.cells
        .iter()
        .take_while(|cell| cell.range.end <= pos)
        .map(|cell| cell.width)
        .sum()
}

fn row_position(rows: &[Row], idx: usize, target_col: usize) -> usize {
    let row = &rows[idx];
    for cell in &row.cells {
        if cell.column + cell.width > target_col {
            return cell.range.start;
        }
    }
    // A soft-wrap boundary belongs to the following row, not this one.
    if rows
        .get(idx + 1)
        .is_some_and(|next| next.range.start == row.range.end)
    {
        return row
            .cells
            .last()
            .map(|cell| cell.range.start)
            .unwrap_or(row.range.start);
    }
    row.range.end
}

fn row_line(row: &Row) -> Line<'static> {
    Line::raw(
        row.cells
            .iter()
            .map(|cell| cell.symbol.as_str())
            .collect::<String>(),
    )
}

fn wrap_cells(text: &str, width: usize) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut row = Row::default();
    for (start, grapheme) in text.grapheme_indices(true) {
        let end = start + grapheme.len();
        if grapheme == "\n" {
            row.range.end = start;
            rows.push(row);
            row = Row::at(end);
            continue;
        }
        let symbol = super::safe_text(grapheme);
        let size = Line::from(symbol.as_str()).width();
        let (symbol, size) = if size > width {
            ("�".to_owned(), 1)
        } else {
            (symbol, size)
        };
        if row.columns + size > width {
            rows.push(row);
            row = Row::at(start);
        }
        row.cells.push(Cell {
            range: start..end,
            symbol,
            column: row.columns,
            width: size,
        });
        row.columns += size;
        row.range.end = end;
    }
    let full = row.columns == width;
    rows.push(row);
    if full {
        rows.push(Row {
            range: text.len()..text.len(),
            ..Row::default()
        });
    }
    rows
}
