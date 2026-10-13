// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// MODIFIED: UI-only unified-diff preview adapted from
// codex-rs/tui/src/diff_render.rs at
// 9b738582b13c2cdbeff54af0afd04c50c3e7ba09.
// DiffLineType, line_number_width, styled-span wrapping and gutter/sign layout
// derive from Codex. Parsing, dual gutters and terminal-safe narrow wrapping
// are local adaptations; no patch is applied and no files are inspected.

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Clone, Copy, Debug, PartialEq)]
enum DiffLineType {
    Insert,
    Delete,
    Context,
}

struct Row<'a> {
    text: &'a str,
    kind: Option<DiffLineType>,
    old: Option<usize>,
    new: Option<usize>,
}

#[derive(Clone, Copy)]
struct Range {
    start: usize,
    count: usize,
}

struct Hunk {
    old: Range,
    new: Range,
    old_used: usize,
    new_used: usize,
    marker_allowed: bool,
}

/// Recognize file-header pairs, not ordinary prose containing +/- characters.
pub(super) fn is_diff(source: &str) -> bool {
    let source = body(source);
    let raw: Vec<_> = source.lines().collect();
    let Some(index) = raw.iter().position(|text| !metadata(text)) else {
        return false;
    };
    raw.get(index..index + 3).is_some_and(diff_start)
}

fn diff_start(rows: &[&str]) -> bool {
    rows[0].starts_with("--- ")
        && rows[0].len() > 4
        && rows[1].starts_with("+++ ")
        && rows[1].len() > 4
        && rows[2].starts_with("@@ ")
}

/// Produce already-wrapped, read-only rows. Invalid patches have no numbers.
pub(super) fn lines(source: &str, width: u16) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let source = body(source);
    let raw: Vec<_> = source.lines().collect();
    let parsed = parse(&raw);
    let label = if parsed.is_some() {
        "Diff preview"
    } else {
        "Diff preview · unvalidated"
    };
    let width = usize::from(width);
    let mut result = plain_rows(label, Style::default().fg(Color::Cyan), width);
    match parsed {
        Some(rows) => render_valid(&rows, width, &mut result),
        None => render_raw(&raw, width, &mut result),
    }
    result
}

fn body(source: &str) -> &str {
    let Some((opening, rest)) = source.split_once('\n') else {
        return source;
    };
    if !matches!(
        opening.trim(),
        "```diff" | "```patch" | "~~~diff" | "~~~patch"
    ) {
        return source;
    }
    let fence = &opening.trim()[..3];
    let trimmed = rest.trim_end_matches(['\r', '\n']);
    let Some((content, closing)) = trimmed.rsplit_once('\n') else {
        return rest;
    };
    if closing.trim() == fence {
        content
    } else {
        rest
    }
}

fn parse<'a>(raw: &[&'a str]) -> Option<Vec<Row<'a>>> {
    let mut rows = Vec::new();
    let mut index = 0;
    let mut files = 0;
    while index < raw.len() {
        consume_metadata(raw, &mut index, &mut rows)?;
        let text = *raw.get(index)?;
        if !text.starts_with("--- ") || text.len() <= 4 {
            return None;
        }
        let next = *raw.get(index + 1)?;
        if !next.starts_with("+++ ") || next.len() <= 4 {
            return None;
        }
        rows.extend([header(text), header(next)]);
        index += 2;
        parse_file(raw, &mut index, &mut rows)?;
        files += 1;
    }
    (files > 0).then_some(rows)
}

fn consume_metadata<'a>(raw: &[&'a str], index: &mut usize, rows: &mut Vec<Row<'a>>) -> Option<()> {
    let mut git_header = false;
    while let Some(text) = raw.get(*index).filter(|text| metadata(text)) {
        if text.starts_with("diff --git ") && std::mem::replace(&mut git_header, true) {
            return None;
        }
        rows.push(header(text));
        *index += 1;
    }
    Some(())
}

fn metadata(text: &str) -> bool {
    [
        "diff --git ",
        "index ",
        "new file mode ",
        "deleted file mode ",
        "old mode ",
        "new mode ",
        "similarity index ",
        "dissimilarity index ",
        "rename from ",
        "rename to ",
        "copy from ",
        "copy to ",
    ]
    .iter()
    .any(|prefix| text.starts_with(prefix))
}

fn header(text: &str) -> Row<'_> {
    Row {
        text,
        kind: None,
        old: None,
        new: None,
    }
}

fn parse_file<'a>(raw: &[&'a str], index: &mut usize, rows: &mut Vec<Row<'a>>) -> Option<()> {
    let mut previous: Option<(usize, usize)> = None;
    let mut hunks = 0;
    while let Some(text) = raw.get(*index) {
        if !text.starts_with("@@ ") {
            break;
        }
        let mut hunk = parse_hunk(text)?;
        check_order(previous, &hunk)?;
        previous = Some((hunk.old.boundary_end()?, hunk.new.boundary_end()?));
        rows.push(header(text));
        *index += 1;
        consume_hunk(raw, index, rows, &mut hunk)?;
        hunks += 1;
    }
    (hunks > 0).then_some(())
}

fn check_order(previous: Option<(usize, usize)>, hunk: &Hunk) -> Option<()> {
    if let Some((old, new)) = previous {
        if hunk.old.boundary_start()? < old || hunk.new.boundary_start()? < new {
            return None;
        }
    }
    Some(())
}

impl Range {
    fn end(self) -> Option<usize> {
        self.start.checked_add(self.count)
    }

    fn boundary_start(self) -> Option<usize> {
        self.start.checked_add(usize::from(self.count == 0))
    }

    fn boundary_end(self) -> Option<usize> {
        if self.count == 0 {
            self.boundary_start()
        } else {
            self.end()
        }
    }

    fn parse(source: &str, sign: char) -> Option<Self> {
        let source = source.strip_prefix(sign)?;
        let (start, count) = source.split_once(',').unwrap_or((source, "1"));
        let range = Self {
            start: decimal(start)?,
            count: decimal(count)?,
        };
        if range.count > 0 && range.start == 0 {
            return None;
        }
        range.boundary_end()?;
        Some(range)
    }
}

fn decimal(source: &str) -> Option<usize> {
    if source.is_empty() || !source.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    source.parse().ok()
}

fn parse_hunk(text: &str) -> Option<Hunk> {
    let rest = text.strip_prefix("@@ ")?;
    let (ranges, suffix) = rest.split_once(" @@")?;
    if !suffix.is_empty() && !suffix.starts_with(' ') {
        return None;
    }
    let (old, new) = ranges.split_once(' ')?;
    Some(Hunk {
        old: Range::parse(old, '-')?,
        new: Range::parse(new, '+')?,
        old_used: 0,
        new_used: 0,
        marker_allowed: false,
    })
}

fn consume_hunk<'a>(
    raw: &[&'a str],
    index: &mut usize,
    rows: &mut Vec<Row<'a>>,
    hunk: &mut Hunk,
) -> Option<()> {
    while let Some(text) = raw.get(*index) {
        if *text == "\\ No newline at end of file" && !hunk.marker_allowed {
            return None;
        }
        if *text == "\\ No newline at end of file" {
            rows.push(header(text));
            hunk.marker_allowed = false;
            *index += 1;
            continue;
        }
        if hunk.old_used == hunk.old.count && hunk.new_used == hunk.new.count {
            return Some(());
        }
        rows.push(hunk_row(text, hunk)?);
        *index += 1;
    }
    (hunk.old_used == hunk.old.count && hunk.new_used == hunk.new.count).then_some(())
}

fn hunk_row<'a>(text: &'a str, hunk: &mut Hunk) -> Option<Row<'a>> {
    let kind = match text.as_bytes().first()? {
        b'+' => DiffLineType::Insert,
        b'-' => DiffLineType::Delete,
        b' ' => DiffLineType::Context,
        _ => return None,
    };
    let old = advance(hunk.old, &mut hunk.old_used, kind != DiffLineType::Insert)?;
    let new = advance(hunk.new, &mut hunk.new_used, kind != DiffLineType::Delete)?;
    hunk.marker_allowed = true;
    Some(Row {
        text: &text[1..],
        kind: Some(kind),
        old,
        new,
    })
}

fn advance(range: Range, used: &mut usize, present: bool) -> Option<Option<usize>> {
    if !present {
        return Some(None);
    }
    if *used >= range.count {
        return None;
    }
    let number = range.start.checked_add(*used)?;
    *used += 1;
    Some(Some(number))
}

fn render_valid(rows: &[Row<'_>], width: usize, result: &mut Vec<Line<'static>>) {
    let maximum = rows
        .iter()
        .flat_map(|row| [row.old, row.new])
        .flatten()
        .max();
    let digits = line_number_width(maximum.unwrap_or(0));
    for row in rows {
        match row.kind {
            Some(kind) => render_numbered(row, kind, digits, width, result),
            None => result.extend(plain_rows(row.text, header_style(row.text), width)),
        }
    }
}

fn render_raw(raw: &[&str], width: usize, result: &mut Vec<Line<'static>>) {
    for text in raw {
        let style = raw_style(text);
        result.extend(plain_rows(text, style, width));
    }
}

fn raw_style(text: &str) -> Style {
    if text.starts_with("+++ ") || text.starts_with("--- ") || text.starts_with("@@") {
        return header_style(text);
    }
    match text.as_bytes().first() {
        Some(b'+') => line_style(DiffLineType::Insert),
        Some(b'-') => line_style(DiffLineType::Delete),
        _ => line_style(DiffLineType::Context),
    }
}

fn header_style(text: &str) -> Style {
    let color = if text.starts_with("@@") {
        Color::Cyan
    } else {
        Color::Gray
    };
    Style::default().fg(color)
}

fn line_style(kind: DiffLineType) -> Style {
    let color = match kind {
        DiffLineType::Insert => Color::Green,
        DiffLineType::Delete => Color::Red,
        DiffLineType::Context => Color::Gray,
    };
    Style::default().fg(color)
}

fn sign(kind: DiffLineType) -> char {
    match kind {
        DiffLineType::Insert => '+',
        DiffLineType::Delete => '-',
        DiffLineType::Context => ' ',
    }
}

fn render_numbered(
    row: &Row<'_>,
    kind: DiffLineType,
    digits: usize,
    width: usize,
    result: &mut Vec<Line<'static>>,
) {
    let style = line_style(kind);
    if width == 1 {
        let text = format!("{}{}", sign(kind), row.text);
        result.extend(plain_rows(&text, style, width));
        return;
    }
    let gutter = number_gutter(row, digits);
    let prefix = if gutter.len() + 2 < width {
        format!("{gutter}{} ", sign(kind))
    } else {
        sign(kind).to_string()
    };
    let content = Span::styled(sanitize(row.text), style);
    let available = width - prefix.len();
    let chunks = wrap_styled_spans(&[content], available);
    append_chunks(chunks, prefix, style, result);
}

fn number_gutter(row: &Row<'_>, digits: usize) -> String {
    let old = row.old.map(|n| n.to_string()).unwrap_or_default();
    let new = row.new.map(|n| n.to_string()).unwrap_or_default();
    format!("{old:>digits$} {new:>digits$} ")
}

fn append_chunks(
    chunks: Vec<Vec<Span<'static>>>,
    prefix: String,
    style: Style,
    result: &mut Vec<Line<'static>>,
) {
    for (index, chunk) in chunks.into_iter().enumerate() {
        let mut spans = prefix_spans(&prefix, style, index == 0);
        spans.extend(chunk);
        result.push(Line::from(spans));
    }
}

fn prefix_spans(prefix: &str, style: Style, first: bool) -> Vec<Span<'static>> {
    let gutter_style = Style::default().fg(Color::Gray);
    if !first {
        return vec![Span::styled(" ".repeat(prefix.len()), gutter_style)];
    }
    if prefix.len() <= 2 {
        return vec![Span::styled(prefix.to_owned(), style)];
    }
    let (numbers, sign) = prefix.split_at(prefix.len() - 2);
    vec![
        Span::styled(numbers.to_owned(), gutter_style),
        Span::styled(sign.to_owned(), style),
    ]
}

fn sanitize(text: &str) -> String {
    super::safe_text(&text.replace('\t', "    "))
}

fn plain_rows(text: &str, style: Style, width: usize) -> Vec<Line<'static>> {
    let spans = [Span::styled(sanitize(text), style)];
    wrap_styled_spans(&spans, width)
        .into_iter()
        .map(Line::from)
        .collect()
}

/// Codex's span-chunk algorithm, with extracted steps to keep nesting shallow.
/// Oversize graphemes become a replacement glyph rather than overflowing.
fn wrap_styled_spans(spans: &[Span<'static>], max_cols: usize) -> Vec<Vec<Span<'static>>> {
    if max_cols == 0 {
        return Vec::new();
    }
    let mut result = Vec::new();
    let mut current = Vec::new();
    let mut col = 0;
    for span in spans {
        wrap_span(span, max_cols, &mut result, &mut current, &mut col);
    }
    if !current.is_empty() || result.is_empty() {
        result.push(current);
    }
    result
}

fn wrap_span(
    span: &Span<'static>,
    max_cols: usize,
    result: &mut Vec<Vec<Span<'static>>>,
    current: &mut Vec<Span<'static>>,
    col: &mut usize,
) {
    let mut remaining = span.content.as_ref();
    while !remaining.is_empty() {
        let (bytes, columns) = fitting_chunk(remaining, max_cols - *col);
        if bytes == 0 {
            wrap_fallback(&mut remaining, span.style, max_cols, result, current, col);
            continue;
        }
        let (chunk, rest) = remaining.split_at(bytes);
        current.push(Span::styled(chunk.to_owned(), span.style));
        *col += columns;
        remaining = rest;
        if *col == max_cols {
            result.push(std::mem::take(current));
            *col = 0;
        }
    }
}

fn fitting_chunk(text: &str, available: usize) -> (usize, usize) {
    let mut bytes = 0;
    let mut columns = 0;
    for grapheme in text.graphemes(true) {
        let width = Line::raw(grapheme).width();
        if columns + width > available {
            break;
        }
        bytes += grapheme.len();
        columns += width;
    }
    (bytes, columns)
}

fn wrap_fallback(
    remaining: &mut &str,
    style: Style,
    max_cols: usize,
    result: &mut Vec<Vec<Span<'static>>>,
    current: &mut Vec<Span<'static>>,
    col: &mut usize,
) {
    if !current.is_empty() {
        result.push(std::mem::take(current));
    }
    // The caller supplies nonempty text, and each iteration consumes a grapheme.
    let Some(grapheme) = remaining.graphemes(true).next() else {
        return;
    };
    let width = Line::raw(grapheme).width();
    let symbol = if width > max_cols { "�" } else { grapheme };
    current.push(Span::styled(symbol.to_owned(), style));
    *col = Line::raw(symbol).width();
    *remaining = &remaining[grapheme.len()..];
}

fn line_number_width(max_line_number: usize) -> usize {
    if max_line_number == 0 {
        1
    } else {
        max_line_number.to_string().len()
    }
}
