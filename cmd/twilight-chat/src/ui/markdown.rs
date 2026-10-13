// Copyright 2025 OpenAI
// SPDX-License-Identifier: Apache-2.0
// Adapted from openai/codex, rust-v0.20.0 (45aed5edd139c38132de044596fdd55e47236657),
// codex-rs/tui/src/markdown.rs. License and upstream notice: ../../licenses/codex/.
// MODIFIED: presentation-only adapter; sanitize terminal controls, omit filesystem/citation
// rewriting, extract owned-line conversion, and preserve unsupported raw HTML as text.

use std::ops::Range;

use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag};
use ratatui::text::{Line, Span};

use super::safe_text;

pub(super) fn lines(source: &str, width: u16) -> Vec<Line<'static>> {
    if super::diff::is_diff(source) {
        return super::diff::lines(source, width);
    }
    // Preserve code indentation as four terminal cells instead of dropping tabs.
    let source = safe_text(&source.replace('\t', "    "));
    // tui-markdown drops HTML events. Preserve them as literal text instead.
    let source = literal_html(&source);
    let mut lines = Vec::new();
    let mut cursor = 0;
    for (range, body) in diff_blocks(&source) {
        lines.extend(plain_markdown(&source[cursor..range.start]));
        lines.extend(super::diff::lines(&body, width));
        cursor = range.end;
    }
    lines.extend(plain_markdown(&source[cursor..]));
    lines
}

fn plain_markdown(source: &str) -> Vec<Line<'static>> {
    tui_markdown::from_str(source)
        .lines
        .into_iter()
        .map(owned_line)
        .collect()
}

fn diff_blocks(source: &str) -> Vec<(Range<usize>, String)> {
    Parser::new_ext(source, Options::ENABLE_STRIKETHROUGH)
        .into_offset_iter()
        .filter_map(|(event, range)| diff_block(event, range, source))
        .collect()
}

fn diff_block(
    event: Event<'_>,
    range: Range<usize>,
    source: &str,
) -> Option<(Range<usize>, String)> {
    let Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(language))) = event else {
        return None;
    };
    let language = language.split_ascii_whitespace().next().unwrap_or_default();
    if !language.eq_ignore_ascii_case("diff") && !language.eq_ignore_ascii_case("patch") {
        return None;
    }
    let body = Parser::new(&source[range.clone()])
        .filter_map(code_text)
        .collect();
    Some((range, body))
}

fn code_text(event: Event<'_>) -> Option<String> {
    match event {
        Event::Text(text) => Some(text.into_string()),
        _ => None,
    }
}

// Codex's HistoryCell owns its lines: do not retain parser/input lifetimes in the cache.
fn owned_line(borrowed_line: Line<'_>) -> Line<'static> {
    let owned_spans = borrowed_line
        .spans
        .iter()
        .map(|span| Span::styled(span.content.to_string(), span.style))
        .collect::<Vec<_>>();
    let owned_line = Line::from(owned_spans).style(borrowed_line.style);
    match borrowed_line.alignment {
        Some(alignment) => owned_line.alignment(alignment),
        None => owned_line,
    }
}

fn literal_html(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0;
    let parser = Parser::new_ext(source, Options::ENABLE_STRIKETHROUGH);
    for (event, range) in parser.into_offset_iter() {
        if !matches!(event, Event::Html(_) | Event::InlineHtml(_)) {
            continue;
        }
        output.push_str(&source[cursor..range.start]);
        output.push_str(&source[range.clone()].replace('<', "&lt;"));
        cursor = range.end;
    }
    output.push_str(&source[cursor..]);
    output
}
