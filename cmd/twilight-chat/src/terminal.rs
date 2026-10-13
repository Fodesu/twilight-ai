//! Terminal ownership and local editing; business decisions belong to the reducer.
use std::{io::Stdout, time::Duration};

use anyhow::Result;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags,
    PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::{
    cursor::MoveTo,
    event::{DisableBracketedPaste, DisableFocusChange, EnableBracketedPaste, EnableFocusChange},
    execute, queue,
    style::Print,
    terminal::{Clear, ClearType},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Rect, Size},
};

use crate::app::{event::UiAction, state::AppState};
use crate::custom_terminal::{Frame, Terminal as InlineTerminal};
use crate::history_insert;
use crate::ui::{self, host_form::HostForm, textarea::TextArea, RenderCaches};

pub struct TerminalUi {
    terminal: InlineTerminal<CrosstermBackend<Stdout>>,
    keyboard_enhancement: bool,
    input: LocalInput,
    caches: RenderCaches,
    history: HistoryLedger,
    screen_size: Size,
    host_editor: bool,
}

impl TerminalUi {
    pub fn new() -> Result<Self> {
        let size = crossterm::terminal::size()?;
        let keyboard_enhancement = matches!(
            crossterm::terminal::supports_keyboard_enhancement(),
            Ok(true)
        );
        let terminal =
            init_for_screen(size, keyboard_enhancement).inspect_err(|_| ratatui::restore())?;
        let screen_size = terminal.size()?;
        Ok(Self {
            terminal,
            keyboard_enhancement,
            input: LocalInput::default(),
            caches: RenderCaches,
            history: HistoryLedger::default(),
            screen_size,
            host_editor: true,
        })
    }

    pub fn poll(&mut self, state: &AppState) -> Result<Vec<UiAction>> {
        self.input.sync(state);
        poll_events(&mut self.input, state)
    }

    pub fn draw(&mut self, state: &AppState) -> Result<()> {
        self.sync_terminal_size()?;
        self.input.sync(state);
        if self.host_editor != state.ui.host_editor {
            execute!(self.terminal.backend_mut(), Clear(ClearType::All))?;
            self.history = HistoryLedger::default();
            let mut area = self.terminal.viewport_area;
            area.y = 0;
            self.terminal.set_viewport_area(area);
            self.terminal.invalidate();
            self.host_editor = state.ui.host_editor;
        }
        let desired_height = ui::desired_height(
            self.screen_size.width,
            self.screen_size.height,
            state,
            &mut self.input.editor,
        );
        self.ensure_viewport(desired_height)?;
        self.terminal.autoresize()?;
        if !state.ui.host_editor {
            self.sync_history(state)?;
        }
        let form = self.input.form.as_ref();
        let caches = &mut self.caches;
        let editor = &mut self.input.editor;
        self.terminal
            .draw(|frame| render_frame(frame, state, form, caches, editor))?;
        Ok(())
    }
}

impl Drop for TerminalUi {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), DisableBracketedPaste, DisableFocusChange,);
        if self.keyboard_enhancement {
            let _ = execute!(std::io::stdout(), PopKeyboardEnhancementFlags);
        }
        ratatui::restore();
    }
}

fn render_frame(
    frame: &mut Frame<'_>,
    state: &AppState,
    form: Option<&HostForm>,
    caches: &mut RenderCaches,
    editor: &mut TextArea,
) {
    ui::render_with_composer(frame, state, form, caches, editor);
}

#[derive(Default)]
struct HistoryLedger {
    viewport: Option<(u64, String, u16)>,
    // Native scrollback is append-only during normal operation. Track stable
    // turn identities, not mutable message signatures; a later snapshot may
    // enrich status/text without making already-written rows replaceable.
    records: Vec<String>,
}

impl TerminalUi {
    fn sync_terminal_size(&mut self) -> Result<()> {
        let size = self.terminal.size()?;
        if size == self.screen_size {
            return Ok(());
        }
        execute!(self.terminal.backend_mut(), Clear(ClearType::All))?;
        self.terminal.resize(size)?;
        self.screen_size = size;
        self.terminal
            .set_viewport_area(Rect::new(0, 0, size.width, size.height));
        self.history = HistoryLedger::default();
        Ok(())
    }

    fn ensure_viewport(&mut self, height: u16) -> Result<()> {
        let previous = self.terminal.viewport_area;
        let area = resize_inline_viewport(
            self.terminal.backend_mut(),
            previous,
            self.screen_size,
            height,
        )?;
        self.terminal.set_viewport_area(area);
        Ok(())
    }

    fn sync_history(&mut self, state: &AppState) -> Result<()> {
        if !history_is_consolidated(state) {
            return Ok(());
        }
        let width = self.terminal.size()?.width;
        let viewport = (
            state.connection.epoch,
            state.connection.config.session.clone().unwrap_or_default(),
            width,
        );
        let records = history_records(state);
        let reset = self.history.viewport.as_ref() != Some(&viewport);
        if reset {
            self.reset_inline_history()?;
            self.history.records.clear();
        } else if !history_prefix_matches(&self.history.records, &records) {
            // Native scrollback is not safely editable. A normal completion
            // must never purge already-rendered turns just because a snapshot
            // changed a message signature or temporarily exposed a different
            // committed prefix. A real session/width change above is the only
            // normal path allowed to rebuild it.
            return Ok(());
        }
        let new_keys = records[self.history.records.len()..].to_vec();
        if !new_keys.is_empty() {
            let lines = ui::transcript::history_turn_lines(state, width, &new_keys);
            // Full-screen insertion works even with a full-height viewport and
            // invalidates Ratatui's buffers after the external ANSI mutation.
            // Advance the ledger only after all writes have succeeded.
            history_insert::insert_history_lines(&mut self.terminal, lines)?;
        }
        self.history.viewport = Some(viewport);
        self.history.records = records;
        Ok(())
    }

    fn reset_inline_history(&mut self) -> Result<()> {
        execute!(
            self.terminal.backend_mut(),
            Clear(ClearType::Purge),
            Clear(ClearType::All)
        )?;
        let mut area = self.terminal.viewport_area;
        area.y = 0;
        self.terminal.set_viewport_area(area);
        self.terminal.invalidate();
        Ok(())
    }
}

// Like Codex's inline viewport: preserve the top on shrink, grow downward,
// and scroll only the overflow. Bottom anchoring on every frame creates gaps
// between completed turns and can scroll away unrelated history on growth.
fn resize_inline_viewport(
    writer: &mut impl std::io::Write,
    previous: Rect,
    screen: Size,
    height: u16,
) -> Result<Rect> {
    let mut area = Rect::new(0, previous.y, screen.width, height.min(screen.height));
    let overflow = area.bottom().saturating_sub(screen.height);
    if area != previous {
        // Clear the mutable tail before scrolling, so composer/streaming rows
        // never accidentally become permanent terminal history.
        queue!(
            writer,
            MoveTo(0, previous.y),
            Clear(ClearType::FromCursorDown)
        )?;
        if overflow > 0 {
            queue!(
                writer,
                history_insert::ResetScrollRegion,
                MoveTo(0, screen.height.saturating_sub(1))
            )?;
            for _ in 0..overflow {
                queue!(writer, Print("\r\n"))?;
            }
            area.y = screen.height.saturating_sub(area.height);
        }
    }
    Ok(area)
}

fn init_for_screen(
    size: (u16, u16),
    keyboard_enhancement: bool,
) -> Result<InlineTerminal<CrosstermBackend<Stdout>>> {
    init_inline_terminal(Size::new(size.0, size.1), size.1, keyboard_enhancement)
}

fn init_inline_terminal(
    screen_size: Size,
    height: u16,
    keyboard_enhancement: bool,
) -> Result<InlineTerminal<CrosstermBackend<Stdout>>> {
    crossterm::terminal::enable_raw_mode()?;
    let backend = CrosstermBackend::new(std::io::stdout());
    let mut terminal = InlineTerminal::with_options(backend)?;
    terminal.set_viewport_area(Rect::new(0, 0, screen_size.width, height));
    if keyboard_enhancement {
        let flags = KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES;
        execute!(std::io::stdout(), PushKeyboardEnhancementFlags(flags))?;
    }
    execute!(std::io::stdout(), EnableBracketedPaste, EnableFocusChange)?;
    Ok(terminal)
}

fn history_is_consolidated(state: &AppState) -> bool {
    state.session.loaded && !state.session.dirty && !state.session.snapshot_inflight
}

fn history_records(state: &AppState) -> Vec<String> {
    ui::presentation::build(state)
        .committed
        .iter()
        .map(ui::presentation::turn_key)
        .collect()
}

fn history_prefix_matches(previous: &[String], current: &[String]) -> bool {
    previous.len() <= current.len()
        && previous
            .iter()
            .zip(current)
            .all(|(previous, current)| previous == current)
}

fn poll_events(input: &mut LocalInput, state: &AppState) -> Result<Vec<UiAction>> {
    drain_events(input, state, read_ready_event)
}

fn read_ready_event() -> Result<Option<Event>> {
    if event::poll(Duration::ZERO)? {
        return Ok(Some(event::read()?));
    }
    Ok(None)
}

fn drain_events(
    input: &mut LocalInput,
    state: &AppState,
    mut next: impl FnMut() -> Result<Option<Event>>,
) -> Result<Vec<UiAction>> {
    let mut actions = Vec::new();
    for _ in 0..32 {
        let Some(event) = next()? else {
            break;
        };
        input.handle(event, state, &mut actions);
        if actions.last().is_some_and(ends_batch) {
            break;
        }
    }
    Ok(actions)
}

fn ends_batch(action: &UiAction) -> bool {
    matches!(
        action,
        UiAction::Quit | UiAction::OpenHost | UiAction::CloseHost | UiAction::Configure(_)
    )
}

#[derive(Default)]
struct LocalInput {
    form: Option<HostForm>,
    draft_revision: Option<u64>,
    editor: TextArea,
}

impl LocalInput {
    fn sync(&mut self, state: &AppState) {
        self.sync_draft(state);
        if !state.ui.host_editor {
            self.form = None;
        } else if self.form.is_none() {
            self.form = Some(HostForm::new(&state.connection.config));
        }
    }

    fn sync_draft(&mut self, state: &AppState) {
        if self.draft_revision == Some(state.ui.draft_revision) {
            return;
        }
        if self.editor.text() != state.ui.draft {
            self.editor.reset(&state.ui.draft);
        }
        self.draft_revision = Some(state.ui.draft_revision);
    }

    fn handle(&mut self, event: Event, state: &AppState, actions: &mut Vec<UiAction>) {
        match event {
            Event::Key(key) if key.kind != KeyEventKind::Release => self.key(key, state, actions),
            Event::Paste(text) => self.paste(&text, state, actions),
            _ => {}
        }
    }

    fn key(&mut self, key: KeyEvent, state: &AppState, actions: &mut Vec<UiAction>) {
        if is_quit(key) {
            actions.push(UiAction::Quit);
            return;
        }
        if state.ui.host_editor {
            self.form_key(key, actions);
            return;
        }
        self.chat_key(key, actions);
    }

    fn form_key(&mut self, key: KeyEvent, actions: &mut Vec<UiAction>) {
        let action = self.form.as_mut().and_then(|form| form.handle_key(key));
        actions.extend(action);
    }

    fn chat_key(&mut self, key: KeyEvent, actions: &mut Vec<UiAction>) {
        if is_newline(key) {
            self.edit("\n", actions);
            return;
        }
        match key.code {
            KeyCode::Enter => actions.push(submit_action(self.editor.text())),
            KeyCode::Tab if key.modifiers.is_empty() => self.edit("    ", actions),
            _ => self.editor_key(key, actions),
        }
    }

    fn paste(&mut self, text: &str, state: &AppState, actions: &mut Vec<UiAction>) {
        let text = ui::paste_text(text);
        if !state.ui.host_editor {
            self.edit(&text, actions);
            return;
        }
        if let Some(form) = self.form.as_mut() {
            form.paste(&text);
        }
    }

    fn edit(&mut self, text: &str, actions: &mut Vec<UiAction>) {
        if !text.is_empty() {
            self.editor.insert_str(text);
            actions.push(UiAction::Edit(self.editor.text().to_owned()));
        }
    }

    fn editor_key(&mut self, key: KeyEvent, actions: &mut Vec<UiAction>) {
        let before = self.editor.text().to_owned();
        self.editor.input(key);
        if self.editor.text() != before {
            actions.push(UiAction::Edit(self.editor.text().to_owned()));
        }
    }
}

fn is_newline(key: KeyEvent) -> bool {
    let modifiers = KeyModifiers::SHIFT | KeyModifiers::ALT;
    key.code == KeyCode::Enter && key.modifiers.intersects(modifiers)
}

fn is_quit(key: KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
}

fn submit_action(draft: &str) -> UiAction {
    match draft.trim() {
        "/host" => UiAction::OpenHost,
        "/disconnect" => UiAction::Disconnect,
        "/resume" => UiAction::Resume,
        "/stop" => UiAction::Stop,
        "/events" => UiAction::ToggleEvents,
        "/refresh" => UiAction::Refresh,
        "/exit" => UiAction::Quit,
        _ => UiAction::Send,
    }
}
