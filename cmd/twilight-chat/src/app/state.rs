use chat_client::config::HostConfig;
use chat_protocol::{
    ActiveEffect, Approval, Capabilities, Command, CommandBody, Cursor, EffectKey, Message, Turn,
    TurnStatus,
};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionPhase {
    Unconfigured,
    Connecting,
    Ready,
    Unavailable,
    Incompatible,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservationPhase {
    Idle,
    Connecting,
    Live,
    Retrying,
}

pub struct ConnectionState {
    pub epoch: u64,
    pub config: HostConfig,
    pub phase: ConnectionPhase,
    pub capabilities: Option<Capabilities>,
}

#[derive(Default)]
pub struct SessionView {
    pub transcript: Vec<Message>,
    pub turns: Vec<Turn>,
    pub queued: Vec<Message>,
    pub unassigned: Vec<Message>,
    pub active_effects: HashMap<EffectKey, LiveEffect>,
    // Effects that have ended in the event stream but whose durable transcript
    // has not arrived in a consistent snapshot yet. Keep their last rendered
    // value as the mutable tail until snapshot consolidation completes.
    pub settling_effects: HashMap<EffectKey, LiveEffect>,
    pub approvals: Vec<Approval>,
    pub cursor: Option<Cursor>,
    pub next_commit: u64,
    pub loaded: bool,
    pub snapshot_inflight: bool,
    pub error: Option<String>,
    pub dirty: bool,
    pub watch_needed: bool,
    pub revision: u64,
}

impl SessionView {
    pub fn active_turn(&self) -> Option<&Turn> {
        self.turns
            .iter()
            .find(|turn| turn.status == TurnStatus::Active)
    }
}

pub struct LiveEffect {
    pub descriptor: ActiveEffect,
    pub generation: Option<i64>,
    pub sequence: Option<u64>,
    pub text: String,
}

impl LiveEffect {
    pub fn new(descriptor: ActiveEffect) -> Self {
        Self {
            descriptor,
            generation: None,
            sequence: None,
            text: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionPhase {
    Queued,
    Submitting,
    Accepted,
    Applied,
    Rejected(String),
    Unknown(String),
}

impl SubmissionPhase {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Applied | Self::Rejected(_))
    }
}

pub struct PendingCommand {
    pub command: Command,
    pub epoch: u64,
    pub draft_revision: Option<u64>,
    pub phase: SubmissionPhase,
    pub reconciliation_inflight: bool,
}

impl PendingCommand {
    pub fn text(&self) -> Option<&str> {
        match &self.command.body {
            CommandBody::Submit { text, .. } | CommandBody::Steer { text, .. } => Some(text),
            CommandBody::Stop { .. } => Some("/stop"),
            CommandBody::Resume { .. } => Some("/resume"),
        }
    }
}

#[derive(Default)]
pub struct UiState {
    pub draft: String,
    pub draft_revision: u64,
    pub events: bool,
    pub host_editor: bool,
    pub notice: Option<String>,
}

pub struct AppState {
    pub connection: ConnectionState,
    pub observation: ObservationPhase,
    pub session: SessionView,
    pub ui: UiState,
    pub commands: HashMap<String, PendingCommand>,
    pub command_order: Vec<String>,
    pub debug: VecDeque<String>,
    pub quit: bool,
    pub revision: u64,
    pub instance: String,
    pub next_command: u64,
}

impl AppState {
    pub fn new(instance: String) -> Self {
        let connection = ConnectionState {
            epoch: 0,
            config: HostConfig::default(),
            phase: ConnectionPhase::Unconfigured,
            capabilities: None,
        };
        let ui = UiState {
            host_editor: true,
            ..Default::default()
        };
        Self {
            connection,
            observation: ObservationPhase::Idle,
            session: SessionView::default(),
            ui,
            commands: HashMap::new(),
            command_order: Vec::new(),
            debug: VecDeque::new(),
            quit: false,
            revision: 0,
            instance,
            next_command: 0,
        }
    }

    pub fn can_submit(&self) -> bool {
        let supported = self
            .connection
            .capabilities
            .as_ref()
            .is_some_and(|caps| caps.submit);
        self.connection.phase == ConnectionPhase::Ready && supported
    }

    pub fn log(&mut self, text: String) {
        self.debug.push_back(text);
        while self.debug.len() > 200 {
            self.debug.pop_front();
        }
    }
}
