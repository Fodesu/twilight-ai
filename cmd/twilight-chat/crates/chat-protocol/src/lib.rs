//! Versioned presentation protocol. Independent of terminal, transport and Core.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Cursor {
    pub commit: u64,
    pub index: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capabilities {
    pub version: u32,
    pub submit: bool,
    pub stop: bool,
    pub resume: bool,
    pub approvals: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub version: u32,
    pub session: String,
    pub next_commit: u64,
    pub transcript: Vec<Message>,
    pub turns: Vec<Turn>,
    #[serde(default)]
    pub queued: Vec<Message>,
    #[serde(default)]
    pub unassigned: Vec<Message>,
    pub active_effects: Vec<ActiveEffect>,
    #[serde(default)]
    pub approvals: Vec<Approval>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub kind: MessageKind,
    pub id: String,
    pub position: Cursor,
    pub text: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub turn_id: String,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub call_id: String,
    #[serde(default)]
    pub superseded_by: String,
    #[serde(default)]
    pub calls: Vec<ToolCall>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    #[default]
    Input,
    Assistant,
    ToolResult,
    Summary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub id: String,
    pub status: TurnStatus,
    #[serde(default)]
    pub reason: String,
    pub run_id: String,
    #[serde(default)]
    pub input_ids: Vec<String>,
    #[serde(default)]
    pub items: Vec<Message>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Active,
    Completed,
    Stopped,
    Failed,
    Superseded,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct EffectKey {
    pub run_id: String,
    pub effect_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveEffect {
    #[serde(flatten)]
    pub key: EffectKey,
    pub kind: EffectKind,
    pub label: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Model,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    pub id: String,
    pub run_id: String,
    pub call_id: String,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerEvent {
    Committed {
        session: String,
        cursor: Cursor,
        #[serde(default, rename = "effectsStarted")]
        effects_started: Vec<ActiveEffect>,
        #[serde(default, rename = "effectsEnded")]
        effects_ended: Vec<EffectKey>,
        #[serde(default, rename = "endedRuns")]
        ended_runs: Vec<String>,
    },
    Progress {
        session: String,
        #[serde(flatten)]
        key: EffectKey,
        generation: i64,
        sequence: u64,
        update: ProgressKind,
        #[serde(default)]
        text: String,
    },
}

impl ServerEvent {
    pub fn session(&self) -> &str {
        match self {
            Self::Committed { session, .. } | Self::Progress { session, .. } => session,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgressKind {
    TextDelta,
    ReasoningDelta,
    ToolProgress,
    Reset,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    #[serde(flatten)]
    pub body: CommandBody,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum CommandBody {
    Submit {
        input_id: String,
        text: String,
    },
    Steer {
        turn_id: String,
        input_id: String,
        text: String,
    },
    Stop {
        turn_id: String,
        reason: String,
    },
    Resume {
        turn_id: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandStatus {
    Accepted,
    Applied,
    Rejected(String),
    Unknown(String),
}

impl CommandStatus {
    pub fn terminal(&self) -> bool {
        matches!(self, Self::Applied | Self::Rejected(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn command_envelope_has_stable_identity_and_typed_payload() {
        let command = Command {
            id: "cmd".into(),
            body: CommandBody::Submit {
                input_id: "input".into(),
                text: "hello".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            json!({"id":"cmd","kind":"submit","payload":{"inputId":"input","text":"hello"}})
        );
    }

    #[test]
    fn normalized_progress_round_trip_preserves_identity_and_empty_reset() {
        let value = json!({"kind":"progress","session":"s","runId":"r","effectId":"e","generation":3,"sequence":0,"update":"reset","text":""});
        let event: ServerEvent = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(event).unwrap(), value);
    }

    #[test]
    fn raw_core_facts_and_unknown_presentation_kinds_are_not_guessed() {
        let raw = json!({"kind":"run_ended","value":{"RunID":"r","Fact":{"effect":"e"}}});
        assert!(serde_json::from_value::<ServerEvent>(raw).is_err());
        assert!(serde_json::from_value::<MessageKind>(json!("future_kind")).is_err());
    }

    #[test]
    fn cursors_compare_both_commit_and_index() {
        assert!(
            Cursor {
                commit: 3,
                index: 2
            } > Cursor {
                commit: 3,
                index: 1
            }
        );
        assert!(
            Cursor {
                commit: 4,
                index: 0
            } > Cursor {
                commit: 3,
                index: 99
            }
        );
    }
}
