//! Typed, read-only reconciliation port shared by local and remote Hosts.
use crate::{config::HostConfig, sse, transport};
use async_trait::async_trait;
pub use chat_protocol::{Capabilities, Command, CommandStatus, ServerEvent, Snapshot};
use futures_util::Stream;
use reqwest::{Method, Response, Url};
use serde::Deserialize;
use std::{fmt, pin::Pin, time::Duration};

pub type ClientResult<T> = Result<T, ClientError>;
pub type Events = Pin<Box<dyn Stream<Item = ClientResult<ServerEvent>> + Send>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Transport,
    Unauthorized,
    Incompatible,
    Rejected,
    NotFound,
    Protocol,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientError {
    pub kind: ErrorKind,
    pub message: String,
}

impl ClientError {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ClientError {}

#[async_trait]
pub trait ChatClient: Send + Sync {
    fn session(&self) -> &str;
    async fn capabilities(&self) -> ClientResult<Capabilities>;
    async fn ensure(&self) -> ClientResult<()>;
    async fn snapshot(&self) -> ClientResult<Snapshot>;
    async fn subscribe(&self, from: u64) -> ClientResult<Events>;
    async fn post(&self, command: &Command) -> ClientResult<CommandStatus>;
    async fn reconcile(&self, id: &str) -> ClientResult<CommandStatus>;
}

#[derive(Clone)]
pub struct HttpChatClient {
    http: reqwest::Client,
    base: Url,
    session: String,
}

impl HttpChatClient {
    pub fn new(url: &str, session: String) -> ClientResult<Self> {
        let config = HostConfig {
            url: url.into(),
            session: Some(session.clone()),
            launch: None,
        };
        config
            .validate()
            .map_err(|error| ClientError::new(ErrorKind::Protocol, error.to_string()))?;
        let base = Url::parse(url)
            .map_err(|_| ClientError::new(ErrorKind::Protocol, "invalid Host URL"))?;
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| transport::transport_error())?;
        Ok(Self {
            http,
            base,
            session,
        })
    }

    fn base_url(&self, parts: &[&str]) -> ClientResult<Url> {
        transport::append_path(&self.base, parts)
    }

    fn session_url(&self, parts: &[&str]) -> ClientResult<Url> {
        let mut path = vec!["sessions", self.session.as_str()];
        path.extend_from_slice(parts);
        self.base_url(&path)
    }

    async fn request(&self, method: Method, url: Url) -> ClientResult<Response> {
        self.http
            .request(method, url)
            .timeout(Duration::from_secs(12))
            .send()
            .await
            .map_err(|_| transport::transport_error())
    }
}

#[async_trait]
impl ChatClient for HttpChatClient {
    fn session(&self) -> &str {
        &self.session
    }

    async fn capabilities(&self) -> ClientResult<Capabilities> {
        let url = self.base_url(&["chat", "capabilities"])?;
        let response = self.request(Method::GET, url).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(capability_endpoint_missing());
        }
        transport::check_status(response.status())?;
        let capabilities: Capabilities = transport::read_json(response).await?;
        transport::check_version(capabilities.version)?;
        Ok(capabilities)
    }

    async fn ensure(&self) -> ClientResult<()> {
        let response = self.request(Method::PUT, self.session_url(&[])?).await?;
        transport::check_status(response.status())
    }

    async fn snapshot(&self) -> ClientResult<Snapshot> {
        let url = self.session_url(&["chat", "snapshot"])?;
        let response = self.request(Method::GET, url).await?;
        transport::check_status(response.status())?;
        let snapshot: Snapshot = transport::read_json(response).await?;
        transport::check_version(snapshot.version)?;
        transport::check_session(&snapshot.session, &self.session)?;
        Ok(snapshot)
    }

    async fn subscribe(&self, from: u64) -> ClientResult<Events> {
        let mut url = self.session_url(&["chat", "events"])?;
        url.query_pairs_mut().append_pair("from", &from.to_string());
        // Only establishment has a deadline. No request/global timeout can expire
        // the live body, including arbitrarily long idle periods.
        let request = self.http.get(url).header("Accept", "text/event-stream");
        let response = tokio::time::timeout(Duration::from_secs(5), request.send())
            .await
            .map_err(|_| ClientError::new(ErrorKind::Transport, "SSE headers timed out"))?
            .map_err(|_| transport::transport_error())?;
        transport::check_status(response.status())?;
        sse::check_content_type(&response)?;
        Ok(sse::events(response, self.session.clone()))
    }

    async fn post(&self, command: &Command) -> ClientResult<CommandStatus> {
        let url = self.session_url(&["commands"])?;
        let answer = self
            .http
            .post(url)
            .json(command)
            .timeout(Duration::from_secs(12))
            .send()
            .await;
        let response = match answer {
            Ok(response) => response,
            Err(_) => return Ok(unknown_submission()),
        };
        let status = response.status();
        if status.is_server_error() {
            return Ok(unknown_submission());
        }
        if status.is_client_error() {
            let reason = transport::http_error(status).message;
            return Ok(CommandStatus::Rejected(reason));
        }
        transport::check_status(status)?;
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(unknown_submission());
        }
        let entry = transport::read_json::<CommandEntry>(response).await;
        Ok(entry
            .and_then(|entry| entry.resolution(&command.id))
            .unwrap_or_else(|_| unknown_submission()))
    }

    async fn reconcile(&self, id: &str) -> ClientResult<CommandStatus> {
        let mut url = self.session_url(&["commands", id])?;
        url.query_pairs_mut().append_pair("wait", "1s");
        let response = self.request(Method::GET, url).await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let reason = "command not found; not resubmitted".into();
            return Ok(CommandStatus::Unknown(reason));
        }
        transport::check_status(response.status())?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(receipt_missing());
        }
        transport::read_json::<CommandEntry>(response)
            .await?
            .resolution(id)
    }
}

fn receipt_missing() -> CommandStatus {
    CommandStatus::Unknown("command receipt absent; not resubmitted".into())
}

fn mismatched_receipt() -> ClientError {
    ClientError::new(
        ErrorKind::Protocol,
        "Host returned a different command receipt",
    )
}

fn rejected(reason: String) -> CommandStatus {
    CommandStatus::Rejected(reason.chars().take(2000).collect())
}

fn capability_endpoint_missing() -> ClientError {
    ClientError::new(
        ErrorKind::Incompatible,
        "Host requires chat protocol v2; capability endpoint missing",
    )
}

fn invalid_command_status() -> ClientError {
    ClientError::new(ErrorKind::Protocol, "invalid command result status")
}

fn unknown_submission() -> CommandStatus {
    CommandStatus::Unknown("submission outcome unknown; not resubmitted".into())
}

#[derive(Deserialize)]
struct CommandEntry {
    command: ReceiptIdentity,
    #[serde(default)]
    result: Option<CommandResult>,
}

#[derive(Deserialize)]
struct ReceiptIdentity {
    id: String,
}

#[derive(Deserialize)]
struct CommandResult {
    status: String,
    #[serde(default)]
    reason: String,
}

impl CommandEntry {
    fn resolution(self, expected_id: &str) -> ClientResult<CommandStatus> {
        if self.command.id != expected_id {
            return Err(mismatched_receipt());
        }
        let Some(result) = self.result else {
            return Ok(CommandStatus::Accepted);
        };
        match result.status.as_str() {
            "applied" => Ok(CommandStatus::Applied),
            "rejected" => Ok(rejected(result.reason)),
            "pending" | "accepted" => Ok(CommandStatus::Accepted),
            _ => Err(invalid_command_status()),
        }
    }
}
