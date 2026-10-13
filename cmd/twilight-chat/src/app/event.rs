use chat_client::{config::HostConfig, ClientError};
use chat_protocol::{Capabilities, Command, CommandStatus, ServerEvent, Snapshot};

#[derive(Clone, Debug)]
pub enum UiAction {
    Edit(String),
    Send,
    Stop,
    Resume,
    Refresh,
    Configure(HostConfig),
    OpenHost,
    CloseHost,
    Disconnect,
    ToggleEvents,
    Quit,
}

pub enum AppEvent {
    Ui(UiAction),
    Connected {
        epoch: u64,
        result: Result<Capabilities, ClientError>,
    },
    Snapshot {
        epoch: u64,
        result: Result<Snapshot, ClientError>,
    },
    StreamOpened {
        epoch: u64,
    },
    Server {
        epoch: u64,
        event: ServerEvent,
    },
    StreamFailed {
        epoch: u64,
        error: ClientError,
    },
    CommandStarted {
        command: Command,
    },
    CommandResult {
        id: String,
        status: CommandStatus,
    },
    RetryConnection {
        epoch: u64,
    },
    RetryStream {
        epoch: u64,
    },
    SettingsLoaded {
        config: Option<HostConfig>,
        error: Option<String>,
    },
    Tick,
}
