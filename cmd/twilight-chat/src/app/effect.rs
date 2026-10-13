use super::event::AppEvent;
use chat_client::config::HostConfig;
use chat_protocol::Command;

#[derive(Clone, Copy)]
pub enum Control {
    Stop,
    Resume,
}

pub enum Effect {
    Connect {
        epoch: u64,
        config: HostConfig,
    },
    Disconnect,
    Snapshot {
        epoch: u64,
    },
    Watch {
        epoch: u64,
        from: u64,
    },
    Post {
        epoch: u64,
        command: Command,
    },
    Control {
        epoch: u64,
        id: String,
        intent: Control,
    },
    Reconcile {
        epoch: u64,
        id: String,
    },
    After {
        milliseconds: u64,
        event: Box<AppEvent>,
    },
}
