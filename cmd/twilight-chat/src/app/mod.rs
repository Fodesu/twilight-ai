pub mod commands;
pub mod driver;
pub mod effect;
pub mod event;
pub mod reducer;
pub mod state;
pub mod streaming;

use event::{AppEvent, UiAction};
use state::AppState;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Application {
    pub state: AppState,
    events: mpsc::Receiver<AppEvent>,
    sender: mpsc::Sender<AppEvent>,
    driver: driver::Driver,
}

impl Application {
    pub fn new(instance: String) -> Self {
        let (sender, events) = mpsc::channel(512);
        let driver = driver::Driver::new(sender.clone());
        Self {
            state: AppState::new(instance),
            events,
            sender,
            driver,
        }
    }

    pub fn sender(&self) -> mpsc::Sender<AppEvent> {
        self.sender.clone()
    }

    pub fn action(&mut self, action: UiAction) {
        self.apply(AppEvent::Ui(action));
    }

    pub fn apply(&mut self, event: AppEvent) {
        let effects = reducer::update(&mut self.state, event);
        self.driver.execute(effects);
    }

    pub async fn pump(&mut self, wait: Duration) {
        let event = tokio::time::timeout(wait, self.events.recv()).await;
        if let Ok(Some(event)) = event {
            self.apply(event);
        }
        drain(self);
    }

    pub fn tick(&mut self) {
        self.apply(AppEvent::Tick);
    }

    pub async fn close(self) {
        self.driver.close().await;
    }
}

fn drain(app: &mut Application) {
    for _ in 0..63 {
        if !drain_one(app) {
            break;
        }
    }
}

fn drain_one(app: &mut Application) -> bool {
    let Ok(event) = app.events.try_recv() else {
        return false;
    };
    app.apply(event);
    true
}
