//! I/O executor: tasks return events; application state stays in the reducer.
use super::effect::{Control, Effect};
use super::event::AppEvent;
use chat_client::{
    config::HostConfig, host::LocalHost, ChatClient, ClientError, ErrorKind, HttpChatClient,
};
use chat_protocol::{Command, CommandBody, CommandStatus, TurnStatus};
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

type Clients = Arc<Mutex<HashMap<u64, Arc<dyn ChatClient>>>>;
struct Bootstrap {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}
struct PostWork {
    client: Arc<dyn ChatClient>,
    command: Command,
    cancel: CancellationToken,
}

pub struct Driver {
    events: mpsc::Sender<AppEvent>,
    clients: Clients,
    scopes: HashMap<u64, CancellationToken>,
    bootstrap: Option<Bootstrap>,
    watch: Option<JoinHandle<()>>,
    tasks: JoinSet<()>,
    posts: mpsc::Sender<PostWork>,
    poster: JoinHandle<()>,
    cancel: CancellationToken,
}

impl Driver {
    pub fn new(events: mpsc::Sender<AppEvent>) -> Self {
        let (posts, receiver) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let poster = tokio::spawn(post_queue(receiver, events.clone(), cancel.clone()));
        Self {
            events,
            clients: Arc::new(Mutex::new(HashMap::new())),
            scopes: HashMap::new(),
            bootstrap: None,
            watch: None,
            tasks: JoinSet::new(),
            posts,
            poster,
            cancel,
        }
    }

    pub fn execute(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            execute_one(self, effect);
        }
        while self.tasks.try_join_next().is_some() {}
    }

    fn snapshot(&mut self, epoch: u64) {
        self.tasks.spawn(read_snapshot(
            self.clients.clone(),
            self.events.clone(),
            epoch,
        ));
    }

    fn control(&mut self, epoch: u64, id: String, intent: Control) {
        self.tasks.spawn(control(
            self.clients.clone(),
            self.events.clone(),
            epoch,
            id,
            intent,
            self.scope(epoch),
        ));
    }

    fn reconcile(&mut self, epoch: u64, id: String) {
        self.tasks.spawn(reconcile(
            self.clients.clone(),
            self.events.clone(),
            epoch,
            id,
        ));
    }

    fn delay(&mut self, milliseconds: u64, event: AppEvent) {
        self.tasks
            .spawn(after(self.events.clone(), milliseconds, event));
    }

    fn connect(&mut self, epoch: u64, config: HostConfig) {
        self.stop_watch();
        let old = self.bootstrap.take();
        if let Some(old) = &old {
            old.cancel.cancel();
        }
        let cancel = CancellationToken::new();
        self.scopes.insert(epoch, cancel.clone());
        let future = bootstrap(
            old,
            self.clients.clone(),
            self.events.clone(),
            epoch,
            config,
            cancel.clone(),
        );
        self.bootstrap = Some(Bootstrap {
            cancel,
            task: tokio::spawn(future),
        });
    }

    fn watch(&mut self, epoch: u64, from: u64) {
        self.stop_watch();
        self.watch = Some(tokio::spawn(watch(
            self.clients.clone(),
            self.events.clone(),
            epoch,
            from,
        )));
    }

    fn post(&self, epoch: u64, command: Command) {
        let client = client(&self.clients, epoch);
        match client {
            Ok(client) => self.post_ready(epoch, client, command),
            Err(error) => self.reject(command.id, error.message),
        }
    }

    fn post_ready(&self, epoch: u64, client: Arc<dyn ChatClient>, command: Command) {
        let work = PostWork {
            client,
            command,
            cancel: self.scope(epoch),
        };
        self.enqueue_post(work);
    }

    fn scope(&self, epoch: u64) -> CancellationToken {
        self.scopes
            .get(&epoch)
            .cloned()
            .unwrap_or_else(|| self.cancel.clone())
    }

    fn enqueue_post(&self, work: PostWork) {
        if let Err(error) = self.posts.try_send(work) {
            let id = error.into_inner().command.id;
            self.reject(id, "Submission queue full; draft retained".into());
        }
    }

    fn reject(&self, id: String, reason: String) {
        let event = AppEvent::CommandResult {
            id,
            status: CommandStatus::Rejected(reason),
        };
        let sender = self.events.clone();
        tokio::spawn(async move {
            let _ = sender.send(event).await;
        });
    }

    fn stop_watch(&mut self) {
        if let Some(watch) = self.watch.take() {
            watch.abort();
        }
    }

    fn disconnect(&mut self) {
        self.stop_watch();
        for scope in self.scopes.values() {
            scope.cancel();
        }
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.cancel.cancel();
        }
    }

    pub async fn close(mut self) {
        self.disconnect();
        self.cancel.cancel();
        self.tasks.abort_all();
        let _ = self.poster.await;
        if let Some(bootstrap) = self.bootstrap.take() {
            let _ = bootstrap.task.await;
        }
    }
}

fn execute_one(driver: &mut Driver, effect: Effect) {
    match effect {
        Effect::Connect { epoch, config } => driver.connect(epoch, config),
        Effect::Disconnect => driver.disconnect(),
        Effect::Snapshot { epoch } => driver.snapshot(epoch),
        Effect::Watch { epoch, from } => driver.watch(epoch, from),
        Effect::Post { epoch, command } => driver.post(epoch, command),
        Effect::Control { epoch, id, intent } => driver.control(epoch, id, intent),
        Effect::Reconcile { epoch, id } => driver.reconcile(epoch, id),
        Effect::After {
            milliseconds,
            event,
        } => driver.delay(milliseconds, *event),
    }
}

fn client(clients: &Clients, epoch: u64) -> Result<Arc<dyn ChatClient>, ClientError> {
    clients
        .lock()
        .unwrap()
        .get(&epoch)
        .cloned()
        .ok_or_else(|| local_error("Host client unavailable"))
}

fn local_error(message: impl Into<String>) -> ClientError {
    ClientError {
        kind: ErrorKind::Transport,
        message: message.into(),
    }
}

async fn bootstrap(
    old: Option<Bootstrap>,
    clients: Clients,
    events: mpsc::Sender<AppEvent>,
    epoch: u64,
    config: HostConfig,
    cancel: CancellationToken,
) {
    if let Some(old) = old {
        let _ = old.task.await;
    }
    let mut host = match start_host(&config).await {
        Ok(host) => host,
        Err(error) => {
            report_connected(&events, epoch, Err(error)).await;
            return;
        }
    };
    let result = establish(&clients, epoch, &config, &mut host);
    tokio::select! {
        _ = cancel.cancelled() => (),
        result = result => {
            report_connected(&events, epoch, result).await;
            cancel.cancelled().await;
        }
    }
}

async fn start_host(config: &HostConfig) -> Result<Option<LocalHost>, ClientError> {
    let Some(launch) = config.launch.clone() else {
        return Ok(None);
    };
    let endpoint = config.url.clone();
    let result = tokio::task::spawn_blocking(move || {
        LocalHost::start(&launch.binary, &launch.config, &endpoint)
    })
    .await;
    result
        .map_err(|_| local_error("Host launcher task failed"))?
        .map(Some)
        .map_err(|error| ClientError {
            kind: ErrorKind::Protocol,
            message: format!("Host launch: {error}"),
        })
}

async fn establish(
    clients: &Clients,
    epoch: u64,
    config: &HostConfig,
    host: &mut Option<LocalHost>,
) -> Result<chat_protocol::Capabilities, ClientError> {
    let session = config.session.clone().unwrap_or_default();
    let connection: Arc<dyn ChatClient> = Arc::new(HttpChatClient::new(&config.url, session)?);
    clients.lock().unwrap().insert(epoch, connection.clone());
    let capabilities = capabilities(connection.as_ref(), host).await?;
    connection.ensure().await?;
    Ok(capabilities)
}

async fn capabilities(
    client: &dyn ChatClient,
    host: &mut Option<LocalHost>,
) -> Result<chat_protocol::Capabilities, ClientError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        check_host(host)?;
        let result = client.capabilities().await;
        if !retry_startup(&result, host.is_some(), deadline) {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn check_host(host: &mut Option<LocalHost>) -> Result<(), ClientError> {
    if let Some(host) = host.as_mut() {
        host.check()
            .map_err(|error| local_error(error.to_string()))?;
    }
    Ok(())
}

fn control_rejected(error: ClientError) -> CommandStatus {
    CommandStatus::Rejected(format!(
        "Cannot inspect current Turn: {}; no command submitted",
        error.message
    ))
}

fn retry_startup(
    result: &Result<chat_protocol::Capabilities, ClientError>,
    managed: bool,
    deadline: tokio::time::Instant,
) -> bool {
    managed
        && tokio::time::Instant::now() < deadline
        && matches!(result, Err(error) if matches!(error.kind, ErrorKind::Transport))
}

async fn report_connected(
    events: &mpsc::Sender<AppEvent>,
    epoch: u64,
    result: Result<chat_protocol::Capabilities, ClientError>,
) {
    let _ = events.send(AppEvent::Connected { epoch, result }).await;
}

async fn read_snapshot(clients: Clients, events: mpsc::Sender<AppEvent>, epoch: u64) {
    let result = match client(&clients, epoch) {
        Ok(client) => client.snapshot().await,
        Err(error) => Err(error),
    };
    let _ = events.send(AppEvent::Snapshot { epoch, result }).await;
}

async fn watch(clients: Clients, events: mpsc::Sender<AppEvent>, epoch: u64, from: u64) {
    let result = follow(clients, &events, epoch, from).await;
    if let Err(error) = result {
        let _ = events.send(AppEvent::StreamFailed { epoch, error }).await;
    }
}

async fn follow(
    clients: Clients,
    events: &mpsc::Sender<AppEvent>,
    epoch: u64,
    from: u64,
) -> Result<(), ClientError> {
    let client = client(&clients, epoch)?;
    let mut stream = client.subscribe(from).await?;
    let _ = events.send(AppEvent::StreamOpened { epoch }).await;
    while let Some(event) = stream.next().await {
        let message = AppEvent::Server {
            epoch,
            event: event?,
        };
        if events.send(message).await.is_err() {
            return Ok(());
        }
    }
    Err(local_error(
        "Event stream closed; commands remain available",
    ))
}

async fn post_queue(
    mut work: mpsc::Receiver<PostWork>,
    events: mpsc::Sender<AppEvent>,
    cancel: CancellationToken,
) {
    loop {
        let item = tokio::select! {
            _ = cancel.cancelled() => break,
            item = work.recv() => item,
        };
        if item.is_none() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = process_post(item, &events) => (),
        }
    }
}

async fn process_post(work: Option<PostWork>, events: &mpsc::Sender<AppEvent>) {
    let Some(work) = work else {
        return;
    };
    let result = transmit(work.client, &work.command, work.cancel, events).await;
    report_command(events, work.command.id, result).await;
}

async fn control(
    clients: Clients,
    events: mpsc::Sender<AppEvent>,
    epoch: u64,
    id: String,
    intent: Control,
    cancel: CancellationToken,
) {
    let result = control_command(clients, epoch, &id, intent, cancel, &events).await;
    report_command(&events, id, result).await;
}

async fn control_command(
    clients: Clients,
    epoch: u64,
    id: &str,
    intent: Control,
    cancel: CancellationToken,
    events: &mpsc::Sender<AppEvent>,
) -> Result<CommandStatus, ClientError> {
    let client = client(&clients, epoch)?;
    let answer = tokio::select! {
        _ = cancel.cancelled() => return Ok(cancelled_before_post()),
        result = client.snapshot() => result,
    };
    let snapshot = match answer {
        Ok(snapshot) => snapshot,
        Err(error) => return Ok(control_rejected(error)),
    };
    let active = snapshot
        .turns
        .iter()
        .find(|turn| turn.status == TurnStatus::Active)
        .map(|turn| turn.id.clone());
    let body = match intent {
        Control::Stop => stop_body(active),
        Control::Resume => Some(CommandBody::Resume { turn_id: active }),
    };
    let Some(body) = body else {
        return Ok(CommandStatus::Rejected("No active Turn".into()));
    };
    let command = Command {
        id: id.into(),
        body,
    };
    transmit(client, &command, cancel, events).await
}

fn cancelled_before_post() -> CommandStatus {
    CommandStatus::Rejected("Connection changed before transmission; no command submitted".into())
}

async fn transmit(
    client: Arc<dyn ChatClient>,
    command: &Command,
    cancel: CancellationToken,
    events: &mpsc::Sender<AppEvent>,
) -> Result<CommandStatus, ClientError> {
    if cancel.is_cancelled() {
        return Ok(cancelled_before_post());
    }
    let _ = events
        .send(AppEvent::CommandStarted {
            command: command.clone(),
        })
        .await;
    tokio::select! {
        _ = cancel.cancelled() => Ok(CommandStatus::Unknown("Connection changed during submission; read-only reconciliation".into())),
        result = client.post(command) => result,
    }
}

fn stop_body(active: Option<String>) -> Option<CommandBody> {
    active.map(|turn_id| CommandBody::Stop {
        turn_id,
        reason: "chat user stop".into(),
    })
}

async fn reconcile(clients: Clients, events: mpsc::Sender<AppEvent>, epoch: u64, id: String) {
    let result = match client(&clients, epoch) {
        Ok(client) => client.reconcile(&id).await,
        Err(error) => Err(error),
    };
    report_command(&events, id, result).await;
}

async fn report_command(
    events: &mpsc::Sender<AppEvent>,
    id: String,
    result: Result<CommandStatus, ClientError>,
) {
    let status = result.unwrap_or_else(|error| CommandStatus::Unknown(error.message));
    let _ = events.send(AppEvent::CommandResult { id, status }).await;
}

async fn after(events: mpsc::Sender<AppEvent>, milliseconds: u64, event: AppEvent) {
    tokio::time::sleep(Duration::from_millis(milliseconds)).await;
    let _ = events.send(event).await;
}
