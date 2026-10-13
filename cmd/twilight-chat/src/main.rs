use anyhow::{Context, Result};
use chat_client::config::{self, HostConfig, HostLaunch};
use clap::Parser;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use twilight_chat::{
    app::{event::AppEvent, Application},
    headless::{self, Script},
    terminal::TerminalUi,
};

#[derive(Clone, Parser)]
#[command(about = "Twilight chat terminal")]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    session: Option<String>,
    #[arg(long, alias = "local-host")]
    host_binary: Option<PathBuf>,
    #[arg(long, alias = "local-config")]
    host_config: Option<PathBuf>,
    #[arg(long)]
    headless: bool,
    #[arg(long, requires = "headless")]
    send: Vec<String>,
    #[arg(long, requires = "headless", conflicts_with = "stop")]
    resume: bool,
    #[arg(long, requires = "headless")]
    stop: bool,
    #[arg(long, default_value_t = 90, value_parser = clap::value_parser!(u64).range(1..))]
    wait_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut terminal = if args.headless {
        None
    } else {
        Some(TerminalUi::new()?)
    };
    let mut app = Application::new(uuid::Uuid::new_v4().to_string());
    if let Some(terminal) = &mut terminal {
        terminal.draw(&app.state)?;
    }
    start_settings(&app, args.clone());
    let script = Script {
        send: args.send,
        stop: args.stop,
        resume: args.resume,
        seconds: args.wait_seconds,
    };
    let result = match terminal.as_mut() {
        Some(terminal) => run_terminal(&mut app, terminal).await,
        None => run_script(&mut app, script).await,
    };
    // Dropping terminal ownership restores TTY before cleanup diagnostics.
    drop(terminal);
    app.close().await;
    result
}

async fn run_script(app: &mut Application, script: Script) -> Result<()> {
    tokio::select! {
        result = headless::run(app, script) => result,
        _ = tokio::signal::ctrl_c() => anyhow::bail!("Interrupted; no Stop command issued"),
    }
}

fn start_settings(app: &Application, args: Args) {
    let events = app.sender();
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || settings(args)).await;
        let event = settings_event(result);
        let _ = events.send(event).await;
    });
}

fn settings_event(result: Result<Result<Option<HostConfig>>, tokio::task::JoinError>) -> AppEvent {
    match result {
        Ok(Ok(config)) => AppEvent::SettingsLoaded {
            config,
            error: None,
        },
        Ok(Err(error)) => AppEvent::SettingsLoaded {
            config: None,
            error: Some(error.to_string()),
        },
        Err(_) => AppEvent::SettingsLoaded {
            config: None,
            error: Some("Configuration loader failed".into()),
        },
    }
}

fn settings(args: Args) -> Result<Option<HostConfig>> {
    let loaded = config::load(args.config.as_deref())?;
    let explicit = args.url.is_some()
        || args.session.is_some()
        || args.host_binary.is_some()
        || args.host_config.is_some();
    let configured = explicit || loaded.path.is_some() || args.headless;
    let mut host = loaded.host;
    if let Some(url) = args.url {
        host.url = url;
    }
    if let Some(session) = args.session {
        host.session = Some(session);
    }
    match (args.host_binary, args.host_config) {
        (None, None) => (),
        (Some(binary), Some(config)) => host.launch = Some(HostLaunch { binary, config }),
        _ => anyhow::bail!("Host launch requires both --host-binary and --host-config"),
    }
    host.validate()?;
    Ok(configured.then_some(host))
}

async fn run_terminal(app: &mut Application, terminal: &mut TerminalUi) -> Result<()> {
    let mut frame = Instant::now();
    let mut application_tick = Instant::now();
    while !app.state.quit {
        app.pump(Duration::from_millis(10)).await;
        if frame.elapsed() < Duration::from_millis(16) {
            continue;
        }
        frame = Instant::now();
        for action in terminal.poll(&app.state).context("Terminal input")? {
            app.action(action);
        }
        if application_tick.elapsed() >= Duration::from_millis(300) {
            app.tick();
            application_tick = Instant::now();
        }
        // Bounded 60 Hz frames keep local scrolling responsive while batching input.
        terminal.draw(&app.state).context("Terminal draw")?;
    }
    Ok(())
}
