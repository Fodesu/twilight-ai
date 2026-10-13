//! Host lifecycle is a bootstrap concern, outside ChatClient and ChatView.
//! External Local Host and Cloud Gateway are never owned/stopped by this client.
use anyhow::{bail, Context, Result};
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use uuid::Uuid;

pub struct LocalHost {
    child: Child,
    log_path: PathBuf,
}
impl LocalHost {
    pub fn start(binary: &Path, config: &Path, endpoint: &str) -> Result<Self> {
        let launch = crate::config::HostLaunch {
            binary: binary.into(),
            config: config.into(),
        };
        let host = crate::config::HostConfig {
            url: endpoint.into(),
            session: None,
            launch: Some(launch),
        };
        host.validate()?;
        let url = reqwest::Url::parse(endpoint).context("invalid managed Host URL")?;
        let address = format!(
            "{}:{}",
            url.host_str().unwrap(),
            url.port_or_known_default().unwrap()
        );
        if std::net::TcpStream::connect(&address).is_ok() {
            bail!("Local Host port is already occupied; connect without --local-host instead");
        }
        let log_path =
            std::env::temp_dir().join(format!("twilight-chat-host-{}.log", Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let log = options.open(&log_path)?;
        let child = Command::new(binary)
            .arg("-config")
            .arg(config)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .context("start Local Host")?;
        Ok(Self { child, log_path })
    }
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
    pub fn check(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!("Local Host exited ({status}); inspect its private log");
        }
        Ok(())
    }
}
impl Drop for LocalHost {
    fn drop(&mut self) {
        shutdown(&mut self.child);
    }
}

fn shutdown(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
}
