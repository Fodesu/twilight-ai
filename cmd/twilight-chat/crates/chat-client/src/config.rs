//! Offline Host configuration loading and validation.
use anyhow::{bail, Context, Result};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

const MAX_CONFIG_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostConfig {
    pub url: String,
    pub session: Option<String>,
    pub launch: Option<HostLaunch>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostLaunch {
    pub binary: PathBuf,
    pub config: PathBuf,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8080".into(),
            session: None,
            launch: None,
        }
    }
}

impl HostConfig {
    /// Validate without checking files, contacting a Host, or starting a process.
    pub fn validate(&self) -> Result<()> {
        let url = validate_url(&self.url)?;
        if self.session.as_ref().is_some_and(|id| id.trim().is_empty()) {
            bail!("Session ID must not be empty or whitespace-only");
        }
        if let Some(launch) = &self.launch {
            validate_launch_paths(launch)?;
            validate_managed_endpoint(&url, &self.url)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedConfig {
    pub host: HostConfig,
    pub path: Option<PathBuf>,
}

/// Load an explicit required file, or discover the first available configuration.
/// Loaded file paths and relative managed-host paths are made absolute, without
/// canonicalizing them or requiring the managed files to exist.
pub fn load(explicit: Option<&Path>) -> Result<LoadedConfig> {
    if let Some(path) = explicit {
        return load_file(path);
    }
    let cwd = env::current_dir().context("locate current configuration directory")?;
    let xdg = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
    let home = env::var_os("HOME").map(PathBuf::from);
    load_discovered(discover(&cwd, xdg.as_deref(), home.as_deref())?)
}

fn load_discovered(path: Option<PathBuf>) -> Result<LoadedConfig> {
    match path {
        Some(path) => load_file(&path),
        None => Ok(LoadedConfig {
            host: HostConfig::default(),
            path: None,
        }),
    }
}

fn discover(cwd: &Path, xdg: Option<&Path>, home: Option<&Path>) -> Result<Option<PathBuf>> {
    let local = cwd.join("twilight-chat.json");
    if config_exists(&local)? {
        return Ok(Some(local));
    }
    let root = xdg
        .map(Path::to_path_buf)
        .or_else(|| home.map(|p| p.join(".config")));
    let Some(root) = root else {
        return Ok(None);
    };
    let user = root.join("twilight/chat.json");
    Ok(config_exists(&user)?.then_some(user))
}

fn config_exists(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("inspect chat configuration file"),
    }
}

fn load_file(path: &Path) -> Result<LoadedConfig> {
    let path = absolute_path(path)?;
    let bytes = read_config(&path)?;
    let mut host = parse_config(&bytes)?;
    let directory = path
        .parent()
        .context("configuration file has no directory")?;
    // Validate first: resolving an empty launch path would hide it as a directory.
    host.validate()?;
    if let Some(launch) = &mut host.launch {
        resolve_relative(&mut launch.binary, directory);
        resolve_relative(&mut launch.config, directory);
    }
    Ok(LoadedConfig {
        host,
        path: Some(path),
    })
}

fn parse_config(bytes: &[u8]) -> Result<HostConfig> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(json_error)?;
    if !value.is_object() {
        bail!("chat configuration must be a JSON object");
    }
    let launch = value.get("launch").filter(|launch| !launch.is_null());
    if launch.is_some_and(|launch| !launch.is_object()) {
        bail!("Host launch configuration must be a JSON object");
    }
    serde_json::from_value(value).map_err(json_error)
}

fn json_error(error: serde_json::Error) -> anyhow::Error {
    // Keep user-provided values out of diagnostic messages.
    anyhow::anyhow!(
        "invalid chat configuration JSON at line {}, column {}",
        error.line(),
        error.column()
    )
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = env::current_dir().context("resolve configuration file directory")?;
    Ok(cwd.join(path))
}

fn read_config(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).context("open chat configuration file")?;
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("read chat configuration file")?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        bail!("chat configuration file exceeds 64 KiB");
    }
    Ok(bytes)
}

fn resolve_relative(path: &mut PathBuf, directory: &Path) {
    if path.is_relative() {
        *path = directory.join(&*path);
    }
}

fn validate_url(endpoint: &str) -> Result<Url> {
    let url = Url::parse(endpoint).map_err(|_| anyhow::anyhow!("invalid Host URL"))?;
    let authority = endpoint_authority(endpoint);
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("Host URL must be an absolute HTTP(S) URL");
    }
    if endpoint.chars().any(char::is_whitespace) || endpoint.contains('\\') {
        bail!("Host URL must not contain whitespace or backslashes");
    }
    if authority.is_none() {
        bail!("Host URL must include an authority after ://");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("Host URL must not contain credentials");
    }
    if authority.is_some_and(|value| value.contains('@')) {
        bail!("Host URL must not contain credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("Host URL must not contain a query or fragment");
    }
    Ok(url)
}

fn endpoint_authority(endpoint: &str) -> Option<&str> {
    let (_, rest) = endpoint.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    (!authority.is_empty()).then_some(authority)
}

fn validate_launch_paths(launch: &HostLaunch) -> Result<()> {
    if launch.binary.as_os_str().is_empty() || launch.config.as_os_str().is_empty() {
        bail!("Host launch binary and config paths must not be empty");
    }
    Ok(())
}

fn validate_managed_endpoint(url: &Url, endpoint: &str) -> Result<()> {
    if url.scheme() != "http"
        || !matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
    {
        bail!("launched Host requires a loopback HTTP URL");
    }
    if url.path() != "/" {
        bail!("launched Host URL must use the root path");
    }
    // Url removes explicit default ports (:80), so inspect the original authority.
    let port = endpoint_authority(endpoint).and_then(|a| a.rsplit_once(':'));
    let explicit =
        port.is_some_and(|(_, p)| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    if !explicit {
        bail!("launched Host requires an explicit port");
    }
    Ok(())
}
