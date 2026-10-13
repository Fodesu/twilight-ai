use crate::client::{ClientError, ClientResult, ErrorKind};
use futures_util::StreamExt;
use reqwest::{Response, StatusCode, Url};
use serde::de::DeserializeOwned;

pub(crate) const JSON_LIMIT: usize = 16 * 1024 * 1024;

pub(crate) fn append_path(base: &Url, parts: &[&str]) -> ClientResult<Url> {
    let mut url = base.clone();
    let mut path = url
        .path_segments_mut()
        .map_err(|_| ClientError::new(ErrorKind::Protocol, "invalid Host URL path"))?;
    path.pop_if_empty();
    for part in parts {
        // Url treats literal . and .. as navigation and drops them. Reject these
        // exceptional identifiers rather than silently targeting another session.
        if matches!(*part, "." | "..") {
            return Err(invalid_identifier());
        }
        path.push(part);
    }
    drop(path);
    Ok(url)
}

pub(crate) fn transport_error() -> ClientError {
    // reqwest errors include request URLs; never expose those or response bodies.
    ClientError::new(
        ErrorKind::Transport,
        "Host request failed (transport or timeout)",
    )
}

pub(crate) fn http_error(status: StatusCode) -> ClientError {
    let kind = match status.as_u16() {
        401 | 403 => ErrorKind::Unauthorized,
        404 => ErrorKind::NotFound,
        426 => ErrorKind::Incompatible,
        400..=499 => ErrorKind::Rejected,
        500..=599 => ErrorKind::Transport,
        _ => ErrorKind::Protocol,
    };
    let reason = status.canonical_reason().unwrap_or("unexpected status");
    ClientError::new(
        kind,
        format!(
            "Host returned HTTP {} {reason} (body suppressed)",
            status.as_u16()
        ),
    )
}

pub(crate) fn check_status(status: StatusCode) -> ClientResult<()> {
    if status.is_success() {
        Ok(())
    } else {
        Err(http_error(status))
    }
}

pub(crate) fn check_version(version: u32) -> ClientResult<()> {
    if version != chat_protocol::VERSION {
        return Err(ClientError::new(
            ErrorKind::Incompatible,
            "Host requires chat protocol v2",
        ));
    }
    Ok(())
}

pub(crate) fn check_session(actual: &str, expected: &str) -> ClientResult<()> {
    if actual != expected {
        return Err(ClientError::new(
            ErrorKind::Protocol,
            "Host returned a different session",
        ));
    }
    Ok(())
}

pub(crate) async fn read_json<T: DeserializeOwned>(response: Response) -> ClientResult<T> {
    if response
        .content_length()
        .is_some_and(|length| length > JSON_LIMIT as u64)
    {
        return Err(oversized_json());
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| transport_error())?;
        if chunk.len() > JSON_LIMIT - bytes.len() {
            return Err(oversized_json());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(json_error)
}

fn invalid_identifier() -> ClientError {
    ClientError::new(ErrorKind::Protocol, "invalid path identifier")
}

fn json_error(error: serde_json::Error) -> ClientError {
    let message = format!(
        "invalid Host JSON at line {}, column {}",
        error.line(),
        error.column()
    );
    ClientError::new(ErrorKind::Protocol, message)
}

fn oversized_json() -> ClientError {
    ClientError::new(ErrorKind::Protocol, "Host JSON response exceeds 16 MiB")
}
