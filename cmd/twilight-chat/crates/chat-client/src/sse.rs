use crate::client::{ClientError, ClientResult, ErrorKind, Events, ServerEvent};
use crate::transport;
use futures_util::{stream, Stream, StreamExt};
use reqwest::Response;
use std::{io, pin::Pin};
use tokio_util::{
    codec::{FramedRead, LinesCodec, LinesCodecError},
    io::StreamReader,
};

pub(crate) const FRAME_LIMIT: usize = 4 * 1024 * 1024;
type Lines = Pin<Box<dyn Stream<Item = Result<String, LinesCodecError>> + Send>>;

pub(crate) fn check_content_type(response: &Response) -> ClientResult<()> {
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let media_type = content_type.split(';').next().unwrap_or("").trim();
    if !media_type.eq_ignore_ascii_case("text/event-stream") {
        return Err(ClientError::new(
            ErrorKind::Protocol,
            "Host did not return an SSE stream",
        ));
    }
    Ok(())
}

pub(crate) fn events(response: Response, session: String) -> Events {
    let bytes = response
        .bytes_stream()
        .map(|chunk| chunk.map_err(|_| io::Error::other("Host stream transport failed")));
    let reader = StreamReader::new(bytes);
    let lines = FramedRead::new(reader, LinesCodec::new_with_max_length(FRAME_LIMIT));
    decode(Box::pin(lines), session)
}

fn decode(lines: Lines, session: String) -> Events {
    let decoder = Decoder {
        lines,
        session,
        frame: Frame::default(),
        first: true,
        done: false,
    };
    Box::pin(stream::unfold(decoder, |mut decoder| async move {
        let result = next_event(&mut decoder).await?;
        decoder.done = result.is_err();
        Some((result, decoder))
    }))
}

struct Decoder {
    lines: Lines,
    session: String,
    frame: Frame,
    first: bool,
    done: bool,
}

async fn next_event(decoder: &mut Decoder) -> Option<ClientResult<ServerEvent>> {
    if decoder.done {
        return None;
    }
    while let Some(line) = decoder.lines.next().await {
        let line = match line {
            Ok(line) => line,
            Err(error) => return Some(Err(line_error(error))),
        };
        if let Some(event) = decoder.consume(&line) {
            return Some(event);
        }
    }
    // SSE dispatches only at a blank line; incomplete EOF frames are dropped.
    None
}

impl Decoder {
    fn consume(&mut self, line: &str) -> Option<ClientResult<ServerEvent>> {
        let line = if self.first {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        self.first = false;
        match self.frame.line(line) {
            Err(error) => Some(Err(error)),
            Ok(Some(data)) => Some(parse_event(&data, &self.session)),
            Ok(None) => None,
        }
    }
}

#[derive(Default)]
struct Frame {
    bytes: usize,
    data: String,
    has_data: bool,
}

impl Frame {
    fn line(&mut self, line: &str) -> ClientResult<Option<String>> {
        if line.is_empty() {
            return Ok(self.finish());
        }
        self.bytes = self.bytes.saturating_add(line.len() + 2);
        if self.bytes > FRAME_LIMIT {
            return Err(oversized_frame());
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        if field == "data" {
            self.has_data = true;
            self.data.push_str(value.strip_prefix(' ').unwrap_or(value));
            self.data.push('\n');
        }
        Ok(None)
    }

    fn finish(&mut self) -> Option<String> {
        self.bytes = 0;
        if !std::mem::take(&mut self.has_data) {
            return None;
        }
        self.data.pop();
        Some(std::mem::take(&mut self.data))
    }
}

fn oversized_frame() -> ClientError {
    ClientError::new(ErrorKind::Protocol, "SSE frame exceeds 4 MiB")
}

fn parse_event(data: &str, session: &str) -> ClientResult<ServerEvent> {
    let event: ServerEvent = serde_json::from_str(data)
        .map_err(|_| ClientError::new(ErrorKind::Protocol, "invalid chat SSE JSON"))?;
    transport::check_session(event.session(), session)?;
    Ok(event)
}

fn line_error(error: LinesCodecError) -> ClientError {
    match error {
        LinesCodecError::MaxLineLengthExceeded => {
            ClientError::new(ErrorKind::Protocol, "SSE line exceeds 4 MiB")
        }
        LinesCodecError::Io(error) if error.kind() == io::ErrorKind::InvalidData => {
            ClientError::new(ErrorKind::Protocol, "invalid SSE UTF-8")
        }
        LinesCodecError::Io(_) => transport::transport_error(),
    }
}
