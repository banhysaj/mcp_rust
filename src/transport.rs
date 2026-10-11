//! Transports: the read and write halves of a newline-delimited message stream.

use std::collections::VecDeque;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};

/// The write half: deliver one message.
pub trait LineWrite: Send {
    /// Write one message, normally terminated with a newline.
    fn write_line(&mut self, line: &str) -> io::Result<()>;
}

/// A transport that can be read from and written to.
///
/// The read half is owned by the server's read loop. The write half has to be
/// cheap to clone, because each worker thread writes through its own clone; for a
/// handle to the process-wide stdio streams that costs nothing, and a transport
/// with per-connection state can share that state behind an `Arc`.
///
/// [`StdioTransport`] is the transport used by [`crate::Server::run_stdio`].
/// Implement this trait to serve a line-delimited protocol over something else, or
/// use [`MemoryTransport`] to drive the server in process.
pub trait Transport: LineWrite + Clone + Send + 'static {
    /// Read one message, without its trailing newline.
    ///
    /// Returns `Ok(None)` at end of input.
    fn read_line(&mut self) -> io::Result<Option<String>>;
}

/// Newline-delimited JSON over stdin and stdout.
///
/// Rust's standard handles are binary, so no newline translation is applied on
/// Windows: a `\n` written to stdout is a single byte on every platform.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdioTransport;

impl StdioTransport {
    /// A handle to the process-wide stdin and stdout.
    pub fn new() -> Self {
        StdioTransport
    }
}

impl LineWrite for StdioTransport {
    fn write_line(&mut self, line: &str) -> io::Result<()> {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        out.write_all(line.as_bytes())?;
        out.write_all(b"\n")?;
        out.flush()
    }
}

impl Transport for StdioTransport {
    fn read_line(&mut self) -> io::Result<Option<String>> {
        let mut buf = Vec::new();
        let n = io::stdin().lock().read_until(b'\n', &mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        // Strip the newline but keep a carriage return: the server trims whitespace
        // from the line before parsing, so CRLF input works either way.
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        // JSON is UTF-8. Invalid bytes become replacement characters so that a
        // malformed line turns into a parse error instead of ending the session.
        Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
    }
}

/// An in-memory transport with a fixed list of input lines and a captured output.
///
/// Useful for tests, and for embedding the server in a host that already speaks
/// line-delimited JSON.
#[derive(Debug, Clone, Default)]
pub struct MemoryTransport {
    input: Arc<Mutex<VecDeque<String>>>,
    output: Arc<Mutex<Vec<String>>>,
}

impl MemoryTransport {
    /// Build a transport that will read `lines` in order, then report end of input.
    pub fn new<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        MemoryTransport {
            input: Arc::new(Mutex::new(lines.into_iter().map(Into::into).collect())),
            output: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Queue one more input line.
    pub fn push_input(&self, line: impl Into<String>) {
        self.input.lock().unwrap().push_back(line.into());
    }

    /// Everything written so far, one entry per `write_line` call.
    pub fn output(&self) -> Vec<String> {
        self.output.lock().unwrap().clone()
    }

    /// Everything written so far, joined by newlines with a trailing newline.
    ///
    /// This is the byte stream a transport-level client would have seen.
    pub fn output_text(&self) -> String {
        let out = self.output.lock().unwrap();
        if out.is_empty() {
            return String::new();
        }
        let mut s = out.join("\n");
        s.push('\n');
        s
    }
}

impl LineWrite for MemoryTransport {
    fn write_line(&mut self, line: &str) -> io::Result<()> {
        self.output.lock().unwrap().push(line.to_string());
        Ok(())
    }
}

impl Transport for MemoryTransport {
    fn read_line(&mut self) -> io::Result<Option<String>> {
        Ok(self.input.lock().unwrap().pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_transport_round_trips() {
        let t = MemoryTransport::new(["a", "b"]);
        let mut writer = t.clone();
        writer.write_line("x").unwrap();
        let mut reader = t.clone();
        assert_eq!(reader.read_line().unwrap().as_deref(), Some("a"));
        assert_eq!(reader.read_line().unwrap().as_deref(), Some("b"));
        assert_eq!(reader.read_line().unwrap(), None);
        assert_eq!(t.output(), vec!["x".to_string()]);
        assert_eq!(t.output_text(), "x\n");
        t.push_input("c");
        assert_eq!(reader.read_line().unwrap().as_deref(), Some("c"));
    }
}
