//! Test fixtures, dependency fakes, and fault-injection support for Sporos.

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct HttpStep {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub delay: Duration,
    pub drop_connection: bool,
}

impl HttpStep {
    pub fn json(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body: body.into(),
            delay: Duration::ZERO,
            drop_connection: false,
        }
    }

    pub fn dropped() -> Self {
        Self {
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            delay: Duration::ZERO,
            drop_connection: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub struct ScriptedHttpServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ScriptedHttpServer {
    pub fn start(steps: impl IntoIterator<Item = HttpStep>) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let mut steps = steps.into_iter().collect::<VecDeque<_>>();
        let handle = thread::spawn(move || {
            while let Some(step) = steps.pop_front() {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let Ok(request) = read_request(&mut stream) else {
                    return;
                };
                captured
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(request);
                if step.drop_connection {
                    continue;
                }
                thread::sleep(step.delay);
                if write_response(&mut stream, &step).is_err() {
                    return;
                }
            }
        });
        Ok(Self {
            address,
            requests,
            handle: Some(handle),
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn finish(mut self) -> thread::Result<()> {
        self.handle.take().expect("server handle").join()
    }
}

impl Drop for ScriptedHttpServer {
    fn drop(&mut self) {
        if self.handle.is_some() {
            // A deliberately unfinished fixture may still be blocked in accept; detaching the
            // thread avoids turning test cleanup into a second control plane.
            self.handle.take();
        }
    }
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<CapturedRequest> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if bytes.len() > 1024 * 1024 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
    };
    let head =
        std::str::from_utf8(&bytes[..header_end]).map_err(|_| std::io::ErrorKind::InvalidData)?;
    let mut lines = head.lines();
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let target = request_line.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    let mut content_length = 0_usize;
    for line in lines.filter(|line| !line.is_empty()) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().to_owned();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().map_err(|_| std::io::ErrorKind::InvalidData)?;
        }
        headers.push((name.to_owned(), value));
    }
    while bytes.len().saturating_sub(header_end) < content_length {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Ok(CapturedRequest {
        method,
        target,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

fn write_response(stream: &mut TcpStream, step: &HttpStep) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n",
        step.status,
        step.body.len()
    )?;
    for (name, value) in &step.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&step.body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_delays_drops_and_captures_requests() {
        let server = ScriptedHttpServer::start([
            HttpStep::json(429, br#"{"retry":true}"#.to_vec()),
            HttpStep::dropped(),
        ])
        .unwrap();
        let address = server.address;
        let mut first = TcpStream::connect(address).unwrap();
        first
            .write_all(b"POST /api HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}")
            .unwrap();
        let mut response = String::new();
        first.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 429"));
        let mut second = TcpStream::connect(address).unwrap();
        second.write_all(b"GET /drop HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(second.read(&mut [0_u8; 1]).unwrap(), 0);
        server.finish().unwrap();
    }
}
