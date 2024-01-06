use anyhow::{bail, Context, Result};
use async_watcher::{notify::RecursiveMode, AsyncDebouncer};
use brotli::{enc::BrotliEncoderParams, BrotliCompress};
use log::{debug, error, info, trace, warn};
use std::{
    collections::{HashMap, VecDeque},
    env,
    fmt::{Arguments, Display},
    fs,
    io::{Cursor, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpSocket, TcpStream},
    sync::broadcast::{self, Receiver},
};

const REQUEST_BUFFER_SIZE: usize = 4096;

struct File {
    uncompressed: Vec<u8>,
    compressed: Vec<u8>,
    mime_type: &'static str,
}

impl File {
    fn new(uncompressed: Vec<u8>, mime_type: &'static str) -> Result<Self> {
        let mut reader = Cursor::new(uncompressed);
        let mut compressed: Vec<u8> = Vec::new();
        BrotliCompress(
            &mut reader,
            &mut compressed,
            &BrotliEncoderParams::default(),
        )?;

        let uncompressed = reader.into_inner();

        let compression_ratio = uncompressed.len() as f64 / compressed.len() as f64;
        debug!("Compression ratio: {compression_ratio}");

        Ok(Self {
            uncompressed,
            compressed,
            mime_type,
        })
    }
}

type FileCache = HashMap<PathBuf, File>;

struct Args {
    backlog: u32,
    addr: SocketAddr,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            backlog: 1024,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        }
    }
}

fn cache_files() -> Result<FileCache> {
    let mut entries: VecDeque<_> = fs::read_dir("static")?.collect();
    let mut files = HashMap::new();

    while let Some(entry) = entries.pop_front() {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                error!("{e}");
                continue;
            }
        };

        let metadata = entry
            .metadata()
            .with_context(|| format!("Tried reading metadata for {}", entry.path().display()))?;

        let path = entry.path();

        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                entries.push_back(entry)
            }
            continue;
        }

        let content = fs::read(&path)
            .with_context(|| format!("Couldn't read file {}", entry.path().display()))?;

        let mime_type = get_mime_type(&path);

        let file = File::new(content, mime_type)?;

        //Skip static/
        let path: PathBuf = path.components().skip(1).collect();
        debug!("Caching as {}", path.display());
        files.insert(path, file);
    }

    Ok(files)
}

fn get_mime_type(path: &Path) -> &'static str {
    match path.extension().map(|s| s.to_str()).flatten() {
        Some("html") => "text/html",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    }
}

fn parse_args() -> Result<Args> {
    let mut args = Args::default();

    for arg in env::args() {
        if arg.starts_with("--backlog=") {
            args.backlog = arg[10..]
                .parse()
                .with_context(|| format!("Cannot parse {} as a number", &arg[10..]))?;
        } else if arg.starts_with("--addr=") {
            let ip = arg[7..]
                .parse()
                .with_context(|| format!("Cannot parse {} as an ip address", &arg[7..]))?;

            args.addr.set_ip(ip);
        } else if arg.starts_with("--port=") {
            let port = arg[7..]
                .parse()
                .with_context(|| format!("Cannot parse {} as a port number", &arg[7..]))?;

            args.addr.set_port(port);
        } else {
            warn!("Unknown argument {arg}");
        }
    }

    Ok(args)
}

fn format_to<'buf>(buf: &'buf mut [u8], fmt: Arguments) -> Result<&'buf str> {
    let mut writer = Cursor::new(buf);

    writer.write_fmt(fmt)?;

    let n = writer.position() as usize;

    let buf = writer.into_inner();
    let s = str::from_utf8(&buf[..n])?;

    Ok(s)
}

async fn handle_connection(
    connection: TcpStream,
    mut notifier: Receiver<()>,
    files: &FileCache,
) -> Result<()> {
    let mut connection = BufStream::new(connection);

    let mut buf = [0u8; REQUEST_BUFFER_SIZE];

    loop {
        let nread = tokio::select! {
            res = connection.read(&mut buf) => res?,
            _ = notifier.recv() => return Ok(())
        };
        if nread == 0 {
            break;
        }
        trace!("read {nread} bytes from socket");
        let s = str::from_utf8(&buf[..nread])?;

        let Some(s) = s.strip_prefix("GET ") else {
            debug!("Invalid verb in {s:?}");
            connection
                .write(b"HTTP/1.1 400 Invalid verb\r\n\r\n")
                .await?;
            connection.flush().await?;
            bail!("Invalid verb");
        };

        let Some(n) = s.find(' ') else {
            debug!("invalid path in {s:?}");
            connection
                .write(b"HTTP/1.1 400 Invalid path\r\n\r\n")
                .await?;
            connection.flush().await?;
            bail!("Invalid path");
        };

        let path = match &s[..n] {
            "/" => "index.html",
            s => s.strip_prefix('/').unwrap_or(s),
        };
        debug!("Attempting to serve file {path:?}");
        let path = Path::new(path);

        let n = n + 1;
        let s = &s[n..];

        let s = match s.strip_prefix("HTTP/1.1\r\n") {
            Some(s) => s,
            None => {
                debug!("Invalid Request version {s:?}");
                connection
                    .write(b"HTTP/1.1 400 Invalid version\r\n\r\n")
                    .await?;
                bail!("Invalid request version");
            }
        };

        let mut use_compression = false;

        for line in s.lines().take_while(|s| !s.is_empty()) {
            if let Some(line) = line.strip_prefix("Accept-Encoding: ") {
                use_compression = line.contains("br");
            }
        }

        let Some(file) = files.get(path) else {
            debug!("Not found: {}", path.display());
            connection
                .write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 13\r\nConnection: Keep-Alive\r\n\r\nNot Found :.(")
                .await?;
            connection.flush().await?;
            continue;
        };

        let content = if use_compression {
            trace!("Client accepts compression :)");
            &file.compressed
        } else {
            &file.uncompressed
        };
        let encoding_header = if use_compression {
            "Content-Encoding: br\r\n"
        } else {
            ""
        };

        let response = format_to(&mut buf, format_args!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: keep-alive\r\n{}\r\n",
                content.len(),
                file.mime_type,
                encoding_header,
            ))?;

        connection.write_all(response.as_bytes()).await?;
        connection.write_all(&content).await?;
        connection.flush().await?;
    }

    debug!("Connection closed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { backlog, addr } = parse_args()?;

    debug!("connection backlog = {backlog}");

    let files = Arc::new(cache_files()?);

    let (sender, mut receiver) = broadcast::channel(backlog as usize);

    let task_sender = sender.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Can listen for CTRL-C");
        task_sender.send(()).expect("Can send shutdown signal");
    });

    let mut tasks = vec![];

    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;

    socket.bind(addr)?;
    let socket = socket.listen(backlog)?;
    info!("Listening on {}", socket.local_addr()?);

    loop {
        tokio::select! {
            Ok((connection, addr)) = socket.accept() => {
                debug!("Got connection from {}", addr);

                let files = Arc::clone(&files);
                let receiver = sender.subscribe();
                let task = tokio::spawn(async move {
                    if let Err(e) = handle_connection(connection, receiver, &files).await {
                        warn!("Failed to handle connection {addr} : {e}");
                    }
                });
                tasks.push(task);
            },
            _ = receiver.recv() => break,
        };
    }

    info!("Received shutdown signal. Goodybe :)");

    for task in tasks {
        if let Err(e) = task.await {
            error!("Handler panicked: {e}");
        }
    }

    Ok(())
}
