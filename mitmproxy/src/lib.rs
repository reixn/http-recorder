#![feature(iterator_try_collect)]

use anyhow::Context;
use pyo3::{pyclass, pymethods, pymodule, FromPyObject};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
};

#[derive(FromPyObject)]
struct Headers<'a> {
    fields: Vec<(&'a [u8], &'a [u8])>,
}
#[derive(FromPyObject)]
struct Request<'a> {
    timestamp_start: f64,
    http_version: &'a str,
    method: &'a str,
    url: &'a str,
    headers: Headers<'a>,
    content: Option<&'a [u8]>,
}
impl<'a> Request<'a> {
    fn into_request(self) -> anyhow::Result<http_recorder::Request> {
        http_recorder::Request::parse(
            self.http_version,
            self.method,
            self.url,
            self.headers.fields.into_iter(),
            self.content,
        )
        .context("failed to parse request")
    }
}
#[derive(FromPyObject)]
struct Response<'a> {
    timestamp_end: f64,
    http_version: &'a str,
    status_code: u16,
    headers: Headers<'a>,
    content: Option<&'a [u8]>,
}
impl<'a> Response<'a> {
    fn into_response(self, url: &str) -> anyhow::Result<http_recorder::Response> {
        http_recorder::Response::parse(
            self.http_version,
            self.status_code,
            url,
            self.headers.fields.into_iter(),
            self.content,
        )
        .context("failed to parse response")
    }
}

#[derive(FromPyObject)]
enum Addr<'a> {
    Short((&'a str, u16)),
    Long((&'a str, u16, &'a pyo3::PyAny, &'a pyo3::PyAny)),
}
impl<'a> Addr<'a> {
    fn to_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        let (a, p) = match self {
            Self::Short(ap) => *ap,
            Self::Long((a, p, _, _)) => (*a, *p),
        };
        Ok(std::net::SocketAddr::new(
            a.parse().context("failed to parse address")?,
            p,
        ))
    }
}

#[derive(FromPyObject)]
struct Client<'a> {
    peername: Addr<'a>,
}
#[derive(FromPyObject)]
struct Server<'a> {
    peername: Option<Addr<'a>>,
}
#[derive(FromPyObject)]
pub struct Flow<'a> {
    client_conn: Client<'a>,
    server_conn: Server<'a>,
    request: Request<'a>,
    response: Response<'a>,
}
impl<'a> Flow<'a> {
    fn to_entry(self, index: u32) -> anyhow::Result<http_recorder::Entry> {
        use chrono::TimeZone;
        let utc = chrono::Utc;
        Ok(http_recorder::Entry {
            version: http_recorder::VERSION,
            index,
            client_addr: self.client_conn.peername.to_addr()?,
            server_addr: match self.server_conn.peername {
                Some(a) => Some(a.to_addr()?),
                None => None,
            },
            timings: http_recorder::Timings {
                start_time: utc
                    .timestamp_nanos((self.request.timestamp_start * 1_000_000_000f64) as i64),
                finish_time: utc
                    .timestamp_nanos((self.response.timestamp_end * 1000_000_000f64) as i64),
            },
            response: self.response.into_response(self.request.url)?,
            request: self.request.into_request()?,
        })
    }
}

type Receiver = mpsc::Receiver<http_recorder::Entry>;

mod tar_saver;
mod tmp_saver;

struct Saver {
    receiver: Receiver,
    tmp_saver: tmp_saver::TmpSaver,
    tar_saver: tar_saver::DestSaver,
}
impl Saver {
    fn new<P: AsRef<Path>>(
        receiver: Receiver,
        dest: P,
        entry: http_recorder::Entry,
    ) -> anyhow::Result<Self> {
        let mut tar_saver = tar_saver::DestSaver::new(dest)?;
        tar_saver
            .add_entry(&entry)
            .context("failed to add entry to dest")?;
        Ok(Self {
            receiver,
            tar_saver,
            tmp_saver: tmp_saver::TmpSaver::new(entry).context("failed to create tmp saver")?,
        })
    }
    fn run(mut self) -> anyhow::Result<()> {
        for entry in self.receiver.into_iter() {
            self.tar_saver
                .add_entry(&entry)
                .context("failed to add entry to dest")?;
            if let Err(e) = self.tmp_saver.add_entry(entry) {
                match e {
                    tmp_saver::AddEntryError::Io(e) => {
                        return Err(e).context("failed to save item to tmp dir")
                    }
                    tmp_saver::AddEntryError::Packer => break,
                }
            }
        }
        self.tar_saver
            .finish()
            .context("failed to close dest file")?;
        fs::remove_dir_all(self.tmp_saver.finish()?).context("failed to remove tmp dir")
    }
}

struct InnerRecorder {
    index: u32,
    sender: mpsc::Sender<http_recorder::Entry>,
    handle: std::thread::JoinHandle<anyhow::Result<()>>,
}
enum AddFlowError {
    ParseError(anyhow::Error),
    SaveError,
}
impl InnerRecorder {
    fn new<P: AsRef<Path>>(dest: P, flow: Flow<'_>) -> anyhow::Result<Self> {
        let entry = flow.to_entry(0)?;
        let (sender, receiver) = mpsc::channel();
        Ok(Self {
            sender,
            index: 1,
            handle: std::thread::Builder::new()
                .name(String::from("request-saver"))
                .spawn({
                    let saver = Saver::new(receiver, dest, entry)?;
                    move || Saver::run(saver)
                })
                .context("failed to spawn thrad")?,
        })
    }
    fn add_flow(&mut self, flow: Flow<'_>) -> Result<(), AddFlowError> {
        match self.sender.send(
            flow.to_entry(self.index)
                .map_err(AddFlowError::ParseError)?,
        ) {
            Ok(()) => {
                self.index += 1;
                Ok(())
            }
            Err(_) => Err(AddFlowError::SaveError),
        }
    }
    fn finish(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.handle.join().unwrap()
    }
}

#[pyclass]
struct Recorder {
    dest: PathBuf,
    inner: Option<InnerRecorder>,
}

#[pymethods]
impl Recorder {
    #[new]
    pub fn new(dest: &str) -> anyhow::Result<Self> {
        Ok(Self {
            dest: PathBuf::from(dest),
            inner: None,
        })
    }
    pub fn add_flow(&mut self, flow: Flow<'_>) -> anyhow::Result<()> {
        match &mut self.inner {
            Some(i) => match i.add_flow(flow) {
                Ok(()) => Ok(()),
                Err(AddFlowError::ParseError(p)) => Err(p),
                Err(AddFlowError::SaveError) => Ok(self.finish().unwrap()),
            },
            None => {
                self.inner = Some(InnerRecorder::new(self.dest.as_path(), flow)?);
                Ok(())
            }
        }
    }
    pub fn finish(&mut self) -> anyhow::Result<()> {
        match self.inner.take() {
            Some(i) => i.finish(),
            None => Ok(()),
        }
    }
}

#[pymodule]
#[pyo3(name = "http_recorder")]
pub fn module(_: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    pyo3_log::init();
    m.add_class::<Recorder>()
}
