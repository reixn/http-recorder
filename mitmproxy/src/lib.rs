#![feature(iterator_try_collect)]

use anyhow::Context;
use pyo3::{pyclass, pymethods, pymodule, FromPyObject};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
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
    fn into_entry(self, index: u32) -> anyhow::Result<http_recorder::Entry> {
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
                    .timestamp_nanos((self.request.timestamp_start * 1_000_000_000_f64) as i64),
                finish_time: utc
                    .timestamp_nanos((self.response.timestamp_end * 1_000_000_000_f64) as i64),
            },
            response: self.response.into_response(self.request.url)?,
            request: self.request.into_request()?,
        })
    }
}

mod tar_saver;
mod tmp_saver;

struct InnerRecorder {
    index: u32,
    tmp_saver: tmp_saver::TmpSaver,
    dest_saver: tar_saver::DestSaverHandle,
}
enum AddFlowError {
    ParseError(anyhow::Error),
    SaveError(anyhow::Error),
    SaverFailed,
}
impl InnerRecorder {
    fn new<P: AsRef<Path>>(dest: P, flow: Flow<'_>) -> anyhow::Result<Self> {
        let entry = flow.into_entry(0)?;
        let (tmp_core, dest_core) = {
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            if cores.len() < 3 {
                log::warn!("too few cpu cores: {}, at lease 3 recommanded", cores.len());
            }
            (cores.get(0).copied(), cores.get(1).copied())
        };
        let mut ret = Self {
            index: 0,
            tmp_saver: tmp_saver::TmpSaver::new(tmp_core, &entry)
                .context("failed to start tmp saver")?,
            dest_saver: tar_saver::DestSaver::start(dest, dest_core, &entry)
                .context("failed to start tar saver")?,
        };
        match ret.save_entry(Arc::new(entry)) {
            Ok(()) => Ok(ret),
            Err(e) => Err(match e {
                AddFlowError::ParseError(e) => e.context("failed to parse flow"),
                AddFlowError::SaveError(e) => e,
                AddFlowError::SaverFailed => {
                    log::error!("saver failed {:?}", ret.finish().unwrap_err());
                    std::process::abort();
                }
            }),
        }
    }
    fn save_entry(&mut self, entry: Arc<http_recorder::Entry>) -> Result<(), AddFlowError> {
        if let Err(e) = self.tmp_saver.add_entry(Arc::clone(&entry)) {
            return Err(match e {
                tmp_saver::AddEntryError::Io(e) => AddFlowError::SaveError(
                    anyhow::Error::new(e).context("failed to save entry to tmpdir"),
                ),
                tmp_saver::AddEntryError::Packer => AddFlowError::SaverFailed,
            });
        }
        self.dest_saver
            .sender
            .send(entry)
            .map_err(|_| AddFlowError::SaverFailed)
    }
    fn add_flow(&mut self, flow: Flow<'_>) -> Result<(), AddFlowError> {
        self.save_entry(Arc::new(
            flow.into_entry(self.index)
                .map_err(AddFlowError::ParseError)?,
        ))?;
        self.index += 1;
        Ok(())
    }
    fn finish(self) -> anyhow::Result<PathBuf> {
        self.dest_saver
            .finish()
            .context("failed to finish dest saver")?;
        self.tmp_saver.finish()
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
                Err(AddFlowError::SaveError(e)) => {
                    log::error!("{:?}", e);
                    std::process::abort()
                }
                Err(AddFlowError::SaverFailed) => self.finish(),
            },
            None => {
                self.inner = Some(InnerRecorder::new(self.dest.as_path(), flow)?);
                Ok(())
            }
        }
    }
    pub fn finish(&mut self) -> anyhow::Result<()> {
        match self.inner.take() {
            Some(i) => match i.finish() {
                Ok(p) => fs::remove_dir_all(p).context("failed to remove tmp dir"),
                Err(e) => {
                    log::error!("saver failed: {:?}", e);
                    std::process::abort()
                }
            },
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
