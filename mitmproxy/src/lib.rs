#![feature(iterator_try_collect)]

use anyhow::Context;
use pyo3::{pyclass, pymethods, pymodule, FromPyObject};
use std::{
    fs, io,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, RwLock},
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
    fn to_entry(self) -> anyhow::Result<http_recorder::Entry> {
        use chrono::TimeZone;
        let utc = chrono::Utc;
        Ok(http_recorder::Entry {
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

type Data = Arc<RwLock<http_recorder::HttpRecord>>;
type Cancal = Arc<(Mutex<bool>, Condvar)>;

pub struct Saver {
    cancal: Cancal,
    count: u32,
    data: Data,
    duration: std::time::Duration,
    tmp_dest: PathBuf,
    old_path: PathBuf,
    active_path: PathBuf,
    active_file: fs::File,
    old_file: fs::File,
}
impl Saver {
    fn new(cancal: Cancal, data: Data, duration: std::time::Duration) -> anyhow::Result<Self> {
        let tmp_dir = tempfile::Builder::new()
            .prefix("http-recorder-mitmproxy")
            .tempdir()
            .context("failed to create temp directory")?;
        log::info!("temporary data dir: {}", tmp_dir.path().display());
        let old_path = tmp_dir.path().join("init0");
        let active_path = tmp_dir.path().join("init1");
        Ok(Self {
            cancal,
            count: 0,
            data,
            duration,
            active_file: fs::File::create(&active_path).context("failed to create tmp file")?,
            old_file: fs::File::create(&old_path).context("failed to create tmp file")?,
            tmp_dest: tmp_dir.into_path(),
            old_path,
            active_path,
        })
    }
    fn save_data(&mut self) -> anyhow::Result<()> {
        use std::{
            io::{Seek, Write},
            mem::swap,
            ops::Deref,
        };
        if self
            .old_file
            .stream_position()
            .context("failed to get position")?
            != 0
        {
            self.old_file.rewind().context("failed to seek to begin")?;
            self.old_file
                .set_len(0)
                .context("failed to set file length")?;
        }
        {
            let mut buf = xz2::write::XzEncoder::new(io::BufWriter::new(&self.old_file), 3);
            let lg = self.data.read().unwrap();
            ciborium::ser::into_writer(lg.deref(), &mut buf).context("failed to write file")?;
            buf.finish()
                .context("failed to finish compression")?
                .flush()
                .context("failed to flush buffer")?;
        }

        {
            let mut new_path = self.tmp_dest.join(format!("{}.bin.xz", self.count));
            fs::rename(&self.old_path, &new_path).context("failed to rename file")?;
            self.count += 1;
            swap(&mut new_path, &mut self.active_path);
            self.old_path = new_path;
        }
        swap(&mut self.active_file, &mut self.old_file);
        self.old_file
            .rewind()
            .context("failed to seek old file to begin")?;
        self.old_file
            .set_len(0)
            .context("failed to set old file len")?;

        log::info!("saved tmp data to {}", self.active_path.display());
        Ok(())
    }
    fn run(mut self) {
        while {
            let (lg, t) = self
                .cancal
                .1
                .wait_timeout(self.cancal.0.lock().unwrap(), self.duration)
                .unwrap();
            !*lg && t.timed_out()
        } {
            if let Err(e) = self.save_data() {
                log::error!("failed to save data to tmp dir: {:?}", e);
            }
        }
        if let Err(e) = fs::remove_dir_all(self.tmp_dest) {
            log::error!("failed to remove tmp dir: {:?}", e);
        }
    }
}

#[pyclass]
pub struct Recorder {
    data: Data,
    dirty: bool,
    tar_dest: PathBuf,
    cancel: Cancal,
    handle: Option<std::thread::JoinHandle<()>>,
}
impl Recorder {
    fn new_impl(dest: &str, last_log: Option<&str>, duration: u32) -> anyhow::Result<Self> {
        let data = Arc::new(RwLock::new(match last_log {
            Some(p) => ciborium::de::from_reader(xz2::read::XzDecoder::new(io::BufReader::new(
                fs::File::open(p).context("failed to open log file")?,
            )))
            .context("failed to deserialize log file")?,
            None => http_recorder::HttpRecord::default(),
        }));
        let cancel = Arc::new((Mutex::new(false), Condvar::new()));
        Ok(Self {
            data: Arc::clone(&data),
            dirty: false,
            tar_dest: PathBuf::from(dest),
            cancel: Arc::clone(&cancel),
            handle: Some(
                std::thread::Builder::new()
                    .name(String::from("background-saver"))
                    .spawn({
                        let saver = Saver::new(
                            cancel,
                            data,
                            std::time::Duration::from_secs(duration as u64),
                        )?;
                        move || Saver::run(saver)
                    })
                    .context("failed to spawn thrad")?,
            ),
        })
    }
    fn add_flow_impl(&mut self, flow: Flow<'_>) -> anyhow::Result<()> {
        let mut lg = self.data.write().unwrap();
        lg.entries.push(flow.to_entry()?);
        self.dirty = true;
        Ok(())
    }
    fn save_tar_impl(&mut self) -> anyhow::Result<()> {
        let lg = self.data.read().unwrap();
        lg.write_tar(xz2::write::XzEncoder::new(
            io::BufWriter::new(fs::File::create(&self.tar_dest).context("failed to create file")?),
            9,
        ))
        .context("failed to write tar")?
        .finish()
        .context("failed to finish compression")?
        .into_inner()
        .context("failed to flush buffer")?;
        self.dirty = false;
        Ok(())
    }
    fn stop_auto_save(&mut self) {
        *self.cancel.0.lock().unwrap() = true;
        self.cancel.1.notify_all();
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
    }
    fn finish_impl(&mut self) {
        if self.dirty {
            let _ = self.save_tar_impl();
        }
        self.stop_auto_save();
    }
}

#[pymethods]
impl Recorder {
    #[new]
    pub fn new(dest: &str, duration: u32, last_log: Option<&str>) -> anyhow::Result<Self> {
        Self::new_impl(dest, last_log, duration)
    }
    pub fn add_flow(&mut self, flow: Flow<'_>) -> anyhow::Result<()> {
        self.add_flow_impl(flow)
    }
    pub fn save_tar(&mut self) -> anyhow::Result<()> {
        self.save_tar_impl()
    }
    pub fn finish(&mut self) {
        self.finish_impl()
    }
}

#[pymodule]
#[pyo3(name = "http_recorder")]
pub fn module(_: pyo3::Python, m: &pyo3::types::PyModule) -> pyo3::PyResult<()> {
    pyo3_log::init();
    m.add_class::<Recorder>()
}
