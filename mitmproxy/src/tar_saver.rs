use anyhow::Context;
use http_recorder::{Entries, Entry};
use std::{
    collections::{hash_map, HashMap},
    fs, io,
    mem::swap,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
};

struct TarFile {
    entry_info: Entries<()>,
    hosts: HashMap<Option<http_recorder::url::Host>, String>,
    tar_file: tar::Builder<xz2::write::XzEncoder<io::BufWriter<fs::File>>>,
}
impl TarFile {
    fn new(dest: &Path, tar_index: u32, entry: &http_recorder::Entry) -> anyhow::Result<Self> {
        let tar_path = dest.join(format!("{}.tar.xz", tar_index));
        Ok(Self {
            entry_info: Entries::new(entry.index, entry.timings.clone()),
            tar_file: tar::Builder::new(xz2::write::XzEncoder::new(
                io::BufWriter::new(
                    fs::File::options()
                        .write(true)
                        .create_new(true)
                        .open(tar_path)
                        .context("failed to create dest file")?,
                ),
                9,
            )),
            hosts: HashMap::new(),
        })
    }
    fn add_entry(&mut self, entry: &http_recorder::Entry) -> anyhow::Result<()> {
        use http_recorder::{content::Content, request};
        let mut dir_header = {
            let mut ret = tar::Header::new_gnu();
            ret.set_mode(0o755);
            ret.set_entry_type(tar::EntryType::Directory);
            ret
        };
        let mut file_header = {
            let mut ret = tar::Header::new_gnu();
            ret.set_mode(0o444);
            ret
        };
        let mut path = match self.hosts.entry(entry.request.url.host.clone()) {
            hash_map::Entry::Occupied(o) => PathBuf::from(o.get().as_str()),
            hash_map::Entry::Vacant(v) => {
                let s = v.insert(match &entry.request.url.host {
                    Some(h) => match h {
                        http_recorder::url::Host::Domain(d) => d.to_owned(),
                        http_recorder::url::Host::Addr(a) => a.to_string(),
                    },
                    None => String::from("unknown"),
                });
                let path = PathBuf::from(s.as_str());
                self.tar_file
                    .append_data(&mut dir_header, &path, io::empty())
                    .context("failed to create domain dir")?;
                path
            }
        };
        path.push(entry.index.to_string());
        self.tar_file
            .append_data(&mut dir_header, &path, io::empty())
            .context("failed to create dir")?;
        if let Some(body) = &entry.request.body {
            match body {
                request::Body::Content(Content {
                    data: Some(data), ..
                }) => {
                    path.push("request-body");
                    file_header.set_size(data.len() as u64);
                    self.tar_file
                        .append_data(&mut file_header, &path, data.as_ref())
                        .context("failed to write request body")?;
                    path.pop();
                }
                request::Body::MultipartForm(v) if !v.is_empty() => {
                    path.push("request-body");
                    self.tar_file
                        .append_data(&mut dir_header, &path, io::empty())
                        .context("failed to write request body dir")?;
                    for (idx, f) in v.iter().enumerate() {
                        if let Some(data) = &f.content.data {
                            match &f.content.extension {
                                Some(ext) => path.push(format!("{}.{}", idx, ext)),
                                None => path.push(idx.to_string()),
                            }
                            file_header.set_size(data.len() as u64);
                            self.tar_file
                                .append_data(&mut file_header, &path, data.as_ref())
                                .with_context(|| {
                                    format!("failed to write multipart form field {}", idx)
                                })?;
                            path.pop();
                        }
                    }
                    path.pop();
                }
                _ => (),
            }
        }
        if let Some(content) = &entry.response.content {
            if let Some(data) = &content.data {
                match &content.extension {
                    Some(ext) => path.push(format!("response-body.{}", ext)),
                    None => path.push("response-body"),
                }
                file_header.set_size(data.len() as u64);
                self.tar_file
                    .append_data(&mut file_header, &path, data.as_ref())
                    .context("failed to write response body")?;
                path.pop();
            }
        }
        {
            path.push("entry.bin");
            let data = {
                let mut r = Vec::new();
                ciborium::ser::into_writer(entry, &mut r).unwrap();
                r
            };
            file_header.set_size(data.len() as u64);
            self.tar_file
                .append_data(&mut file_header, &path, data.as_slice())
                .context("failed to write cbor")?;
            path.pop();
        }
        {
            path.push("entry.json");
            let data = serde_json::to_vec(entry).unwrap();
            file_header.set_size(data.len() as u64);
            self.tar_file
                .append_data(&mut file_header, &path, data.as_slice())
                .context("failed to write json")?;
            path.pop();
        }
        self.entry_info.update(entry);
        Ok(())
    }
    fn finish(self) -> anyhow::Result<Entries<()>> {
        self.tar_file
            .into_inner()
            .context("failed to finish writing tar")?
            .finish()
            .context("failed to finish compress")?
            .into_inner()
            .context("failed to flush tar buffer")?;
        Ok(self.entry_info)
    }
}

const MAX_PACK: u64 = 512 * (1 << 20); // 512 MiB

pub struct DestSaver {
    count: u32,
    path: PathBuf,
    entries: Entries<Vec<Entries<()>>>,
    tar_file: TarFile,
}
impl DestSaver {
    pub fn start<P: AsRef<Path>>(
        path: P,
        core: Option<core_affinity::CoreId>,
        entry: &Entry,
    ) -> anyhow::Result<DestSaverHandle> {
        let path = path.as_ref().to_path_buf();
        let (sender, receiver) = mpsc::channel();
        let ret = Self {
            count: 0,
            entries: Entries::new(entry.index, entry.timings.clone()),
            tar_file: TarFile::new(path.as_path(), 0, entry)
                .context("failed to create tar file")?,
            path,
        };
        Ok(DestSaverHandle {
            handle: thread::Builder::new()
                .name(String::from("tar-saver"))
                .spawn(move || {
                    if let Some(c) = core {
                        core_affinity::set_for_current(c);
                    }
                    ret.run(receiver)
                })
                .context("failed to spawn thread")?,
            sender,
        })
    }
    fn add_entry(&mut self, entry: &Entry) -> anyhow::Result<()> {
        if self.tar_file.entry_info.content_size() > MAX_PACK {
            self.count += 1;
            let mut tar_file = TarFile::new(self.path.as_path(), self.count, entry)
                .context("failed to create new tar file")?;
            swap(&mut self.tar_file, &mut tar_file);
            self.entries.data.push(
                tar_file
                    .finish()
                    .context("failed to finish packed tar file")?,
            );
        }
        self.tar_file
            .add_entry(entry)
            .context("failed to add entry")?;
        self.entries.update(entry);
        Ok(())
    }
    fn run(mut self, receiver: mpsc::Receiver<Arc<Entry>>) -> anyhow::Result<()> {
        for entry in receiver.into_iter() {
            self.add_entry(entry.as_ref())
                .context("failed to add entry to tar")?;
        }
        self.entries.data.push(
            self.tar_file
                .finish()
                .context("failed to finish packed tar file")?,
        );
        let info = serde_yaml::to_string(&self.entries).unwrap();
        self.path.push("info.yaml");
        fs::write(self.path, info).context("failed to write info file")
    }
}

pub struct DestSaverHandle {
    handle: thread::JoinHandle<anyhow::Result<()>>,
    pub sender: mpsc::Sender<Arc<Entry>>,
}
impl DestSaverHandle {
    pub fn finish(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.handle.join().unwrap()
    }
}
