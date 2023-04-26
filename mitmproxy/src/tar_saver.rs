use anyhow::Context;
use http_recorder::{Entries, Entry};
use std::{
    collections::{hash_map, HashMap},
    fs, io,
    mem::swap,
    path::{Path, PathBuf},
};

struct TarFile {
    entry_info: Entries<()>,
    tar_path: PathBuf,
    tar_file: tar::Builder<xz2::write::XzEncoder<io::BufWriter<fs::File>>>,
}
impl TarFile {
    fn new(dest: &Path, tar_index: u32, entry: &http_recorder::Entry) -> anyhow::Result<Self> {
        let tar_path = dest.join(format!("{}.tar.xz", tar_index));
        let mut ret = Self {
            entry_info: Entries::new(entry.index, entry.timings.clone()),
            tar_file: tar::Builder::new(xz2::write::XzEncoder::new(
                io::BufWriter::new(
                    fs::File::options()
                        .write(true)
                        .create_new(true)
                        .open(&tar_path)
                        .context("failed to create dest file")?,
                ),
                9,
            )),
            tar_path,
        };
        ret.add_entry(entry)?;
        Ok(ret)
    }
    fn add_entry(&mut self, entry: &http_recorder::Entry) -> anyhow::Result<()> {
        use http_recorder::{content::Content, request};
        let mut path = PathBuf::from(entry.index.to_string());
        let mut file_header = {
            let mut ret = tar::Header::new_gnu();
            ret.set_mode(0o644);
            ret
        };
        let mut dir_header = {
            let mut ret = tar::Header::new_gnu();
            ret.set_mode(0o755);
            ret.set_entry_type(tar::EntryType::Directory);
            ret
        };
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
    fn finish(mut self) -> anyhow::Result<()> {
        self.tar_file
            .into_inner()
            .context("failed to finish writing tar")?
            .finish()
            .context("failed to finish compress")?
            .into_inner()
            .context("failed to flush tar buffer")?;
        self.tar_path.set_extension("");
        self.tar_path.set_extension("yaml");
        fs::write(
            self.tar_path,
            serde_yaml::to_string(&self.entry_info).unwrap(),
        )
        .context("failed to write entries info")
    }
}

struct HostSaver {
    count: u32,
    path: PathBuf,
    entries: Entries<()>,
    tar_file: TarFile,
}
const MAX_PACK: u64 = 512 * (1 << 20); // 512 MiB
impl HostSaver {
    fn new(path: &Path, domain: &str, entry: &Entry) -> anyhow::Result<Self> {
        let mut path = path.join(domain);
        path.push(chrono::Local::now().to_rfc3339());
        fs::create_dir_all(&path).context("failed to create dest dir")?;
        Ok(Self {
            count: 0,
            entries: {
                let mut ret = Entries::new(entry.index, entry.timings.clone());
                ret.update(entry);
                ret
            },
            tar_file: TarFile::new(path.as_path(), 0, entry)
                .context("failed to create tar file")?,
            path,
        })
    }
    fn add_entry(&mut self, entry: &Entry) -> anyhow::Result<()> {
        self.entries.update(entry);
        if self.tar_file.entry_info.content_size() > MAX_PACK {
            self.count += 1;
            let mut tar_file = TarFile::new(self.path.as_path(), self.count, entry)
                .context("failed to create new tar file")?;
            swap(&mut self.tar_file, &mut tar_file);
            tar_file.finish().context("failed to finish old tar file")
        } else {
            self.tar_file.add_entry(entry)
        }
    }
    fn finish(mut self) -> anyhow::Result<()> {
        self.tar_file.finish()?;
        self.path.push("info.yaml");
        fs::write(self.path, serde_yaml::to_string(&self.entries).unwrap())
            .context("failed to write info")
    }
}

pub struct DestSaver {
    path: PathBuf,
    hosts: HashMap<String, HostSaver>,
}
impl DestSaver {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        Ok(Self {
            path,
            hosts: HashMap::new(),
        })
    }
    pub fn add_entry(&mut self, entry: &Entry) -> anyhow::Result<()> {
        let host = entry.request.url.host.as_ref().map_or_else(
            || String::from("unknown"),
            |h| match h {
                http_recorder::url::Host::Domain(d) => d.clone(),
                http_recorder::url::Host::Addr(a) => match entry.request.url.port {
                    Some(p) => std::net::SocketAddr::new(*a, p).to_string(),
                    None => a.to_string(),
                },
            },
        );
        match self.hosts.entry(host) {
            hash_map::Entry::Occupied(mut o) => o.get_mut().add_entry(entry),
            hash_map::Entry::Vacant(v) => {
                let saver = HostSaver::new(self.path.as_path(), v.key().as_str(), entry)?;
                v.insert(saver);
                Ok(())
            }
        }
    }
    pub fn finish(self) -> anyhow::Result<()> {
        for (k, v) in self.hosts.into_iter() {
            v.finish()
                .with_context(|| format!("failed to close tar for {}", k))?;
        }
        Ok(())
    }
}
