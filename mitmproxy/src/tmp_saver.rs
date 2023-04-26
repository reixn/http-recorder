use anyhow::Context;
use http_recorder::{Entries, Entry};
use std::{
    fs, io,
    mem::swap,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};

fn write_entries(entries: &Entries<Vec<Entry>>, path: &Path) -> anyhow::Result<()> {
    let mut buf = xz2::write::XzEncoder::new(
        io::BufWriter::new(fs::File::create(path).context("failed to create pack file")?),
        9,
    );
    ciborium::ser::into_writer(&entries, &mut buf).context("failed to write file")?;
    buf.finish()
        .context("failed to finish compression")?
        .into_inner()
        .context("failed to flush buffer")?;
    Ok(())
}

type TmpEntries = Entries<Vec<Entry>>;

struct PackerHandle {
    sender: mpsc::Sender<TmpEntries>,
    handle: thread::JoinHandle<anyhow::Result<()>>,
}
impl PackerHandle {
    fn finish(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.handle.join().unwrap()
    }
}
struct Packer {
    path: PathBuf,
    unpacked_path: PathBuf,
    receiver: mpsc::Receiver<TmpEntries>,
}
impl Packer {
    fn start(path: PathBuf, unpacked_path: PathBuf) -> anyhow::Result<PackerHandle> {
        let (sender, receiver) = mpsc::channel();
        Ok(PackerHandle {
            sender,
            handle: thread::Builder::new()
                .name(String::from("entry-packer"))
                .spawn(|| {
                    Self {
                        path,
                        unpacked_path,
                        receiver,
                    }
                    .run()
                })
                .context("failed to spawn thread")?,
        })
    }
    fn run(mut self) -> anyhow::Result<()> {
        for (idx, entries) in self.receiver.into_iter().enumerate() {
            self.path.push(format!("{}.bin.xz", idx));
            log::info!(
                "packing requests {}-{}",
                entries.begin_index,
                entries.end_index
            );
            match write_entries(&entries, &self.path) {
                Ok(()) => {
                    log::info!(
                        "packed requests {}-{} to {}",
                        entries.begin_index,
                        entries.end_index,
                        self.path.display()
                    );
                    self.path.pop();
                }
                Err(e) => {
                    log::error!(
                        "failed to pack requests {}-{} {:?}",
                        entries.begin_index,
                        entries.end_index,
                        e
                    );
                    return Err(e);
                }
            }
            for e in &entries.data {
                self.unpacked_path.push(format!("{}.bin", e.index));
                if let Err(e) = fs::remove_file(&self.unpacked_path) {
                    log::error!(
                        "failed to remove file {}: {:?}",
                        self.unpacked_path.display(),
                        e
                    );
                }
                self.unpacked_path.pop();
            }
        }
        Ok(())
    }
}

pub struct TmpSaver {
    tmp_dir: PathBuf,
    unpacked_path: PathBuf,
    entries: TmpEntries,
    packer: PackerHandle,
}
const TMP_PACK_SIZE: u64 = 256 * (1 << 20); // 256 MiB
pub enum AddEntryError {
    Io(io::Error),
    Packer,
}
fn write_entry(path: &mut PathBuf, entry: &Entry) -> Result<(), io::Error> {
    path.push(format!("{}.bin", entry.index));
    let mut data = Vec::new();
    ciborium::ser::into_writer(&entry, &mut data).unwrap();
    fs::write(&path, data)?;
    path.pop();
    Ok(())
}
impl TmpSaver {
    pub fn new(entry: Entry) -> anyhow::Result<Self> {
        let tmp_dir = tempfile::Builder::new()
            .prefix("http-recorder-mitmproxy")
            .tempdir()
            .context("failed to create temp directory")?;
        let mut unpacked_path = tmp_dir.path().join("unpacked");
        fs::create_dir(&unpacked_path).context("failed to create unpacked dir")?;
        write_entry(&mut unpacked_path, &entry).context("failed to write entry")?;
        let mut ret = Self {
            unpacked_path: unpacked_path.clone(),
            packer: Packer::start(tmp_dir.path().to_path_buf(), unpacked_path)
                .context("failed to start packer")?,
            entries: Entries::new(entry.index, entry.timings.clone()),
            tmp_dir: tmp_dir.into_path(),
        };
        ret.save_entry(entry).context("failed to write entry")?;
        Ok(ret)
    }
    fn save_entry(&mut self, entry: Entry) -> Result<(), io::Error> {
        {
            self.unpacked_path.push(format!("{}.bin", entry.index));
            let mut data = Vec::new();
            ciborium::ser::into_writer(&entry, &mut data).unwrap();
            fs::write(&self.unpacked_path, data)?;
            self.unpacked_path.pop();
        }
        self.entries.update(&entry);
        self.entries.data.push(entry);
        Ok(())
    }
    pub fn add_entry(&mut self, entry: Entry) -> Result<(), AddEntryError> {
        if self.entries.content_size() >= TMP_PACK_SIZE {
            let mut entries = Entries::new(entry.index, entry.timings.clone());
            swap(&mut self.entries, &mut entries);
            self.packer
                .sender
                .send(entries)
                .map_err(|_| AddEntryError::Packer)?;
        }
        self.save_entry(entry).map_err(AddEntryError::Io)
    }
    pub fn finish(self) -> anyhow::Result<PathBuf> {
        self.packer.finish()?;
        Ok(self.tmp_dir)
    }
}
