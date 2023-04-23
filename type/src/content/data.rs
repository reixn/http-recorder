use std::{
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct Data<'a, const D: usize> {
    pub digest: &'a [u8; D],
    pub extension: Option<&'a str>,
    pub data: &'a [u8],
}
impl<'a, 'b, const D: usize> PartialEq<Data<'b, D>> for Data<'a, D> {
    fn eq(&self, other: &Data<'b, D>) -> bool {
        self.digest == other.digest && self.extension == other.extension
    }
}
impl<'a, const D: usize> Eq for Data<'a, D> {}
impl<'a, 'b, const D: usize> PartialOrd<Data<'b, D>> for Data<'a, D> {
    fn partial_cmp(&self, other: &Data<'b, D>) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        Some(match self.digest.cmp(&other.digest) {
            Ordering::Equal => self.extension.cmp(&other.extension),
            v => v,
        })
    }
}
impl<'a, 'b, const D: usize> Ord for Data<'a, D> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match self.digest.cmp(&other.digest) {
            Ordering::Equal => self.extension.cmp(&other.extension),
            v => v,
        }
    }
}

pub fn append_file<W: Write, P: AsRef<Path>>(
    builder: &mut tar::Builder<W>,
    path: P,
    data: &[u8],
) -> std::io::Result<()> {
    let mut header = tar::Header::new_old();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    builder.append_data(&mut header, path, data)
}
impl<'a, const D: usize> Data<'a, D> {
    fn write_tar<W: Write>(
        &self,
        builder: &mut tar::Builder<W>,
        path: &mut PathBuf,
    ) -> std::io::Result<()> {
        path.push(base16::encode_lower(&self.digest));
        if let Some(ext) = self.extension {
            path.set_extension(ext);
        }
        append_file(builder, path.as_path(), self.data)?;
        path.pop();
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct DataMap<'a> {
    pub sha256: Vec<Data<'a, { super::SHA256_OUTPUT_SIZE }>>,
}
impl<'a> DataMap<'a> {
    pub fn write_tar<W: Write>(&mut self, builder: &mut tar::Builder<W>) -> std::io::Result<()> {
        fn append_dir<W: Write, P: AsRef<Path>>(
            builder: &mut tar::Builder<W>,
            path: P,
        ) -> std::io::Result<()> {
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(tar::EntryType::Directory);
            h.set_mode(0o755);
            builder.append_data(&mut h, path, std::io::empty())
        }

        let mut p = PathBuf::with_capacity(4 + 1 + 6 + 2 + 1 + 2 + 1 + 64 + 10);
        p.push("data");
        append_dir(builder, p.as_path())?;

        self.sha256.sort();
        self.sha256.dedup();
        if let Some(d) = self.sha256.first() {
            use std::{ffi::OsStr, os::unix::ffi::OsStrExt};
            let mut first: u8;
            let mut second: u8;
            p.push("sha256");
            append_dir(builder, p.as_path())?;

            first = d.digest[0];
            p.push(OsStr::from_bytes(&base16::encode_byte_l(first)));
            append_dir(builder, p.as_path())?;

            second = d.digest[1];
            p.push(OsStr::from_bytes(&base16::encode_byte_l(second)));
            append_dir(builder, p.as_path())?;

            d.write_tar(builder, &mut p)?;

            for d in self.sha256.iter().skip(1) {
                if first != d.digest[0] {
                    p.pop();
                    first = d.digest[0];
                    second = d.digest[1];
                    p.set_file_name(OsStr::from_bytes(&base16::encode_byte_l(first)));
                    append_dir(builder, p.as_path())?;
                    p.push(OsStr::from_bytes(&base16::encode_byte_l(second)));
                    append_dir(builder, p.as_path())?;
                } else if second != d.digest[1] {
                    second = d.digest[1];
                    p.set_file_name(OsStr::from_bytes(&base16::encode_byte_l(second)));
                    append_dir(builder, p.as_path())?;
                }
                d.write_tar(builder, &mut p)?;
            }
        }
        Ok(())
    }
}
