#![feature(iterator_try_collect)]

use serde::{Deserialize, Serialize};
use std::{error, fmt::Display, io::Write, net::SocketAddr, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpVersion {
    Http09,
    Http10,
    Http11,
    H2,
    H3,
}

#[derive(Debug)]
pub struct HttpVersionParseErr(String);
impl Display for HttpVersionParseErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid http version: {}", self.0)
    }
}
impl error::Error for HttpVersionParseErr {}

impl FromStr for HttpVersion {
    type Err = HttpVersionParseErr;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HTTP/0.9" => Ok(Self::Http09),
            "HTTP/1.0" => Ok(Self::Http10),
            "HTTP/1.1" => Ok(Self::Http11),
            "HTTP/2.0" => Ok(Self::H2),
            "HTTP/3.0" => Ok(Self::H3),
            v => Err(HttpVersionParseErr(v.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Head,
    Options,
    Connect,
    Patch,
    Trace,
    Extension(Box<str>),
}
impl FromStr for Method {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "GET" => Self::Get,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "DELETE" => Self::Delete,
            "HEAD" => Self::Head,
            "OPTIONS" => Self::Options,
            "CONNECT" => Self::Connect,
            "PATCH" => Self::Patch,
            "TRACE" => Self::Trace,
            v => Self::Extension(v.to_owned().into()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCode(pub u16);
impl PartialEq<u16> for StatusCode {
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

pub mod header;
pub use header::{Header, Headers};

pub mod content;

pub mod url;

pub mod request;
pub use request::Request;

pub mod response;
pub use response::Response;

mod serde_date_time {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(
                value
                    .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false)
                    .as_str(),
            )
        } else {
            chrono::serde::ts_microseconds::serialize(value, serializer)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        if deserializer.is_human_readable() {
            DateTime::<Utc>::deserialize(deserializer)
        } else {
            chrono::serde::ts_microseconds::deserialize(deserializer)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timings {
    #[serde(with = "serde_date_time")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[serde(with = "serde_date_time")]
    pub finish_time: chrono::DateTime<chrono::Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub client_addr: SocketAddr,
    pub server_addr: Option<SocketAddr>,
    pub timings: Timings,
    pub request: request::Request,
    pub response: response::Response,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}
pub const VERSION: Version = Version { major: 0, minor: 1 };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRecord {
    pub version: Version,
    pub entries: Vec<Entry>,
}
impl Default for HttpRecord {
    fn default() -> Self {
        Self {
            version: VERSION,
            entries: Vec::new(),
        }
    }
}
impl HttpRecord {
    pub fn write_tar<W: Write>(&self, writer: W) -> std::io::Result<W> {
        use content::data::{append_file, DataMap};
        let mut builder = tar::Builder::new(writer);
        {
            let mut data = DataMap::default();
            self.entries.iter().for_each(|v| {
                if let Some(v) = &v.request.body {
                    v.take_data(&mut data);
                }
                if let Some(v) = &v.response.content {
                    v.take_data(&mut data);
                }
            });
            data.write_tar(&mut builder)?;
        }
        append_file(
            &mut builder,
            "entries.json",
            serde_json::to_vec(&self.entries).unwrap().as_slice(),
        )?;
        append_file(
            &mut builder,
            "version.json",
            serde_json::to_vec(&self.version).unwrap().as_slice(),
        )?;
        builder.into_inner()
    }
}
