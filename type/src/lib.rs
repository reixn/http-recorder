#![feature(iterator_try_collect)]

use serde::{Deserialize, Serialize};
use std::{error, fmt::Display, net::SocketAddr, str::FromStr};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub major: u16,
    pub minor: u16,
}
pub const VERSION: Version = Version { major: 0, minor: 1 };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub version: Version,
    pub index: u32,
    pub client_addr: SocketAddr,
    pub server_addr: Option<SocketAddr>,
    pub timings: Timings,
    pub request: request::Request,
    pub response: response::Response,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodySize {
    pub request: u64,
    pub response: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entries<T> {
    pub begin_index: u32,
    pub begin_time: Timings,
    pub end_index: u32,
    pub end_time: Timings,
    pub count: u32,
    pub body_size: BodySize,
    pub data: T,
}
impl<T: Default> Entries<T> {
    pub fn new(begin_index: u32, begin_time: Timings) -> Self {
        Self {
            begin_index,
            begin_time: begin_time.clone(),
            end_index: begin_index,
            end_time: begin_time,
            count: 0,
            body_size: BodySize {
                request: 0,
                response: 0,
            },
            data: T::default(),
        }
    }
    pub fn update(&mut self, entry: &Entry) {
        self.end_index = entry.index;
        self.end_time = entry.timings.clone();
        self.count += 1;
        self.body_size.request += entry.request.body.as_ref().map_or(0, |b| match b {
            request::Body::Content(c) => c.size,
            request::Body::MultipartForm(f) => f.iter().map(|f| f.content.size).sum(),
            request::Body::UrlEncodedForm(_) => 0,
        });
        self.body_size.response += entry.response.content.as_ref().map_or(0, |r| r.size);
    }
    pub const fn content_size(&self) -> u64 {
        self.body_size.request + self.body_size.response
    }
}
