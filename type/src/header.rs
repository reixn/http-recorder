use serde::{Deserialize, Serialize};

mod name;
pub use name::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HeaderValue {
    Text(String),
    Binary(Box<[u8]>),
}
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InvalidHeaderValue(http::header::InvalidHeaderValue);
impl HeaderValue {
    pub fn parse(value: &[u8]) -> Result<Self, InvalidHeaderValue> {
        let v = http::HeaderValue::from_bytes(value).map_err(InvalidHeaderValue)?;
        Ok(match v.to_str() {
            Ok(s) => Self::Text(s.to_string()),
            Err(_) => Self::Binary(v.as_bytes().to_owned().into_boxed_slice()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Header {
    pub name: HeaderName,
    pub value: HeaderValue,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidHeader {
    #[error("invalid header name")]
    Name(
        #[source]
        #[from]
        name::InvalidHeaderName,
    ),
    #[error("invalid header value")]
    Value(
        #[source]
        #[from]
        InvalidHeaderValue,
    ),
}
impl Header {
    pub fn parse_kv(name: &[u8], value: &[u8]) -> Result<Self, InvalidHeader> {
        Ok(Self {
            name: HeaderName::parse(name).map_err(InvalidHeader::from)?,
            value: HeaderValue::parse(value).map_err(InvalidHeader::from)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Headers(pub Vec<Header>);
impl Headers {
    pub fn parse<HK, HV, I>(headers: I) -> Result<Self, InvalidHeader>
    where
        HK: AsRef<[u8]>,
        HV: AsRef<[u8]>,
        I: Iterator<Item = (HK, HV)>,
    {
        Ok(Self(
            headers
                .map(|(k, v)| Header::parse_kv(k.as_ref(), v.as_ref()))
                .try_collect()?,
        ))
    }
    pub(crate) fn content_type(&self) -> Result<Option<&str>, ()> {
        Ok(match self.0.iter().find(|v| v.name == CONTENT_TYPE) {
            Some(v) => match &v.value {
                HeaderValue::Text(t) => Some(t.as_str()),
                HeaderValue::Binary(_) => {
                    return Err(());
                }
            },
            None => None,
        })
    }
}
