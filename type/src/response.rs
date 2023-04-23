use crate::{
    content::Content,
    header::{self, HeaderValue, Headers, InvalidHeader},
    serde_date_time, HttpVersion, StatusCode,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expiration {
    DateTime(#[serde(with = "serde_date_time")] chrono::DateTime<chrono::Utc>),
    Session,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub http_only: Option<bool>,
    pub secure: Option<bool>,
    pub same_site: Option<SameSite>,
    pub max_age: Option<std::time::Duration>,
    pub expires: Option<Expiration>,
}
#[derive(Debug, thiserror::Error)]
pub enum CookieParseError {
    #[error("invalid header binary data")]
    InvalidHeader,
    #[error("failed to parse Set-Cookie header")]
    Parse(
        #[source]
        #[from]
        cookie::ParseError,
    ),
    #[error("invalid max age value")]
    MaxAge(
        #[source]
        #[from]
        time::error::ConversionRange,
    ),
}
impl Cookie {
    pub fn parse_header(value: &HeaderValue) -> Result<Self, CookieParseError> {
        let cok = cookie::Cookie::parse_encoded(match value {
            HeaderValue::Text(s) => s.as_str(),
            HeaderValue::Binary(_) => return Err(CookieParseError::InvalidHeader),
        })
        .map_err(CookieParseError::from)?;
        Ok(Cookie {
            name: cok.name().to_owned(),
            value: cok.value().to_owned(),
            domain: cok.domain().map(str::to_string),
            path: cok.path().map(str::to_string),
            http_only: cok.http_only(),
            secure: cok.secure(),
            same_site: cok.same_site().map(|v| match v {
                cookie::SameSite::Lax => SameSite::Lax,
                cookie::SameSite::None => SameSite::None,
                cookie::SameSite::Strict => SameSite::Strict,
            }),
            max_age: match cok.max_age() {
                Some(d) => Some(std::time::Duration::try_from(d).map_err(CookieParseError::from)?),
                None => None,
            },
            expires: cok.expires().map(|v| match v {
                cookie::Expiration::DateTime(d) => Expiration::DateTime({
                    use chrono::TimeZone;
                    let u = chrono::Utc;
                    u.timestamp_nanos(d.unix_timestamp_nanos() as i64)
                }),
                cookie::Expiration::Session => Expiration::Session,
            }),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cookies(pub Vec<Cookie>);
impl Cookies {
    pub fn parse_headers(headers: &Headers) -> Result<Self, CookieParseError> {
        Ok(Self(
            headers
                .0
                .iter()
                .filter(|h| h.name == header::SET_COOKIE)
                .map(|h| Cookie::parse_header(&h.value))
                .try_collect()?,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub http_version: HttpVersion,
    pub status_code: StatusCode,
    pub headers: Headers,
    pub cookies: Cookies,
    pub content: Option<Content>,
}
#[derive(Debug, thiserror::Error)]
pub enum InvalidResponse {
    #[error("invalid http version")]
    Version(
        #[source]
        #[from]
        crate::HttpVersionParseErr,
    ),
    #[error("failed to parse response header")]
    Headers(
        #[source]
        #[from]
        InvalidHeader,
    ),
    #[error("failed to parse cookies")]
    Cookies(
        #[source]
        #[from]
        CookieParseError,
    ),
    #[error("invalid Content-Type header: binary data")]
    ContentType,
}
impl Response {
    pub fn parse<HK: AsRef<[u8]>, HV: AsRef<[u8]>, I: Iterator<Item = (HK, HV)>>(
        http_version: &str,
        status_code: u16,
        url: &str,
        headers: I,
        content: Option<&[u8]>,
    ) -> Result<Self, InvalidResponse> {
        let headers = Headers::parse(headers).map_err(InvalidResponse::from)?;
        Ok(Self {
            http_version: http_version.parse().map_err(InvalidResponse::from)?,
            status_code: StatusCode(status_code),
            cookies: Cookies::parse_headers(&headers).map_err(InvalidResponse::from)?,
            content: match content {
                Some([]) => None,
                Some(content) => Some(Content::from_mime(
                    url,
                    headers
                        .content_type()
                        .map_err(|_| InvalidResponse::ContentType)?,
                    content.to_vec().into_boxed_slice(),
                )),
                None => None,
            },
            headers,
        })
    }
}
