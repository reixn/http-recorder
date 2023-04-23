use crate::{
    content::{data::DataMap, Content},
    header::{self, Headers, InvalidHeader},
    url::Url,
    HttpVersion, Method,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cookie {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cookies(pub Vec<Cookie>);
#[derive(Debug, thiserror::Error)]
pub enum CookiesParseError {
    #[error("invalue cookie header: binary value")]
    BinaryHeader,
    #[error("failed to parse cookie")]
    ParseError(
        #[source]
        #[from]
        cookie::ParseError,
    ),
}
impl Cookies {
    pub fn parse(headers: &Headers) -> Result<Self, CookiesParseError> {
        let mut ret = Vec::new();
        for h in headers.0.iter() {
            if h.name == header::COOKIE {
                let s = match &h.value {
                    header::HeaderValue::Text(s) => s.as_str(),
                    header::HeaderValue::Binary(_) => {
                        return Err(CookiesParseError::BinaryHeader);
                    }
                };
                for c in s.split("; ") {
                    let cok = cookie::Cookie::parse_encoded(c).map_err(CookiesParseError::from)?;
                    ret.push(Cookie {
                        name: cok.name().to_owned(),
                        value: cok.value().to_owned(),
                    });
                }
            }
        }
        Ok(Self(ret))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlEncodedFormEntry {
    pub name: String,
    pub value: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartFormEntry {
    pub name: Option<String>,
    pub file_name: Option<String>,
    pub headers: Headers,
    pub content: Content,
}
#[derive(Debug, thiserror::Error)]
pub enum MultipartFormParseError {
    #[error("failed to parse boundary")]
    Boundary(#[source] multer::Error),
    #[error("failed to parse field")]
    Field(multer::Error),
    #[error("failed to get bytes of field {idx}")]
    Bytes {
        idx: usize,
        #[source]
        source: multer::Error,
    },
}
impl MultipartFormEntry {
    pub fn parse(
        url: &str,
        content_type: &str,
        data: &[u8],
    ) -> Result<Vec<Self>, MultipartFormParseError> {
        let mut parser = multer::Multipart::new::<_, _, std::convert::Infallible, _>(
            futures_util::stream::once(std::future::ready(Ok(bytes::Bytes::copy_from_slice(data)))),
            multer::parse_boundary(content_type).map_err(MultipartFormParseError::Boundary)?,
        );
        let mut ret = Vec::new();
        while let Some((idx, f)) = futures::executor::block_on(parser.next_field_with_idx())
            .map_err(MultipartFormParseError::Field)?
        {
            ret.push(Self {
                name: f.name().map(str::to_string),
                file_name: f.file_name().map(str::to_string),
                headers: Headers(
                    f.headers()
                        .iter()
                        .map(|(k, v)| header::Header {
                            name: header::HeaderName::from_lower(k.as_str()),
                            value: match v.to_str() {
                                Ok(s) => header::HeaderValue::Text(s.to_string()),
                                Err(_) => header::HeaderValue::Binary(
                                    v.as_bytes().to_vec().into_boxed_slice(),
                                ),
                            },
                        })
                        .collect(),
                ),
                content: Content::from_mime(
                    url,
                    f.content_type().cloned(),
                    futures::executor::block_on(f.bytes())
                        .map_err(|e| MultipartFormParseError::Bytes { idx, source: e })?
                        .to_vec()
                        .into_boxed_slice(),
                ),
            })
        }
        Ok(ret)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Body {
    Content(Content),
    UrlEncodedForm(Vec<UrlEncodedFormEntry>),
    MultipartForm(Vec<MultipartFormEntry>),
}
#[derive(Debug, thiserror::Error)]
pub enum BodyParseError {
    #[error("invalud Content-Type header: binary data")]
    BinaryHeader,
    #[error("failed to parse Content-Type")]
    Mime(
        #[from]
        #[source]
        mime::FromStrError,
    ),
    #[error("failed to parse multipart form")]
    MultipartForm(
        #[from]
        #[source]
        MultipartFormParseError,
    ),
}
impl Body {
    pub fn parse(url: &str, headers: &Headers, content: &[u8]) -> Result<Self, BodyParseError> {
        let content_type = headers
            .content_type()
            .map_err(|_| BodyParseError::BinaryHeader)?;
        if let Some(content_type_str) = content_type {
            let content_type: mime::Mime =
                content_type_str.parse().map_err(BodyParseError::from)?;
            if content_type == mime::APPLICATION_WWW_FORM_URLENCODED {
                return Ok(Body::UrlEncodedForm(
                    url::form_urlencoded::parse(content)
                        .map(|(k, v)| UrlEncodedFormEntry {
                            name: k.to_string(),
                            value: v.to_string(),
                        })
                        .collect(),
                ));
            } else if content_type == mime::MULTIPART_FORM_DATA {
                return Ok(Body::MultipartForm(MultipartFormEntry::parse(
                    url,
                    content_type_str,
                    content,
                )?));
            }
        }
        Ok(Self::Content(Content::from_mime(
            url,
            content_type,
            content.to_owned().into_boxed_slice(),
        )))
    }
    pub(crate) fn take_data<'a: 'b, 'b>(&'a self, data: &mut DataMap<'b>) {
        match self {
            Self::Content(c) => c.take_data(data),
            Self::MultipartForm(f) => {
                f.iter().for_each(|v| v.content.take_data(data));
            }
            _ => (),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub http_version: HttpVersion,
    pub method: Method,
    pub url: Url,
    pub headers: Headers,
    pub cookies: Cookies,
    pub body: Option<Body>,
}
#[derive(Debug, thiserror::Error)]
pub enum InvalidRequest {
    #[error("invalid http version")]
    Version(
        #[source]
        #[from]
        super::HttpVersionParseErr,
    ),
    #[error("invalid header")]
    Headers(
        #[source]
        #[from]
        InvalidHeader,
    ),
    #[error("invalid url")]
    Url(
        #[source]
        #[from]
        crate::url::InvalidUrl,
    ),
    #[error("failed to parse cookies")]
    Cookies(
        #[source]
        #[from]
        CookiesParseError,
    ),
    #[error("failed to parse body")]
    Body(
        #[source]
        #[from]
        BodyParseError,
    ),
}

impl Request {
    pub fn parse<HK: AsRef<[u8]>, HV: AsRef<[u8]>, I: Iterator<Item = (HK, HV)>>(
        http_version: &str,
        method: &str,
        url: &str,
        headers: I,
        content: Option<&[u8]>,
    ) -> Result<Self, InvalidRequest> {
        let headers = Headers::parse(headers).map_err(InvalidRequest::from)?;
        Ok(Self {
            http_version: http_version.parse().map_err(InvalidRequest::from)?,
            method: method.parse().unwrap(),
            url: url.parse().map_err(InvalidRequest::from)?,
            cookies: Cookies::parse(&headers).map_err(InvalidRequest::from)?,
            body: match content {
                Some([]) => None,
                Some(content) => {
                    Some(Body::parse(url, &headers, content).map_err(InvalidRequest::from)?)
                }
                None => None,
            },
            headers,
        })
    }
}
