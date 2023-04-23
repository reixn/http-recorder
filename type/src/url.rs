use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Host {
    Domain(String),
    Addr(std::net::IpAddr),
}
mod serde_url {
    use serde::{de, Deserializer, Serializer};
    use url::Url;

    pub fn serialize<S: Serializer>(url: &Url, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(url.as_str())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Url, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = Url;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("url")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Url::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryString {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Url {
    #[serde(with = "serde_url")]
    pub url: url::Url,

    pub scheme: String,
    pub host: Option<Host>,
    pub port: Option<u16>,
    pub path: String,
    pub query: Vec<QueryString>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InvalidUrl(url::ParseError);
impl FromStr for Url {
    type Err = InvalidUrl;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let u = url::Url::parse(s).map_err(InvalidUrl)?;
        Ok(Self {
            scheme: u.scheme().to_owned(),
            host: u.host().map(|h| match h {
                url::Host::Domain(d) => Host::Domain(d.to_owned()),
                url::Host::Ipv4(a) => Host::Addr(std::net::IpAddr::V4(a)),
                url::Host::Ipv6(a) => Host::Addr(std::net::IpAddr::V6(a)),
            }),
            port: u.port(),
            path: u.path().to_owned(),
            query: u
                .query_pairs()
                .map(|(k, v)| QueryString {
                    name: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            url: u,
        })
    }
}
