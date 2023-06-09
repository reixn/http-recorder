use serde::{Deserialize, Serialize};

mod serde_mime {
    use mime::Mime;
    use serde::{de, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Mime, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(value.to_string().as_str())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Mime, D::Error> {
        struct MimeVisitor;
        impl<'de> de::Visitor<'de> for MimeVisitor {
            type Value = Mime;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("mime")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_str(MimeVisitor)
    }
}
mod serde_data {
    use serde::{Deserializer, Serialize, Serializer};
    pub type Value = Option<Box<[u8]>>;

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            Value::None.serialize(serializer)
        } else {
            serde_bytes::serialize(value, serializer)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        serde_bytes::deserialize(deserializer)
    }
}
mod serde_digest {
    use std::fmt::Display;

    use serde::{de, Deserializer, Serializer};

    pub fn serialize<const N: usize, S: Serializer>(
        value: &[u8; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            hex::serde::serialize(value, serializer)
        } else {
            serializer.serialize_bytes(value)
        }
    }
    pub fn deserialize<'de, const N: usize, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u8; N], D::Error>
    where
        [u8; N]: hex::FromHex,
        <[u8; N] as hex::FromHex>::Error: Display,
    {
        if deserializer.is_human_readable() {
            hex::serde::deserialize(deserializer)
        } else {
            struct Visit<const N: usize>;
            impl<'de, const N: usize> de::Visitor<'de> for Visit<N> {
                type Value = [u8; N];
                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    write!(formatter, "{} bit hash", N)
                }
                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    v.try_into().map_err(E::custom)
                }
            }
            deserializer.deserialize_bytes(Visit)
        }
    }
}

pub const SHA256_OUTPUT_SIZE: usize = 32;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SHA256Digest(#[serde(with = "serde_digest")] pub [u8; SHA256_OUTPUT_SIZE]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "algo", content = "hash")]
pub enum Digest {
    SHA256(SHA256Digest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Content {
    #[serde(with = "serde_mime")]
    pub content_type: mime::Mime,
    pub digest: Digest,
    pub extension: Option<String>,
    pub size: u64,
    #[serde(with = "serde_data")]
    pub data: Option<Box<[u8]>>,
}
impl Content {
    pub fn from_mime<CT: AsRef<str>>(url: &str, content_type: Option<CT>, data: Box<[u8]>) -> Self {
        use mime_sniffer::MimeTypeSnifferExt;
        use sha2::{digest::FixedOutput, Digest, Sha256};
        let content_type = match content_type {
            Some(ct) => mime_sniffer::HttpRequest {
                url: &url,
                content: &data,
                type_hint: ct.as_ref(),
            }
            .sniff_mime_type_ext(),
            None => mime_sniffer::HttpRequest {
                url: &url,
                content: &data,
                type_hint: mime::APPLICATION_OCTET_STREAM.as_ref(),
            }
            .sniff_mime_type_ext(),
        }
        .unwrap_or(mime::APPLICATION_OCTET_STREAM);
        Self {
            digest: {
                self::Digest::SHA256(SHA256Digest(
                    Sha256::new_with_prefix(data.as_ref())
                        .finalize_fixed()
                        .into(),
                ))
            },
            extension: mime2ext::mime2ext(&content_type).map(|v| v.to_string()),
            content_type,
            size: data.len() as u64,
            data: Some(data),
        }
    }
}
