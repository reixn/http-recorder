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
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub type Value = Option<Box<[u8]>>;

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            Value::None.serialize(serializer)
        } else {
            value.serialize(serializer)
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        if deserializer.is_human_readable() {
            Ok(None)
        } else {
            Value::deserialize(deserializer)
        }
    }
}

pub(crate) mod data;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SHA256Digest(#[serde(with = "hex::serde")] pub [u8; 32]);
pub const SHA256_OUTPUT_SIZE: usize = 32;

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
            data: Some(data),
        }
    }
    pub(crate) fn take_data<'a: 'b, 'b>(&'a self, data: &mut data::DataMap<'b>) {
        if let Some(d) = &self.data {
            let Digest::SHA256(v) = &self.digest;
            data.sha256.push(data::Data {
                digest: &v.0,
                extension: self.extension.as_ref().map(|v| v.as_str()),
                data: d.as_ref(),
            });
        }
    }
}
