//! Envelope codecs. MessagePack is the default, JSON is always available, and
//! inbound payloads can be sniffed so peers need not agree on a codec.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::wire::{Escaping, Unescaping, may_contain_sentinel};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Codec {
    #[cfg(feature = "msgpack")]
    #[default]
    MsgPack,
    #[cfg_attr(not(feature = "msgpack"), default)]
    Json,
}

impl Codec {
    pub fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "msgpack")]
            Self::MsgPack => "msgpack",
            Self::Json => "json",
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            #[cfg(feature = "msgpack")]
            Self::MsgPack => "application/msgpack",
            Self::Json => "application/json",
        }
    }

    /// Resolves the names used in `meta.codec` (`msgpack`, `json`, `json-fast`).
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            #[cfg(feature = "msgpack")]
            "msgpack" => Some(Self::MsgPack),
            "json" | "json-fast" => Some(Self::Json),
            _ => None,
        }
    }

    /// Guesses the codec from the leading bytes of an envelope.
    pub fn detect(bytes: &[u8]) -> Option<Self> {
        let first = bytes.iter().copied().find(|b| !b.is_ascii_whitespace())?;
        match first {
            b'{' | b'[' => Some(Self::Json),
            #[cfg(feature = "msgpack")]
            0x80..=0x9f | 0xdc..=0xdf => Some(Self::MsgPack),
            _ => None,
        }
    }

    pub fn encode<T: Serialize + ?Sized>(self, value: &T) -> Result<Bytes> {
        self.encode_to_vec(value).map(Bytes::from)
    }

    pub fn encode_to_vec<T: Serialize + ?Sized>(self, value: &T) -> Result<Vec<u8>> {
        match self {
            #[cfg(feature = "msgpack")]
            Self::MsgPack => {
                let mut buf = Vec::with_capacity(128);
                let mut ser = rmp_serde::Serializer::new(&mut buf).with_struct_map();
                value
                    .serialize(Escaping(&mut ser))
                    .map_err(|e| Error::parse(format!("MessagePack encode error: {e}")))?;
                Ok(buf)
            }
            Self::Json => {
                let mut buf = Vec::with_capacity(128);
                let mut ser = serde_json::Serializer::new(&mut buf);
                value
                    .serialize(Escaping(&mut ser))
                    .map_err(|e| Error::parse(format!("JSON encode error: {e}")))?;
                Ok(buf)
            }
        }
    }

    pub fn decode<T: DeserializeOwned>(self, bytes: &[u8]) -> Result<T> {
        let escaped = may_contain_sentinel(bytes);
        match self {
            #[cfg(feature = "msgpack")]
            Self::MsgPack => {
                let mut de = rmp_serde::Deserializer::from_read_ref(bytes);
                let value = if escaped {
                    T::deserialize(Unescaping(&mut de))
                } else {
                    T::deserialize(&mut de)
                };
                value.map_err(|e| Error::parse(format!("MessagePack decode error: {e}")))
            }
            Self::Json => {
                let mut de = serde_json::Deserializer::from_slice(bytes);
                let value = if escaped {
                    T::deserialize(Unescaping(&mut de))
                } else {
                    T::deserialize(&mut de)
                };
                let value = value.map_err(|e| Error::parse(format!("JSON parse error: {e}")))?;
                de.end()
                    .map_err(|e| Error::parse(format!("JSON parse error: {e}")))?;
                Ok(value)
            }
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Codec {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_name(s).ok_or_else(|| Error::internal(format!("Codec {s:?} is not available")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::BigInt;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        id: BigInt,
        name: String,
        tags: Vec<String>,
        blob: Vec<u8>,
        weight: f64,
        missing: Option<u8>,
    }

    fn sample() -> Payload {
        Payload {
            id: BigInt::parse("9007199254740993").unwrap(),
            name: "@@nevo:not-a-bigint".into(),
            tags: vec!["a".into(), "@@nevo:str:x".into()],
            blob: vec![1, 2, 3],
            weight: 1.5,
            missing: None,
        }
    }

    #[test]
    fn json_round_trip() {
        let bytes = Codec::Json.encode(&sample()).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("\"id\":\"@@nevo:bigint:9007199254740993\""));
        assert!(text.contains("\"name\":\"@@nevo:str:@@nevo:not-a-bigint\""));
        let back: Payload = Codec::Json.decode(&bytes).unwrap();
        assert_eq!(back, sample());
        assert_eq!(Codec::detect(&bytes), Some(Codec::Json));
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_round_trip() {
        let bytes = Codec::MsgPack.encode(&sample()).unwrap();
        assert_eq!(Codec::detect(&bytes), Some(Codec::MsgPack));
        let back: Payload = Codec::MsgPack.decode(&bytes).unwrap();
        assert_eq!(back, sample());
    }

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_structs_are_maps_with_string_keys() {
        let req = crate::wire::Request::new("u", "m", serde_json::json!({"a": 1}));
        let bytes = Codec::MsgPack.encode(&req).unwrap();
        let generic: serde_json::Value = Codec::MsgPack.decode(&bytes).unwrap();
        assert_eq!(generic["uuid"], "u");
        assert_eq!(generic["params"]["a"], 1);
    }

    #[test]
    fn parse_error_is_reported_with_code() {
        let err = Codec::Json.decode::<Payload>(b"not-json").unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::ParseError);
        let err = Codec::Json.decode::<Payload>(b"{} trailing").unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::ParseError);
    }

    #[test]
    fn legacy_n_suffix_strings_stay_strings() {
        let out: serde_json::Value = Codec::Json.decode(br#"{"version":"42n"}"#).unwrap();
        assert_eq!(out["version"], "42n");
    }
}
