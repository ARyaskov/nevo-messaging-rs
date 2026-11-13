//! A structural hash of the `params` member of an envelope, computed while
//! streaming through the codec so no intermediate value tree is built.

use std::fmt;

use serde::Deserialize;
use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use sha2::{Digest, Sha256};

use crate::codec::Codec;

const TAG_BOOL: u8 = 1;
const TAG_NEG: u8 = 2;
const TAG_UINT: u8 = 3;
const TAG_NEG128: u8 = 4;
const TAG_UINT128: u8 = 5;
const TAG_FLOAT: u8 = 6;
const TAG_STR: u8 = 7;
const TAG_BYTES: u8 = 8;
const TAG_NULL: u8 = 9;
const TAG_SEQ: u8 = 10;
const TAG_SEQ_END: u8 = 11;
const TAG_MAP: u8 = 12;
const TAG_MAP_END: u8 = 13;
const TAG_VARIANT: u8 = 14;

struct HashVisitor;

struct HashSeed;

impl<'de> DeserializeSeed<'de> for HashSeed {
    // Each seed hashes its subtree on its own hasher and returns the digest, so
    // map entries can be reordered canonically before their parent hashes them.
    type Value = Vec<u8>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_any(HashVisitor)
    }
}

fn digest_of(update: impl FnOnce(&mut Sha256)) -> Vec<u8> {
    let mut hasher = Sha256::new();
    update(&mut hasher);
    hasher.finalize().to_vec()
}

impl<'de> Visitor<'de> for HashVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| h.update([TAG_BOOL, u8::from(v)])))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Vec<u8>, E> {
        if v >= 0 {
            return self.visit_u64(v as u64);
        }
        Ok(digest_of(|h| {
            h.update([TAG_NEG]);
            h.update(v.to_le_bytes());
        }))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| {
            h.update([TAG_UINT]);
            h.update(v.to_le_bytes());
        }))
    }

    fn visit_i128<E: de::Error>(self, v: i128) -> Result<Vec<u8>, E> {
        if let Ok(small) = i64::try_from(v) {
            return self.visit_i64(small);
        }
        if let Ok(unsigned) = u64::try_from(v) {
            return self.visit_u64(unsigned);
        }
        Ok(digest_of(|h| {
            h.update([TAG_NEG128]);
            h.update(v.to_le_bytes());
        }))
    }

    fn visit_u128<E: de::Error>(self, v: u128) -> Result<Vec<u8>, E> {
        if let Ok(small) = u64::try_from(v) {
            return self.visit_u64(small);
        }
        Ok(digest_of(|h| {
            h.update([TAG_UINT128]);
            h.update(v.to_le_bytes());
        }))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Vec<u8>, E> {
        if v.is_finite() && v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 {
            return self.visit_i64(v as i64);
        }
        Ok(digest_of(|h| {
            h.update([TAG_FLOAT]);
            h.update(v.to_bits().to_le_bytes());
        }))
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Vec<u8>, E> {
        self.visit_f64(f64::from(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| {
            h.update([TAG_STR]);
            h.update((v.len() as u64).to_le_bytes());
            h.update(v.as_bytes());
        }))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| {
            h.update([TAG_BYTES]);
            h.update((v.len() as u64).to_le_bytes());
            h.update(v);
        }))
    }

    fn visit_none<E: de::Error>(self) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| h.update([TAG_NULL])))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Vec<u8>, E> {
        Ok(digest_of(|h| h.update([TAG_NULL])))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element_seed(HashSeed)? {
            items.push(item);
        }
        Ok(digest_of(|h| {
            h.update([TAG_SEQ]);
            for item in &items {
                h.update(item);
            }
            h.update([TAG_SEQ_END]);
        }))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Vec<u8>, A::Error> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while let Some(key) = map.next_key_seed(HashSeed)? {
            let value = map.next_value_seed(HashSeed)?;
            entries.push((key, value));
        }
        // Canonical order: logically equal payloads whose map keys arrive in a
        // different order must produce the same fingerprint.
        entries.sort();
        Ok(digest_of(|h| {
            h.update([TAG_MAP]);
            for (key, value) in &entries {
                h.update(key);
                h.update(value);
            }
            h.update([TAG_MAP_END]);
        }))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Vec<u8>, A::Error> {
        let (variant, rest) = data.variant_seed(HashSeed)?;
        let value = rest.newtype_variant_seed(HashSeed)?;
        Ok(digest_of(|h| {
            h.update([TAG_VARIANT]);
            h.update(&variant);
            h.update(&value);
        }))
    }
}

/// Hex digest of the structural content of a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint(pub String);

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let digest = deserializer.deserialize_any(HashVisitor)?;
        Ok(Self(
            digest.iter().take(16).map(|b| format!("{b:02x}")).collect(),
        ))
    }
}

#[derive(Deserialize)]
struct ParamsOnly {
    params: Fingerprint,
}

/// Decodes without the wire's `@@nevo:str:` unescaping pass. The fingerprint
/// hashes the params exactly as they sit on the wire, so a typed `BigInt`'s
/// unescaped `@@nevo:bigint:<digits>` sentinel and a user string carrying that
/// same literal (wire form `@@nevo:str:...`) never collide.
fn raw_decode<D: serde::de::DeserializeOwned>(codec: Codec, raw: &[u8]) -> Option<D> {
    match codec {
        #[cfg(feature = "msgpack")]
        Codec::MsgPack => rmp_serde::from_slice(raw).ok(),
        Codec::Json => serde_json::from_slice(raw).ok(),
    }
}

/// Fingerprints the `params` member of a raw request envelope.
pub fn of_params(codec: Codec, raw: &[u8]) -> Option<String> {
    raw_decode::<ParamsOnly>(codec, raw).map(|p| p.params.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Request;
    use serde_json::json;

    fn fp(codec: Codec, params: serde_json::Value) -> String {
        let raw = codec.encode(&Request::new("u", "m", params)).unwrap();
        of_params(codec, &raw).unwrap()
    }

    #[test]
    fn same_logical_payload_matches_across_codecs() {
        let value = json!({"id": 5, "tags": ["a", "b"], "nested": {"x": null, "f": 1.5}});
        let json = fp(Codec::Json, value.clone());
        #[cfg(feature = "msgpack")]
        assert_eq!(json, fp(Codec::MsgPack, value.clone()));
        assert_eq!(
            json,
            fp(
                Codec::Json,
                json!({"id": 5.0, "tags": ["a", "b"], "nested": {"x": null, "f": 1.5}})
            )
        );
        assert_ne!(
            json,
            fp(
                Codec::Json,
                json!({"id": 6, "tags": ["a", "b"], "nested": {"x": null, "f": 1.5}})
            )
        );
        assert_ne!(
            json,
            fp(
                Codec::Json,
                json!({"id": 5, "tags": ["a", "b", "c"], "nested": {"x": null, "f": 1.5}})
            )
        );
        assert_eq!(json.len(), 32);
    }

    #[test]
    fn map_key_order_does_not_change_the_fingerprint() {
        // Raw envelopes with the same params members in different key orders.
        let ordered = br#"{"uuid":"u","method":"m","params":{"a":1,"b":2,"c":{"x":1,"y":2}}}"#;
        let swapped = br#"{"uuid":"u","method":"m","params":{"c":{"y":2,"x":1},"b":2,"a":1}}"#;
        let a = of_params(Codec::Json, ordered).unwrap();
        let b = of_params(Codec::Json, swapped).unwrap();
        assert_eq!(a, b);
        // A differently valued payload must still be distinguishable.
        let changed = br#"{"uuid":"u","method":"m","params":{"a":1,"b":3,"c":{"x":1,"y":2}}}"#;
        assert_ne!(a, of_params(Codec::Json, changed).unwrap());
    }

    /// The client edge case: a typed `BigInt(5)` and a user string carrying the
    /// literal `@@nevo:bigint:5` must produce different fingerprints on both codecs.
    #[test]
    fn typed_bigint_and_literal_sentinel_params_fingerprint_differ() {
        #[derive(serde::Serialize)]
        struct Typed {
            id: crate::wire::BigInt,
        }
        #[derive(serde::Serialize)]
        struct Literal {
            id: String,
        }
        let typed = |codec: Codec| {
            codec
                .encode(&Request::new(
                    "u",
                    "m@v1",
                    &Typed {
                        id: crate::wire::BigInt::from(5u8),
                    },
                ))
                .unwrap()
        };
        let literal = |codec: Codec| {
            codec
                .encode(&Request::new(
                    "u",
                    "m@v1",
                    &Literal {
                        id: "@@nevo:bigint:5".into(),
                    },
                ))
                .unwrap()
        };
        let check = |codec: Codec| {
            let a = of_params(codec, &typed(codec)).unwrap();
            let b = of_params(codec, &literal(codec)).unwrap();
            assert_ne!(
                a, b,
                "typed BigInt(5) collided with its literal string on {codec}"
            );
        };
        check(Codec::Json);
        #[cfg(feature = "msgpack")]
        check(Codec::MsgPack);
    }
}
