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

struct HashVisitor<'h>(&'h mut Sha256);

struct HashSeed<'h>(&'h mut Sha256);

impl<'de> DeserializeSeed<'de> for HashSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(HashVisitor(self.0))
    }
}

impl<'de> Visitor<'de> for HashVisitor<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<(), E> {
        self.0.update([TAG_BOOL, u8::from(v)]);
        Ok(())
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<(), E> {
        if v >= 0 {
            self.visit_u64(v as u64)
        } else {
            self.0.update([TAG_NEG]);
            self.0.update(v.to_le_bytes());
            Ok(())
        }
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<(), E> {
        self.0.update([TAG_UINT]);
        self.0.update(v.to_le_bytes());
        Ok(())
    }

    fn visit_i128<E: de::Error>(self, v: i128) -> Result<(), E> {
        if let Ok(small) = i64::try_from(v) {
            return self.visit_i64(small);
        }
        if let Ok(unsigned) = u64::try_from(v) {
            return self.visit_u64(unsigned);
        }
        self.0.update([TAG_NEG128]);
        self.0.update(v.to_le_bytes());
        Ok(())
    }

    fn visit_u128<E: de::Error>(self, v: u128) -> Result<(), E> {
        if let Ok(small) = u64::try_from(v) {
            return self.visit_u64(small);
        }
        self.0.update([TAG_UINT128]);
        self.0.update(v.to_le_bytes());
        Ok(())
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<(), E> {
        if v.is_finite() && v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 {
            return self.visit_i64(v as i64);
        }
        self.0.update([TAG_FLOAT]);
        self.0.update(v.to_bits().to_le_bytes());
        Ok(())
    }

    fn visit_f32<E: de::Error>(self, v: f32) -> Result<(), E> {
        self.visit_f64(f64::from(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<(), E> {
        self.0.update([TAG_STR]);
        self.0.update((v.len() as u64).to_le_bytes());
        self.0.update(v.as_bytes());
        Ok(())
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<(), E> {
        self.0.update([TAG_BYTES]);
        self.0.update((v.len() as u64).to_le_bytes());
        self.0.update(v);
        Ok(())
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        self.0.update([TAG_NULL]);
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.0.update([TAG_NULL]);
        Ok(())
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        self.0.update([TAG_SEQ]);
        while seq.next_element_seed(HashSeed(self.0))?.is_some() {}
        self.0.update([TAG_SEQ_END]);
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        self.0.update([TAG_MAP]);
        while map.next_key_seed(HashSeed(self.0))?.is_some() {
            map.next_value_seed(HashSeed(self.0))?;
        }
        self.0.update([TAG_MAP_END]);
        Ok(())
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<(), A::Error> {
        self.0.update([TAG_VARIANT]);
        let ((), variant) = data.variant_seed(HashSeed(self.0))?;
        variant.newtype_variant_seed(HashSeed(self.0))
    }
}

/// Hex digest of the structural content of a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint(pub String);

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut hasher = Sha256::new();
        deserializer.deserialize_any(HashVisitor(&mut hasher))?;
        let digest = hasher.finalize();
        Ok(Self(
            digest.iter().take(16).map(|b| format!("{b:02x}")).collect(),
        ))
    }
}

#[derive(Deserialize)]
struct ParamsOnly {
    params: Fingerprint,
}

/// Fingerprints the `params` member of a raw request envelope.
pub fn of_params(codec: Codec, raw: &[u8]) -> Option<String> {
    codec.decode::<ParamsOnly>(raw).ok().map(|p| p.params.0)
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
}
