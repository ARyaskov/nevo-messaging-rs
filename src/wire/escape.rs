//! Serde adapters implementing the wire-level string escaping shared with the
//! TypeScript peer: user strings that begin with `@@nevo:` travel as
//! `@@nevo:str:<original>` so they can never be mistaken for a bigint sentinel.

use std::fmt;

use serde::de::{
    DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::ser::{
    Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant, Serializer,
};

pub const SENTINEL_PREFIX: &str = "@@nevo:";
pub const STRING_ESCAPE: &str = "@@nevo:str:";
pub const BIGINT_SENTINEL: &str = "@@nevo:bigint:";
pub(crate) const BIGINT_NEWTYPE: &str = "__nevo_bigint";

/// True when the buffer may hold a sentinel and therefore needs the unescaping pass.
pub fn may_contain_sentinel(bytes: &[u8]) -> bool {
    memchr::memmem::find(bytes, SENTINEL_PREFIX.as_bytes()).is_some()
}

/// A well-formed `@@nevo:bigint:<digits>` literal. Such strings travel unescaped even
/// when they come from a dynamic value, so a [`BigInt`](super::BigInt) embedded in a
/// `serde_json::Value` still reaches the peer as a bigint.
pub fn is_bigint_sentinel(value: &str) -> bool {
    value
        .strip_prefix(BIGINT_SENTINEL)
        .map(|digits| digits.strip_prefix('-').unwrap_or(digits))
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

pub struct Escaping<S>(pub S);

struct Wrap<'a, T: ?Sized>(&'a T);

impl<T: ?Sized + Serialize> Serialize for Wrap<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(Escaping(serializer))
    }
}

pub struct EscapingCompound<S>(S);

impl<S: Serializer> Serializer for Escaping<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = EscapingCompound<S::SerializeSeq>;
    type SerializeTuple = EscapingCompound<S::SerializeTuple>;
    type SerializeTupleStruct = EscapingCompound<S::SerializeTupleStruct>;
    type SerializeTupleVariant = EscapingCompound<S::SerializeTupleVariant>;
    type SerializeMap = EscapingCompound<S::SerializeMap>;
    type SerializeStruct = EscapingCompound<S::SerializeStruct>;
    type SerializeStructVariant = EscapingCompound<S::SerializeStructVariant>;

    fn serialize_bool(self, v: bool) -> Result<S::Ok, S::Error> {
        self.0.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<S::Ok, S::Error> {
        self.0.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<S::Ok, S::Error> {
        self.0.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<S::Ok, S::Error> {
        self.0.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<S::Ok, S::Error> {
        self.0.serialize_i64(v)
    }

    fn serialize_i128(self, v: i128) -> Result<S::Ok, S::Error> {
        self.0.serialize_i128(v)
    }

    fn serialize_u8(self, v: u8) -> Result<S::Ok, S::Error> {
        self.0.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<S::Ok, S::Error> {
        self.0.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<S::Ok, S::Error> {
        self.0.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<S::Ok, S::Error> {
        self.0.serialize_u64(v)
    }

    fn serialize_u128(self, v: u128) -> Result<S::Ok, S::Error> {
        self.0.serialize_u128(v)
    }

    fn serialize_f32(self, v: f32) -> Result<S::Ok, S::Error> {
        self.0.serialize_f32(v)
    }

    fn serialize_f64(self, v: f64) -> Result<S::Ok, S::Error> {
        self.0.serialize_f64(v)
    }

    fn serialize_char(self, v: char) -> Result<S::Ok, S::Error> {
        self.0.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<S::Ok, S::Error> {
        if v.starts_with(SENTINEL_PREFIX) && !is_bigint_sentinel(v) {
            let mut escaped = String::with_capacity(STRING_ESCAPE.len() + v.len());
            escaped.push_str(STRING_ESCAPE);
            escaped.push_str(v);
            self.0.serialize_str(&escaped)
        } else {
            self.0.serialize_str(v)
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<S::Ok, S::Error> {
        self.0.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<S::Ok, S::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<S::Ok, S::Error> {
        self.0.serialize_some(&Wrap(value))
    }

    fn serialize_unit(self) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit_variant(name, index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        if name == BIGINT_NEWTYPE {
            value.serialize(self.0)
        } else {
            self.0.serialize_newtype_struct(name, &Wrap(value))
        }
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        self.0
            .serialize_newtype_variant(name, index, variant, &Wrap(value))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, S::Error> {
        self.0.serialize_seq(len).map(EscapingCompound)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, S::Error> {
        self.0.serialize_tuple(len).map(EscapingCompound)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, S::Error> {
        self.0
            .serialize_tuple_struct(name, len)
            .map(EscapingCompound)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, S::Error> {
        self.0
            .serialize_tuple_variant(name, index, variant, len)
            .map(EscapingCompound)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, S::Error> {
        self.0.serialize_map(len).map(EscapingCompound)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, S::Error> {
        self.0.serialize_struct(name, len).map(EscapingCompound)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, S::Error> {
        self.0
            .serialize_struct_variant(name, index, variant, len)
            .map(EscapingCompound)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

impl<S: SerializeSeq> SerializeSeq for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        self.0.serialize_element(&Wrap(value))
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeTuple> SerializeTuple for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        self.0.serialize_element(&Wrap(value))
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeTupleStruct> SerializeTupleStruct for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        self.0.serialize_field(&Wrap(value))
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeTupleVariant> SerializeTupleVariant for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        self.0.serialize_field(&Wrap(value))
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeMap> SerializeMap for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), S::Error> {
        self.0.serialize_key(key)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        self.0.serialize_value(&Wrap(value))
    }

    fn serialize_entry<K: ?Sized + Serialize, V: ?Sized + Serialize>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), S::Error> {
        self.0.serialize_entry(key, &Wrap(value))
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeStruct> SerializeStruct for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        self.0.serialize_field(key, &Wrap(value))
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.0.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeStructVariant> SerializeStructVariant for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        self.0.serialize_field(key, &Wrap(value))
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.0.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

pub struct Unescaping<D>(pub D);

struct UnVisitor<V>(V);

struct UnSeed<T>(T);

struct UnSeq<A>(A);

struct UnMap<A>(A);

struct UnEnum<A>(A);

struct UnVariant<A>(A);

macro_rules! forward_deserialize {
    ($($method:ident),*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
            self.0.$method(UnVisitor(visitor))
        }
    )*};
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for Unescaping<D> {
    type Error = D::Error;

    forward_deserialize!(
        deserialize_any,
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any
    );

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_unit_struct(name, UnVisitor(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_newtype_struct(name, UnVisitor(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_tuple(len, UnVisitor(visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0
            .deserialize_tuple_struct(name, len, UnVisitor(visitor))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_struct(name, fields, UnVisitor(visitor))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        self.0.deserialize_enum(name, variants, UnVisitor(visitor))
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

macro_rules! forward_visit {
    ($($method:ident($t:ty)),*) => {$(
        fn $method<E: serde::de::Error>(self, v: $t) -> Result<V::Value, E> {
            self.0.$method(v)
        }
    )*};
}

impl<'de, V: Visitor<'de>> Visitor<'de> for UnVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.expecting(f)
    }

    forward_visit!(
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>)
    );

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<V::Value, E> {
        match v.strip_prefix(STRING_ESCAPE) {
            Some(rest) => self.0.visit_str(rest),
            None => self.0.visit_str(v),
        }
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, v: &'de str) -> Result<V::Value, E> {
        match v.strip_prefix(STRING_ESCAPE) {
            Some(rest) => self.0.visit_borrowed_str(rest),
            None => self.0.visit_borrowed_str(v),
        }
    }

    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<V::Value, E> {
        match v.strip_prefix(STRING_ESCAPE) {
            Some(rest) => self.0.visit_str(rest),
            None => self.0.visit_string(v),
        }
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.0.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<V::Value, D::Error> {
        self.0.visit_some(Unescaping(deserializer))
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.0.visit_unit()
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<V::Value, D::Error> {
        self.0.visit_newtype_struct(Unescaping(deserializer))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        self.0.visit_seq(UnSeq(seq))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        self.0.visit_map(UnMap(map))
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        self.0.visit_enum(UnEnum(data))
    }
}

impl<'de, T: DeserializeSeed<'de>> DeserializeSeed<'de> for UnSeed<T> {
    type Value = T::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<T::Value, D::Error> {
        self.0.deserialize(Unescaping(deserializer))
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for UnSeq<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        self.0.next_element_seed(UnSeed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for UnMap<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        self.0.next_key_seed(seed)
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        self.0.next_value_seed(UnSeed(seed))
    }

    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}

impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for UnEnum<A> {
    type Error = A::Error;
    type Variant = UnVariant<A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self::Variant), A::Error> {
        let (value, variant) = self.0.variant_seed(seed)?;
        Ok((value, UnVariant(variant)))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for UnVariant<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.0.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        self.0.newtype_variant_seed(UnSeed(seed))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        self.0.tuple_variant(len, UnVisitor(visitor))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        self.0.struct_variant(fields, UnVisitor(visitor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::BigInt;
    use serde::{Deserialize, Serialize};

    fn to_json<T: Serialize>(value: &T) -> String {
        let mut buf = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut buf);
        value.serialize(Escaping(&mut ser)).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn from_json<T: for<'de> Deserialize<'de>>(json: &str) -> T {
        let mut de = serde_json::Deserializer::from_str(json);
        T::deserialize(Unescaping(&mut de)).unwrap()
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Doc {
        plain: String,
        tricky: String,
        id: BigInt,
        nested: Vec<Option<String>>,
        map: std::collections::BTreeMap<String, String>,
        kind: Kind,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Kind {
        Unit,
        Named { label: String },
        Tuple(String, u8),
    }

    #[test]
    fn escapes_only_sentinel_prefixed_values_and_never_keys() {
        let mut map = std::collections::BTreeMap::new();
        map.insert("@@nevo:key".to_owned(), "@@nevo:value".to_owned());
        let doc = Doc {
            plain: "hello".into(),
            tricky: "@@nevo:bigint:x".into(),
            id: BigInt::from(1u8),
            nested: vec![Some("@@nevo:x".into()), None],
            map,
            kind: Kind::Named {
                label: "@@nevo:inner".into(),
            },
        };
        let json = to_json(&doc);
        assert!(json.contains("\"tricky\":\"@@nevo:str:@@nevo:bigint:x\""));
        assert!(json.contains("\"id\":\"@@nevo:bigint:1\""));
        assert!(json.contains("\"@@nevo:key\":\"@@nevo:str:@@nevo:value\""));
        assert!(json.contains("\"label\":\"@@nevo:str:@@nevo:inner\""));
        let back: Doc = from_json(&json);
        assert_eq!(back, doc);
    }

    #[test]
    fn unescapes_enum_variants_and_tuples() {
        let json = to_json(&Kind::Tuple("@@nevo:t".into(), 3));
        assert!(json.contains("@@nevo:str:@@nevo:t"));
        assert_eq!(from_json::<Kind>(&json), Kind::Tuple("@@nevo:t".into(), 3));
        assert_eq!(from_json::<Kind>("\"Unit\""), Kind::Unit);
    }

    #[test]
    fn dynamic_bigint_sentinels_pass_through() {
        let value = serde_json::json!({"id": BigInt::from(7u8), "neg": "@@nevo:bigint:-3", "text": "@@nevo:bigint:"});
        let json = to_json(&value);
        assert!(json.contains("\"id\":\"@@nevo:bigint:7\""));
        assert!(json.contains("\"neg\":\"@@nevo:bigint:-3\""));
        assert!(json.contains("\"text\":\"@@nevo:str:@@nevo:bigint:\""));
        assert!(is_bigint_sentinel("@@nevo:bigint:0"));
        assert!(!is_bigint_sentinel("@@nevo:bigint:1n"));
        assert!(!is_bigint_sentinel("@@nevo:str:1"));
    }

    #[test]
    fn sentinel_probe() {
        assert!(!may_contain_sentinel(b"{\"a\":1}"));
        assert!(may_contain_sentinel(b"{\"a\":\"@@nevo:bigint:1\"}"));
    }
}
