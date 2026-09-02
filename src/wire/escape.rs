//! Serde adapters implementing the wire-level value model shared with the
//! TypeScript peer: user strings that begin with `@@nevo:` travel as
//! `@@nevo:str:<original>` so they can never be mistaken for a bigint sentinel,
//! and every codec presents itself as human-readable so types such as
//! `uuid::Uuid` or `chrono` timestamps take their string form, which is what a
//! JavaScript peer expects from MessagePack as much as from JSON.

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

/// Wraps a serializer: escapes sentinel-like strings (values only, never map keys)
/// and reports the wire as human-readable.
pub struct Escaping<S> {
    inner: S,
    escape: bool,
}

impl<S> Escaping<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            escape: true,
        }
    }
}

struct Wrap<'a, T: ?Sized> {
    value: &'a T,
    escape: bool,
}

impl<T: ?Sized + Serialize> Serialize for Wrap<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(Escaping {
            inner: serializer,
            escape: self.escape,
        })
    }
}

pub struct EscapingCompound<S> {
    inner: S,
    escape: bool,
}

impl<S: Serializer> Escaping<S> {
    fn wrap<'a, T: ?Sized>(&self, value: &'a T) -> Wrap<'a, T> {
        Wrap {
            value,
            escape: self.escape,
        }
    }
}

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
        self.inner.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<S::Ok, S::Error> {
        self.inner.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<S::Ok, S::Error> {
        self.inner.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<S::Ok, S::Error> {
        self.inner.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<S::Ok, S::Error> {
        self.inner.serialize_i64(v)
    }

    fn serialize_i128(self, v: i128) -> Result<S::Ok, S::Error> {
        self.inner.serialize_i128(v)
    }

    fn serialize_u8(self, v: u8) -> Result<S::Ok, S::Error> {
        self.inner.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<S::Ok, S::Error> {
        self.inner.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<S::Ok, S::Error> {
        self.inner.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<S::Ok, S::Error> {
        self.inner.serialize_u64(v)
    }

    fn serialize_u128(self, v: u128) -> Result<S::Ok, S::Error> {
        self.inner.serialize_u128(v)
    }

    fn serialize_f32(self, v: f32) -> Result<S::Ok, S::Error> {
        self.inner.serialize_f32(v)
    }

    fn serialize_f64(self, v: f64) -> Result<S::Ok, S::Error> {
        self.inner.serialize_f64(v)
    }

    fn serialize_char(self, v: char) -> Result<S::Ok, S::Error> {
        self.inner.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<S::Ok, S::Error> {
        if self.escape && v.starts_with(SENTINEL_PREFIX) && !is_bigint_sentinel(v) {
            let mut escaped = String::with_capacity(STRING_ESCAPE.len() + v.len());
            escaped.push_str(STRING_ESCAPE);
            escaped.push_str(v);
            self.inner.serialize_str(&escaped)
        } else {
            self.inner.serialize_str(v)
        }
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<S::Ok, S::Error> {
        self.inner.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<S::Ok, S::Error> {
        self.inner.serialize_none()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<S::Ok, S::Error> {
        let wrapped = self.wrap(value);
        self.inner.serialize_some(&wrapped)
    }

    fn serialize_unit(self) -> Result<S::Ok, S::Error> {
        self.inner.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<S::Ok, S::Error> {
        self.inner.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<S::Ok, S::Error> {
        self.inner.serialize_unit_variant(name, index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        if name == BIGINT_NEWTYPE {
            value.serialize(self.inner)
        } else {
            let wrapped = self.wrap(value);
            self.inner.serialize_newtype_struct(name, &wrapped)
        }
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        let wrapped = self.wrap(value);
        self.inner
            .serialize_newtype_variant(name, index, variant, &wrapped)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_seq(len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_tuple(len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_tuple_struct(name, len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_tuple_variant(name, index, variant, len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_map(len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_struct(name, len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, S::Error> {
        let escape = self.escape;
        self.inner
            .serialize_struct_variant(name, index, variant, len)
            .map(|inner| EscapingCompound { inner, escape })
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

impl<S> EscapingCompound<S> {
    fn value<'a, T: ?Sized>(&self, value: &'a T) -> Wrap<'a, T> {
        Wrap {
            value,
            escape: self.escape,
        }
    }

    fn key<'a, T: ?Sized>(&self, key: &'a T) -> Wrap<'a, T> {
        Wrap {
            value: key,
            escape: false,
        }
    }
}

impl<S: SerializeSeq> SerializeSeq for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        let wrapped = self.value(value);
        self.inner.serialize_element(&wrapped)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: SerializeTuple> SerializeTuple for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        let wrapped = self.value(value);
        self.inner.serialize_element(&wrapped)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: SerializeTupleStruct> SerializeTupleStruct for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        let wrapped = self.value(value);
        self.inner.serialize_field(&wrapped)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: SerializeTupleVariant> SerializeTupleVariant for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        let wrapped = self.value(value);
        self.inner.serialize_field(&wrapped)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

impl<S: SerializeMap> SerializeMap for EscapingCompound<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), S::Error> {
        let wrapped = self.key(key);
        self.inner.serialize_key(&wrapped)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        let wrapped = self.value(value);
        self.inner.serialize_value(&wrapped)
    }

    fn serialize_entry<K: ?Sized + Serialize, V: ?Sized + Serialize>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), S::Error> {
        let key = self.key(key);
        let value = self.value(value);
        self.inner.serialize_entry(&key, &value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
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
        let wrapped = self.value(value);
        self.inner.serialize_field(key, &wrapped)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.inner.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
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
        let wrapped = self.value(value);
        self.inner.serialize_field(key, &wrapped)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.inner.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.inner.end()
    }
}

/// Wraps a deserializer: restores escaped strings (values only, never map keys)
/// and reports the wire as human-readable.
pub struct Unescaping<D> {
    inner: D,
    unescape: bool,
}

impl<D> Unescaping<D> {
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            unescape: true,
        }
    }
}

struct UnVisitor<V> {
    inner: V,
    unescape: bool,
}

struct UnSeed<T> {
    inner: T,
    unescape: bool,
}

struct UnSeq<A> {
    inner: A,
    unescape: bool,
}

struct UnMap<A> {
    inner: A,
    unescape: bool,
}

struct UnEnum<A> {
    inner: A,
    unescape: bool,
}

struct UnVariant<A> {
    inner: A,
    unescape: bool,
}

impl<D> Unescaping<D> {
    fn visitor<V>(&self, inner: V) -> UnVisitor<V> {
        UnVisitor {
            inner,
            unescape: self.unescape,
        }
    }
}

macro_rules! forward_deserialize {
    ($($method:ident),*) => {$(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
            let visitor = self.visitor(visitor);
            self.inner.$method(visitor)
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
        let visitor = self.visitor(visitor);
        self.inner.deserialize_unit_struct(name, visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let visitor = self.visitor(visitor);
        self.inner.deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let visitor = self.visitor(visitor);
        self.inner.deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let visitor = self.visitor(visitor);
        self.inner.deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let visitor = self.visitor(visitor);
        self.inner.deserialize_struct(name, fields, visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let visitor = self.visitor(visitor);
        self.inner.deserialize_enum(name, variants, visitor)
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

impl<V> UnVisitor<V> {
    fn nested<D>(&self, inner: D) -> Unescaping<D> {
        Unescaping {
            inner,
            unescape: self.unescape,
        }
    }
}

macro_rules! forward_visit {
    ($($method:ident($t:ty)),*) => {$(
        fn $method<E: serde::de::Error>(self, v: $t) -> Result<V::Value, E> {
            self.inner.$method(v)
        }
    )*};
}

impl<'de, V: Visitor<'de>> Visitor<'de> for UnVisitor<V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
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
        match v.strip_prefix(STRING_ESCAPE).filter(|_| self.unescape) {
            Some(rest) => self.inner.visit_str(rest),
            None => self.inner.visit_str(v),
        }
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, v: &'de str) -> Result<V::Value, E> {
        match v.strip_prefix(STRING_ESCAPE).filter(|_| self.unescape) {
            Some(rest) => self.inner.visit_borrowed_str(rest),
            None => self.inner.visit_borrowed_str(v),
        }
    }

    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<V::Value, E> {
        match v.strip_prefix(STRING_ESCAPE).filter(|_| self.unescape) {
            Some(rest) => self.inner.visit_str(rest),
            None => self.inner.visit_string(v),
        }
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.inner.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<V::Value, D::Error> {
        let nested = self.nested(deserializer);
        self.inner.visit_some(nested)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.inner.visit_unit()
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<V::Value, D::Error> {
        let nested = self.nested(deserializer);
        self.inner.visit_newtype_struct(nested)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        self.inner.visit_seq(UnSeq {
            inner: seq,
            unescape: self.unescape,
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        self.inner.visit_map(UnMap {
            inner: map,
            unescape: self.unescape,
        })
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        self.inner.visit_enum(UnEnum {
            inner: data,
            unescape: self.unescape,
        })
    }
}

impl<'de, T: DeserializeSeed<'de>> DeserializeSeed<'de> for UnSeed<T> {
    type Value = T::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<T::Value, D::Error> {
        self.inner.deserialize(Unescaping {
            inner: deserializer,
            unescape: self.unescape,
        })
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for UnSeq<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        self.inner.next_element_seed(UnSeed {
            inner: seed,
            unescape: self.unescape,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for UnMap<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        self.inner.next_key_seed(UnSeed {
            inner: seed,
            unescape: false,
        })
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        self.inner.next_value_seed(UnSeed {
            inner: seed,
            unescape: self.unescape,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for UnEnum<A> {
    type Error = A::Error;
    type Variant = UnVariant<A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self::Variant), A::Error> {
        let (value, variant) = self.inner.variant_seed(UnSeed {
            inner: seed,
            unescape: false,
        })?;
        Ok((
            value,
            UnVariant {
                inner: variant,
                unescape: self.unescape,
            },
        ))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for UnVariant<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        self.inner.newtype_variant_seed(UnSeed {
            inner: seed,
            unescape: self.unescape,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        self.inner.tuple_variant(
            len,
            UnVisitor {
                inner: visitor,
                unescape: self.unescape,
            },
        )
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        self.inner.struct_variant(
            fields,
            UnVisitor {
                inner: visitor,
                unescape: self.unescape,
            },
        )
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
        value.serialize(Escaping::new(&mut ser)).unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn from_json<T: for<'de> Deserialize<'de>>(json: &str) -> T {
        let mut de = serde_json::Deserializer::from_str(json);
        T::deserialize(Unescaping::new(&mut de)).unwrap()
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

    #[cfg(feature = "msgpack")]
    #[test]
    fn msgpack_is_presented_as_human_readable_for_values_and_keys() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Ids {
            id: uuid::Uuid,
            by_id: std::collections::BTreeMap<uuid::Uuid, String>,
            addr: std::net::IpAddr,
        }
        let id = uuid::Uuid::new_v4();
        let doc = Ids {
            id,
            by_id: [(id, "@@nevo:v".to_owned())].into_iter().collect(),
            addr: "10.0.0.1".parse().unwrap(),
        };

        let mut buf = Vec::new();
        let mut ser = rmp_serde::Serializer::new(&mut buf).with_struct_map();
        doc.serialize(Escaping::new(&mut ser)).unwrap();

        let generic: serde_json::Value = rmp_serde::from_slice(&buf).unwrap();
        assert_eq!(generic["id"], id.to_string());
        assert_eq!(generic["addr"], "10.0.0.1");
        assert_eq!(generic["by_id"][id.to_string()], "@@nevo:str:@@nevo:v");

        let mut de = rmp_serde::Deserializer::from_read_ref(&buf);
        let back = Ids::deserialize(Unescaping::new(&mut de)).unwrap();
        assert_eq!(back, doc);
    }
}
