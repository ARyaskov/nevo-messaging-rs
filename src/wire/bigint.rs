//! Arbitrary-precision integers carried as `@@nevo:bigint:<digits>` sentinel strings,
//! the representation the TypeScript peer uses for JavaScript `bigint` values.

use std::fmt;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::escape::{BIGINT_NEWTYPE, BIGINT_SENTINEL};

/// A decimal integer of any size, preserved exactly across the wire.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BigInt {
    digits: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid bigint literal: {0:?}")]
pub struct ParseBigIntError(String);

impl BigInt {
    /// Parses an optionally signed decimal literal; a `@@nevo:bigint:` prefix is tolerated.
    pub fn parse(input: &str) -> Result<Self, ParseBigIntError> {
        let literal = input.strip_prefix(BIGINT_SENTINEL).unwrap_or(input);
        let (negative, body) = match literal.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, literal),
        };
        if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ParseBigIntError(input.to_owned()));
        }
        let trimmed = body.trim_start_matches('0');
        let magnitude = if trimmed.is_empty() { "0" } else { trimmed };
        let digits = if negative && magnitude != "0" {
            format!("-{magnitude}")
        } else {
            magnitude.to_owned()
        };
        Ok(Self { digits })
    }

    /// Canonical decimal representation without the wire sentinel.
    pub fn as_str(&self) -> &str {
        &self.digits
    }

    pub fn is_negative(&self) -> bool {
        self.digits.starts_with('-')
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.digits.parse().ok()
    }

    pub fn to_u64(&self) -> Option<u64> {
        self.digits.parse().ok()
    }

    pub fn to_i128(&self) -> Option<i128> {
        self.digits.parse().ok()
    }

    pub fn to_u128(&self) -> Option<u128> {
        self.digits.parse().ok()
    }

    /// The sentinel string as it appears on the wire.
    pub fn to_wire_string(&self) -> String {
        format!("{BIGINT_SENTINEL}{}", self.digits)
    }
}

impl fmt::Debug for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BigInt({})", self.digits)
    }
}

impl fmt::Display for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.digits)
    }
}

impl FromStr for BigInt {
    type Err = ParseBigIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

macro_rules! from_int {
    ($($t:ty),*) => {$(
        impl From<$t> for BigInt {
            fn from(value: $t) -> Self {
                Self { digits: value.to_string() }
            }
        }
    )*};
}

from_int!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);

macro_rules! try_into_int {
    ($($t:ty => $method:ident),*) => {$(
        impl TryFrom<&BigInt> for $t {
            type Error = ParseBigIntError;

            fn try_from(value: &BigInt) -> Result<Self, Self::Error> {
                value
                    .$method()
                    .and_then(|v| Self::try_from(v).ok())
                    .ok_or_else(|| ParseBigIntError(value.digits.clone()))
            }
        }

        impl TryFrom<BigInt> for $t {
            type Error = ParseBigIntError;

            fn try_from(value: BigInt) -> Result<Self, Self::Error> {
                Self::try_from(&value)
            }
        }
    )*};
}

try_into_int!(
    i8 => to_i128, i16 => to_i128, i32 => to_i128, i64 => to_i128, i128 => to_i128,
    u8 => to_u128, u16 => to_u128, u32 => to_u128, u64 => to_u128, u128 => to_u128
);

struct Sentinel<'a>(&'a str);

impl Serialize for Sentinel<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{BIGINT_SENTINEL}{}", self.0))
    }
}

impl Serialize for BigInt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(BIGINT_NEWTYPE, &Sentinel(&self.digits))
    }
}

struct BigIntVisitor;

impl Visitor<'_> for BigIntVisitor {
    type Value = BigInt;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an integer or a `@@nevo:bigint:` string")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<BigInt, E> {
        BigInt::parse(v).map_err(E::custom)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<BigInt, E> {
        Ok(BigInt::from(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<BigInt, E> {
        Ok(BigInt::from(v))
    }

    fn visit_i128<E: de::Error>(self, v: i128) -> Result<BigInt, E> {
        Ok(BigInt::from(v))
    }

    fn visit_u128<E: de::Error>(self, v: u128) -> Result<BigInt, E> {
        Ok(BigInt::from(v))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<BigInt, E> {
        if v.is_finite() && v.fract() == 0.0 {
            BigInt::parse(&format!("{v:.0}")).map_err(E::custom)
        } else {
            Err(E::invalid_value(de::Unexpected::Float(v), &self))
        }
    }
}

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BigIntVisitor)
    }
}

macro_rules! with_module {
    ($name:ident, $opt:ident, $t:ty) => {
        /// `#[serde(with = "...")]` adapter that puts a primitive integer on the wire as a bigint sentinel.
        pub mod $name {
            use super::BigInt;
            use serde::{Deserialize, Deserializer, Serialize, Serializer};

            pub fn serialize<S: Serializer>(value: &$t, serializer: S) -> Result<S::Ok, S::Error> {
                BigInt::from(*value).serialize(serializer)
            }

            pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<$t, D::Error> {
                let big = BigInt::deserialize(deserializer)?;
                <$t>::try_from(&big).map_err(serde::de::Error::custom)
            }
        }

        /// Like the non-`opt` adapter, for `Option<...>` fields.
        pub mod $opt {
            use super::BigInt;
            use serde::{Deserialize, Deserializer, Serialize, Serializer};

            pub fn serialize<S: Serializer>(
                value: &Option<$t>,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                value.map(BigInt::from).serialize(serializer)
            }

            pub fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Option<$t>, D::Error> {
                Option::<BigInt>::deserialize(deserializer)?
                    .map(|big| <$t>::try_from(&big).map_err(serde::de::Error::custom))
                    .transpose()
            }
        }
    };
}

with_module!(as_i64, as_i64_opt, i64);
with_module!(as_u64, as_u64_opt, u64);
with_module!(as_i128, as_i128_opt, i128);
with_module!(as_u128, as_u128_opt, u128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_canonicalises() {
        assert_eq!(BigInt::parse("0007").unwrap().as_str(), "7");
        assert_eq!(BigInt::parse("-0").unwrap().as_str(), "0");
        assert_eq!(BigInt::parse("-12").unwrap().as_str(), "-12");
        assert_eq!(BigInt::parse("@@nevo:bigint:99").unwrap().as_str(), "99");
        assert!(BigInt::parse("12n").is_err());
        assert!(BigInt::parse("").is_err());
        assert!(BigInt::parse("-").is_err());
    }

    #[test]
    fn conversions() {
        let big = BigInt::from(-5i64);
        assert_eq!(i32::try_from(&big).unwrap(), -5);
        assert!(u8::try_from(&big).is_err());
        let huge = BigInt::parse("340282366920938463463374607431768211456").unwrap();
        assert!(huge.to_u128().is_none());
    }

    #[test]
    fn plain_json_serialisation_uses_sentinel() {
        let json = serde_json::to_string(&BigInt::from(42u8)).unwrap();
        assert_eq!(json, "\"@@nevo:bigint:42\"");
        let back: BigInt = serde_json::from_str(&json).unwrap();
        assert_eq!(back.as_str(), "42");
        let from_number: BigInt = serde_json::from_str("123").unwrap();
        assert_eq!(from_number.as_str(), "123");
        let from_float: BigInt = serde_json::from_str("1e3").unwrap();
        assert_eq!(from_float.as_str(), "1000");
    }

    #[test]
    fn with_adapters() {
        #[derive(Serialize, Deserialize)]
        struct Row {
            #[serde(with = "as_i64")]
            id: i64,
            #[serde(with = "as_u64_opt")]
            parent: Option<u64>,
        }
        let json = serde_json::to_string(&Row {
            id: -3,
            parent: None,
        })
        .unwrap();
        assert_eq!(json, "{\"id\":\"@@nevo:bigint:-3\",\"parent\":null}");
        let back: Row = serde_json::from_str("{\"id\":\"@@nevo:bigint:7\",\"parent\":9}").unwrap();
        assert_eq!(back.id, 7);
        assert_eq!(back.parent, Some(9));
    }
}
