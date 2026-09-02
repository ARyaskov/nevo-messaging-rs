//! Payload compression negotiated through the `content-encoding` header.

use bytes::Bytes;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Encoding {
    #[default]
    Identity,
    Gzip,
    Deflate,
    Zstd,
}

impl Encoding {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "" | "identity" => Some(Self::Identity),
            "gzip" => Some(Self::Gzip),
            "deflate" => Some(Self::Deflate),
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Zstd => "zstd",
        }
    }

    pub fn is_identity(self) -> bool {
        self == Self::Identity
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for Encoding {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Encoding {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        Self::parse(&raw).ok_or_else(|| {
            serde::de::Error::custom(format!("unsupported content encoding {raw:?}"))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Algorithm {
    #[default]
    Gzip,
    Deflate,
    Zstd,
}

impl Algorithm {
    pub fn encoding(self) -> Encoding {
        match self {
            Self::Gzip => Encoding::Gzip,
            Self::Deflate => Encoding::Deflate,
            Self::Zstd => Encoding::Zstd,
        }
    }
}

/// Outbound compression policy. Disabled by default.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Compression {
    pub enabled: bool,
    pub algorithm: Algorithm,
    pub threshold: usize,
    pub level: u32,
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: Algorithm::Gzip,
            threshold: 1024,
            level: 6,
        }
    }
}

impl Compression {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn gzip() -> Self {
        Self::with(Algorithm::Gzip)
    }

    pub fn deflate() -> Self {
        Self::with(Algorithm::Deflate)
    }

    pub fn zstd() -> Self {
        Self::with(Algorithm::Zstd)
    }

    pub fn with(algorithm: Algorithm) -> Self {
        Self {
            enabled: true,
            algorithm,
            ..Self::default()
        }
    }

    pub fn threshold(mut self, bytes: usize) -> Self {
        self.threshold = bytes;
        self
    }

    pub fn level(mut self, level: u32) -> Self {
        self.level = level;
        self
    }

    fn applies_to(&self, len: usize) -> bool {
        self.enabled && len >= self.threshold
    }
}

/// Compresses `data` when the policy asks for it; identity otherwise.
pub fn compress(data: Bytes, policy: &Compression) -> Result<(Bytes, Encoding)> {
    if !policy.applies_to(data.len()) {
        return Ok((data, Encoding::Identity));
    }
    compress_with(data, policy.algorithm, policy.level)
}

#[cfg(feature = "compression")]
fn compress_with(data: Bytes, algorithm: Algorithm, level: u32) -> Result<(Bytes, Encoding)> {
    use std::io::Write;

    let io_err = |e: std::io::Error| Error::internal(format!("compression failed: {e}"));
    match algorithm {
        Algorithm::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(
                Vec::with_capacity(data.len() / 2),
                flate2::Compression::new(level.min(9)),
            );
            enc.write_all(&data).map_err(io_err)?;
            Ok((Bytes::from(enc.finish().map_err(io_err)?), Encoding::Gzip))
        }
        Algorithm::Deflate => {
            let mut enc = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(data.len() / 2),
                flate2::Compression::new(level.min(9)),
            );
            enc.write_all(&data).map_err(io_err)?;
            Ok((
                Bytes::from(enc.finish().map_err(io_err)?),
                Encoding::Deflate,
            ))
        }
        #[cfg(feature = "zstd")]
        Algorithm::Zstd => {
            let out =
                zstd::stream::encode_all(&data[..], level.clamp(1, 22) as i32).map_err(io_err)?;
            Ok((Bytes::from(out), Encoding::Zstd))
        }
        #[cfg(not(feature = "zstd"))]
        Algorithm::Zstd => {
            tracing::warn!(
                event = "compression.zstd_unavailable",
                "zstd feature is disabled; falling back to gzip"
            );
            compress_with(data, Algorithm::Gzip, level)
        }
    }
}

#[cfg(not(feature = "compression"))]
fn compress_with(data: Bytes, _algorithm: Algorithm, _level: u32) -> Result<(Bytes, Encoding)> {
    tracing::warn!(
        event = "compression.unavailable",
        "compression feature is disabled; sending identity payload"
    );
    Ok((data, Encoding::Identity))
}

/// Inflates `data`, refusing to allocate more than `max_output` bytes.
pub fn decompress(data: Bytes, encoding: Encoding, max_output: usize) -> Result<Bytes> {
    if encoding.is_identity() {
        return Ok(data);
    }
    decompress_with(data, encoding, max_output)
}

#[cfg(feature = "compression")]
fn decompress_with(data: Bytes, encoding: Encoding, max_output: usize) -> Result<Bytes> {
    use std::io::Read;

    fn drain(mut reader: impl Read, max_output: usize) -> Result<Bytes> {
        let mut out = Vec::new();
        let limit = max_output as u64 + 1;
        reader
            .by_ref()
            .take(limit)
            .read_to_end(&mut out)
            .map_err(|e| Error::parse(format!("decompression failed: {e}")))?;
        if out.len() > max_output {
            return Err(Error::payload_too_large(out.len(), max_output));
        }
        Ok(Bytes::from(out))
    }

    match encoding {
        Encoding::Identity => Ok(data),
        Encoding::Gzip => drain(flate2::read::GzDecoder::new(&data[..]), max_output),
        Encoding::Deflate => drain(flate2::read::ZlibDecoder::new(&data[..]), max_output),
        #[cfg(feature = "zstd")]
        Encoding::Zstd => {
            let decoder = zstd::stream::Decoder::new(&data[..])
                .map_err(|e| Error::parse(format!("decompression failed: {e}")))?;
            drain(decoder, max_output)
        }
        #[cfg(not(feature = "zstd"))]
        Encoding::Zstd => Err(Error::bad_request(
            "zstd payload received but the zstd feature is disabled",
        )),
    }
}

#[cfg(not(feature = "compression"))]
fn decompress_with(_data: Bytes, encoding: Encoding, _max_output: usize) -> Result<Bytes> {
    Err(Error::bad_request(format!(
        "{encoding} payload received but the compression feature is disabled"
    )))
}

#[cfg(all(test, feature = "compression"))]
mod tests {
    use super::*;

    #[test]
    fn round_trip_gzip_and_deflate() {
        let original = Bytes::from("hello world ".repeat(200));
        for policy in [
            Compression::gzip().threshold(1),
            Compression::deflate().threshold(1),
        ] {
            let (packed, encoding) = compress(original.clone(), &policy).unwrap();
            assert_eq!(encoding, policy.algorithm.encoding());
            assert!(packed.len() < original.len());
            let back = decompress(packed, encoding, 1 << 20).unwrap();
            assert_eq!(back, original);
        }
    }

    #[test]
    fn below_threshold_is_identity() {
        let (out, enc) = compress(Bytes::from_static(b"tiny"), &Compression::gzip()).unwrap();
        assert_eq!(enc, Encoding::Identity);
        assert_eq!(&out[..], b"tiny");
    }

    #[test]
    fn bombs_are_rejected_before_full_allocation() {
        let zeros = Bytes::from(vec![0u8; 8 * 1024 * 1024]);
        let (bomb, enc) = compress(zeros, &Compression::gzip().threshold(1)).unwrap();
        assert!(bomb.len() < 64 * 1024);
        let err = decompress(bomb, enc, 1024 * 1024).unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::PayloadTooLarge);
    }

    #[test]
    fn encoding_parsing() {
        assert_eq!(Encoding::parse("gzip"), Some(Encoding::Gzip));
        assert_eq!(Encoding::parse(""), Some(Encoding::Identity));
        assert_eq!(Encoding::parse("br"), None);
    }
}
