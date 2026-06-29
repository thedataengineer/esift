//! Decompression for Datadog archive objects.
//!
//! Datadog archives logs as NDJSON compressed with zstd (the current default)
//! or gzip. The codec is selected from the object key's suffix via
//! [`Codec::from_key`], then the bytes are inflated to the raw NDJSON payload.
//!
//! gzip is always available (via `flate2`, already a dependency). zstd support
//! is gated behind the `datadog-s3` feature so the default build pulls in no
//! extra dependencies; the archive source that needs it is gated the same way.

use crate::error::{EsiftError, Result};
use std::io::Read;

/// Compression codec of a Datadog archive object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Zstd,
    Gzip,
}

impl Codec {
    /// Select a codec from an object key's suffix: `.zst`/`.zstd` → [`Codec::Zstd`],
    /// `.gz`/`.gzip` → [`Codec::Gzip`]. Returns `None` for anything else.
    pub fn from_key(key: &str) -> Option<Self> {
        let k = key.to_ascii_lowercase();
        if k.ends_with(".zst") || k.ends_with(".zstd") {
            Some(Codec::Zstd)
        } else if k.ends_with(".gz") || k.ends_with(".gzip") {
            Some(Codec::Gzip)
        } else {
            None
        }
    }
}

/// Decompress `bytes` with `codec`, returning the decoded payload.
pub fn decompress(bytes: &[u8], codec: Codec) -> Result<Vec<u8>> {
    let out = match codec {
        Codec::Gzip => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(bytes)
                .read_to_end(&mut out)
                .map_err(|e| EsiftError::Source(format!("gzip decode failed: {e}")))?;
            out
        }
        Codec::Zstd => {
            #[cfg(feature = "datadog-s3")]
            {
                zstd::stream::decode_all(bytes)
                    .map_err(|e| EsiftError::Source(format!("zstd decode failed: {e}")))?
            }
            #[cfg(not(feature = "datadog-s3"))]
            {
                let _ = bytes;
                return Err(EsiftError::Source(
                    "zstd decompression requires building with --features datadog-s3".into(),
                ));
            }
        }
    };
    // No cloud label in scope here (the codec/sizes are all we know); the archive
    // source records the cloud-labelled byte/duration metrics around this call.
    tracing::debug!(
        codec = ?codec,
        input_bytes = bytes.len(),
        output_bytes = out.len(),
        "decompressed archive object"
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_from_key_matches_suffixes() {
        assert_eq!(Codec::from_key("a/b/0_x.json.zst"), Some(Codec::Zstd));
        assert_eq!(Codec::from_key("a/b/0_x.json.gz"), Some(Codec::Gzip));
        assert_eq!(Codec::from_key("a/b/0_x.JSON.ZST"), Some(Codec::Zstd));
        assert_eq!(Codec::from_key("a/b/0_x.json"), None);
    }

    #[test]
    fn gzip_round_trips() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;
        let payload = b"{\"a\":1}\n{\"b\":2}\n";
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(payload).unwrap();
        let compressed = enc.finish().unwrap();
        let decoded = decompress(&compressed, Codec::Gzip).unwrap();
        assert_eq!(decoded, payload);
    }

    #[cfg(feature = "datadog-s3")]
    #[test]
    fn zstd_round_trips() {
        let payload = b"{\"a\":1}\n{\"b\":2}\n";
        let compressed = zstd::stream::encode_all(&payload[..], 0).unwrap();
        let decoded = decompress(&compressed, Codec::Zstd).unwrap();
        assert_eq!(decoded, payload);
    }
}
