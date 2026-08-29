//! Compression for the remote stripe protocol.
//!
//! Stripes on the wire are database pages, which compress about as well here as
//! they do in an archive, so the same zstd the archive format uses is worth
//! having between a fork and its source: a fork's catch-up is bounded by the
//! link, and prod's writes wait behind the pushes.
//!
//! Only the server sends stripe payloads — replies to pulls, and pushes on a
//! subscription — so the client is always the one decoding. The server
//! advertises what it can encode and the client picks, which keeps the choice
//! with the side that has it in its config.

use serde::Deserialize;

use crate::Result;

/// Compression level for stripes on the wire. Low on purpose: prod compresses
/// with a guest write waiting behind the push, so this buys bandwidth for as
/// little latency as it can.
const WIRE_ZSTD_LEVEL: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireCompression {
    #[default]
    None,
    Zstd,
}

impl WireCompression {
    pub fn code(self) -> u8 {
        match self {
            WireCompression::None => 0x00,
            WireCompression::Zstd => 0x01,
        }
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            0x00 => Ok(WireCompression::None),
            0x01 => Ok(WireCompression::Zstd),
            other => Err(crate::ubiblk_error!(ProtocolError {
                description: format!("Unknown wire compression algorithm {other}"),
            })),
        }
    }

    /// The algorithms this build understands, as a bitmask to advertise.
    pub fn supported_mask() -> u8 {
        WireCompression::None.mask_bit() | WireCompression::Zstd.mask_bit()
    }

    fn mask_bit(self) -> u8 {
        1 << self.code()
    }

    /// The best algorithm both ends have: `mask` is what the peer offered.
    /// Uncompressed is in every mask, so this always lands somewhere.
    pub fn best_of(self, mask: u8) -> Self {
        if self == WireCompression::Zstd && mask & WireCompression::Zstd.mask_bit() != 0 {
            WireCompression::Zstd
        } else {
            WireCompression::None
        }
    }

    pub fn compress<'a>(self, data: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>> {
        match self {
            WireCompression::None => Ok(std::borrow::Cow::Borrowed(data)),
            WireCompression::Zstd => Ok(std::borrow::Cow::Owned(
                zstd::stream::encode_all(data, WIRE_ZSTD_LEVEL)
                    .map_err(|source| crate::ubiblk_error!(IoError { source: source }))?,
            )),
        }
    }

    pub fn decompress(self, data: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            WireCompression::None => Ok(data),
            WireCompression::Zstd => zstd::stream::decode_all(data.as_slice())
                .map_err(|source| crate::ubiblk_error!(IoError { source: source })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_compressed_stripe_survives_the_round_trip() {
        let stripe = [b"postgres page header".to_vec(), vec![0u8; 4096]].concat();
        let compressed = WireCompression::Zstd.compress(&stripe).unwrap();
        assert!(
            compressed.len() < stripe.len(),
            "a mostly empty page must get smaller"
        );
        assert_eq!(
            WireCompression::Zstd
                .decompress(compressed.into_owned())
                .unwrap(),
            stripe
        );
    }

    #[test]
    fn uncompressed_data_is_not_copied() {
        let stripe = vec![0x11u8; 512];
        let passed_through = WireCompression::None.compress(&stripe).unwrap();
        assert!(matches!(passed_through, std::borrow::Cow::Borrowed(_)));
        assert_eq!(passed_through.into_owned(), stripe);
    }

    #[test]
    fn negotiation_falls_back_to_uncompressed() {
        let only_uncompressed = WireCompression::None.mask_bit();
        assert_eq!(
            WireCompression::Zstd.best_of(only_uncompressed),
            WireCompression::None,
            "a peer that cannot encode zstd is not asked to"
        );
        assert_eq!(
            WireCompression::Zstd.best_of(WireCompression::supported_mask()),
            WireCompression::Zstd
        );
        assert_eq!(
            WireCompression::None.best_of(WireCompression::supported_mask()),
            WireCompression::None,
            "a client that asked for no compression does not get it"
        );
    }

    #[test]
    fn an_unknown_algorithm_is_rejected() {
        assert!(WireCompression::from_code(0x7F).is_err());
    }
}
