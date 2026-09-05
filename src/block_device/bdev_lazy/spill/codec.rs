//! The object format for a spilled stripe, and the codec that produces and
//! consumes it.
//!
//! Object key: `<device_id>/<stripe_index>`, which the store prefixes. The
//! wrapped data key, when a KEK is configured, lives at `<device_id>/spill-key`.

use std::{sync::atomic::Ordering, time::Instant};

use crate::{
    archive::ArchiveCompressionAlgorithm,
    backends::SECTOR_SIZE,
    block_device::SpillCounters,
    crypt::{CipherMethod, KeyEncryptionCipher, XtsBlockCipher},
    Result,
};

/// The first eight bytes of every spill object.
pub const SPILL_MAGIC: &[u8; 8] = b"UBISPILL";
/// The only object format version this codec reads or writes.
pub const SPILL_OBJECT_VERSION: u16 = 1;
/// Bytes of header before the payload.
pub const SPILL_HEADER_LEN: usize = 36;

/// Object header flags: how the payload was transformed, in the order
/// compress then encrypt.
pub mod spill_flags {
    /// The payload is zstd-compressed.
    pub const ZSTD: u16 = 1 << 0;
    /// The payload is AES-XTS encrypted with the device's spill key.
    pub const XTS: u16 = 1 << 1;
    /// Every flag this version understands; others fail decoding.
    pub const KNOWN: u16 = ZSTD | XTS;
}

/// The bytes of the header the CRC covers: everything before the CRC itself.
const CRC_COVERED_LEN: usize = 32;

/// Little-endian, fixed 36 bytes, followed by the payload:
///  0..8   magic "UBISPILL"
///  8..10  version u16 = 1
/// 10..12  flags u16 (ZSTD, XTS)
/// 12..16  reserved u32 = 0
/// 16..24  stripe_index u64
/// 24..28  uncompressed_len u32   plaintext bytes (sector multiple; short for the last stripe)
/// 28..32  payload_len u32
/// 32..36  crc32 u32 over bytes [0..32] ++ payload (crc32fast)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillObjectHeader {
    pub flags: u16,
    pub stripe_index: u64,
    pub uncompressed_len: u32,
    pub payload_len: u32,
    pub crc32: u32,
}

fn archive_error(description: String) -> crate::UbiblkError {
    crate::ubiblk_error!(ArchiveError {
        description: description
    })
}

impl SpillObjectHeader {
    /// The header's wire form. Magic, version and reserved come from the
    /// constants; the caller has already computed `crc32`.
    pub fn encode(&self) -> [u8; SPILL_HEADER_LEN] {
        let mut bytes = [0u8; SPILL_HEADER_LEN];
        bytes[0..8].copy_from_slice(SPILL_MAGIC);
        bytes[8..10].copy_from_slice(&SPILL_OBJECT_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&self.flags.to_le_bytes());
        // 12..16 reserved, zero
        bytes[16..24].copy_from_slice(&self.stripe_index.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.crc32.to_le_bytes());
        bytes
    }

    /// Checks magic, version and reserved == 0. The CRC is checked by the codec
    /// because it covers the payload.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SPILL_HEADER_LEN {
            return Err(archive_error(format!(
                "spill object too short for a header: {} < {SPILL_HEADER_LEN}",
                bytes.len()
            )));
        }
        if &bytes[0..8] != SPILL_MAGIC {
            return Err(archive_error(format!(
                "spill object magic mismatch: {:?}",
                &bytes[0..8]
            )));
        }
        let version = u16::from_le_bytes([bytes[8], bytes[9]]);
        if version != SPILL_OBJECT_VERSION {
            return Err(archive_error(format!(
                "unsupported spill object version {version} (expected {SPILL_OBJECT_VERSION})"
            )));
        }
        let reserved = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        if reserved != 0 {
            return Err(archive_error(format!(
                "spill object reserved field is {reserved:#x}, expected 0"
            )));
        }
        Ok(SpillObjectHeader {
            flags: u16::from_le_bytes([bytes[10], bytes[11]]),
            stripe_index: u64::from_le_bytes(
                bytes[16..24]
                    .try_into()
                    .map_err(|_| archive_error("spill object header is malformed".to_string()))?,
            ),
            uncompressed_len: u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
            payload_len: u32::from_le_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]),
            crc32: u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]),
        })
    }
}

/// CRC over the header bytes before the CRC field, then the payload.
fn object_crc32(header_bytes: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header_bytes[..CRC_COVERED_LEN]);
    hasher.update(payload);
    hasher.finalize()
}

/// Object name of a stripe, before the store adds its prefix.
pub fn spill_object_name(device_id: &str, stripe_index: usize) -> String {
    format!("{device_id}/{stripe_index}")
}

/// Object name of the device's wrapped spill key.
pub fn spill_key_object_name(device_id: &str) -> String {
    format!("{device_id}/spill-key")
}

/// Inverse of `spill_object_name` for a name without prefix. None for the key
/// object and anything else that is not a stripe.
pub fn parse_spill_object_name(name: &str) -> Option<usize> {
    name.rsplit('/').next()?.parse().ok()
}

/// Stripe bytes to object bytes and back. One per fetcher and one for the
/// evictor (`XtsBlockCipher` methods take `&mut self`).
#[derive(Clone)]
pub struct SpillCodec {
    compression: ArchiveCompressionAlgorithm,
    cipher: Option<XtsBlockCipher>,
    stripe_sector_count: u64,
}

impl SpillCodec {
    /// `cipher` None means objects are stored in the clear;
    /// `stripe_sector_count` fixes the XTS tweak of each stripe.
    pub fn new(
        compression: ArchiveCompressionAlgorithm,
        cipher: Option<XtsBlockCipher>,
        stripe_sector_count: u64,
    ) -> Self {
        SpillCodec {
            compression,
            cipher,
            stripe_sector_count,
        }
    }

    /// XTS tweak base for a stripe: its first sector on the device, so an
    /// object encrypts like the device region it came from.
    fn sector_start(&self, stripe_index: usize) -> u64 {
        stripe_index as u64 * self.stripe_sector_count
    }

    /// `data.len()` must be a non-zero multiple of SECTOR_SIZE. Order:
    /// compress (sector-padded with its own size header), then XTS with tweak
    /// sector_start = stripe_index * stripe_sector_count, then header + crc32.
    /// Adds elapsed time to `counters.encode_ns` when given.
    pub fn encode(
        &mut self,
        stripe_index: usize,
        data: &[u8],
        counters: Option<&SpillCounters>,
    ) -> Result<Vec<u8>> {
        let start = Instant::now();
        if data.is_empty() || !data.len().is_multiple_of(SECTOR_SIZE) {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "spill payload length {} is not a non-zero multiple of {SECTOR_SIZE}",
                    data.len()
                ),
            }));
        }
        let uncompressed_len = u32::try_from(data.len()).map_err(|_| {
            archive_error(format!("stripe of {} bytes does not fit a u32", data.len()))
        })?;

        let mut flags = 0u16;
        let mut payload = self.compression.compress(data)?;
        if matches!(self.compression, ArchiveCompressionAlgorithm::Zstd { .. }) {
            flags |= spill_flags::ZSTD;
        }
        let sector_start = self.sector_start(stripe_index);
        if let Some(cipher) = &mut self.cipher {
            flags |= spill_flags::XTS;
            let sectors = (payload.len() / SECTOR_SIZE) as u64;
            cipher.encrypt(&mut payload, sector_start, sectors);
        }
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            archive_error(format!(
                "payload of {} bytes does not fit a u32",
                payload.len()
            ))
        })?;

        let mut header = SpillObjectHeader {
            flags,
            stripe_index: stripe_index as u64,
            uncompressed_len,
            payload_len,
            crc32: 0,
        };
        header.crc32 = object_crc32(&header.encode(), &payload);

        let mut object = Vec::with_capacity(SPILL_HEADER_LEN + payload.len());
        object.extend_from_slice(&header.encode());
        object.extend_from_slice(&payload);

        if let Some(counters) = counters {
            counters
                .encode_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(object)
    }

    /// Verifies magic, version, reserved, crc32, stripe_index == expected and
    /// lengths, then decrypts and decompresses into `dst`, zero-filling past
    /// uncompressed_len. Returns uncompressed_len. A mis-keyed or mixed-up
    /// object fails on the index or the CRC before any plaintext is produced.
    pub fn decode_into(
        &mut self,
        expected_stripe_index: usize,
        object: &[u8],
        dst: &mut [u8],
        counters: Option<&SpillCounters>,
    ) -> Result<usize> {
        let start = Instant::now();
        let header = SpillObjectHeader::decode(object)?;
        if header.stripe_index != expected_stripe_index as u64 {
            return Err(archive_error(format!(
                "spill object is for stripe {} but stripe {expected_stripe_index} was requested",
                header.stripe_index
            )));
        }
        if header.flags & !spill_flags::KNOWN != 0 {
            return Err(archive_error(format!(
                "spill object has unknown flags {:#06x}",
                header.flags
            )));
        }
        let payload_len = header.payload_len as usize;
        if object.len() != SPILL_HEADER_LEN + payload_len {
            return Err(archive_error(format!(
                "spill object length {} does not match header + payload {}",
                object.len(),
                SPILL_HEADER_LEN + payload_len
            )));
        }
        let payload = &object[SPILL_HEADER_LEN..];
        let crc32 = object_crc32(object, payload);
        if crc32 != header.crc32 {
            return Err(archive_error(format!(
                "spill object crc32 mismatch: header {:#010x}, computed {crc32:#010x}",
                header.crc32
            )));
        }
        let uncompressed_len = header.uncompressed_len as usize;
        if uncompressed_len == 0
            || !uncompressed_len.is_multiple_of(SECTOR_SIZE)
            || uncompressed_len > dst.len()
        {
            return Err(archive_error(format!(
                "spill object uncompressed length {uncompressed_len} is invalid for a {} byte buffer",
                dst.len()
            )));
        }

        // A key configured here means every object must be encrypted: a
        // plaintext object under a keyed device is a substitution, not data.
        let encrypted = header.flags & spill_flags::XTS != 0;
        let sector_start = self.sector_start(expected_stripe_index);
        let mut payload = payload.to_vec();
        match (&mut self.cipher, encrypted) {
            (Some(cipher), true) => {
                if !payload.len().is_multiple_of(SECTOR_SIZE) {
                    return Err(archive_error(format!(
                        "encrypted spill payload of {} bytes is not sector aligned",
                        payload.len()
                    )));
                }
                let sectors = (payload.len() / SECTOR_SIZE) as u64;
                cipher.decrypt(&mut payload, sector_start, sectors);
            }
            (None, false) => {}
            (Some(_), false) => {
                return Err(archive_error(
                    "spill object is not encrypted but this device has a spill key".to_string(),
                ))
            }
            (None, true) => {
                return Err(archive_error(
                    "spill object is encrypted but this device has no spill key".to_string(),
                ))
            }
        }

        // Decode by the object's flag, not this codec's setting: the setting
        // may have changed since the object was written.
        let plain = if header.flags & spill_flags::ZSTD != 0 {
            ArchiveCompressionAlgorithm::Zstd { level: 0 }.decompress(&payload)?
        } else {
            payload
        };
        if plain.len() != uncompressed_len {
            return Err(archive_error(format!(
                "spill object decoded to {} bytes but its header says {uncompressed_len}",
                plain.len()
            )));
        }
        dst[..uncompressed_len].copy_from_slice(&plain);
        dst[uncompressed_len..].fill(0);

        if let Some(counters) = counters {
            counters
                .decode_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(uncompressed_len)
    }
}

/// Key wrapping. The 32-byte KEK secret becomes an AES-256-GCM
/// KeyEncryptionCipher with auth_data b"ubiblk_spill".
pub fn spill_kek(kek: &[u8]) -> KeyEncryptionCipher {
    KeyEncryptionCipher {
        method: CipherMethod::Aes256Gcm,
        key: Some(kek.to_vec()),
        auth_data: Some(b"ubiblk_spill".to_vec()),
    }
}

/// The wrapped object is encrypt_key_data(key1 ++ key2) of the cipher.
pub fn wrap_spill_key(kek: &KeyEncryptionCipher, cipher: &XtsBlockCipher) -> Result<Vec<u8>> {
    cipher.encrypted_key(kek)
}

/// Inverse of `wrap_spill_key`: the cipher from the key object's bytes.
pub fn unwrap_spill_key(kek: &KeyEncryptionCipher, wrapped: &[u8]) -> Result<XtsBlockCipher> {
    XtsBlockCipher::from_encrypted_key(wrapped.to_vec(), kek)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE_SECTORS: u64 = 8;
    const STRIPE_SIZE: usize = STRIPE_SECTORS as usize * SECTOR_SIZE;

    fn stripe_data(seed: u8) -> Vec<u8> {
        (0..STRIPE_SIZE)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    fn zstd() -> ArchiveCompressionAlgorithm {
        ArchiveCompressionAlgorithm::Zstd { level: 3 }
    }

    fn cipher() -> XtsBlockCipher {
        XtsBlockCipher::new(vec![0x11; 32], vec![0x22; 32]).unwrap()
    }

    fn roundtrip(compression: ArchiveCompressionAlgorithm, cipher: Option<XtsBlockCipher>) {
        let counters = SpillCounters::default();
        let mut codec = SpillCodec::new(compression, cipher, STRIPE_SECTORS);
        let data = stripe_data(7);

        let object = codec.encode(3, &data, Some(&counters)).unwrap();
        let header = SpillObjectHeader::decode(&object).unwrap();
        assert_eq!(header.stripe_index, 3);
        assert_eq!(header.uncompressed_len as usize, STRIPE_SIZE);
        assert_eq!(header.payload_len as usize, object.len() - SPILL_HEADER_LEN);
        assert_eq!(
            header.flags & spill_flags::ZSTD != 0,
            matches!(codec.compression, ArchiveCompressionAlgorithm::Zstd { .. })
        );
        assert_eq!(header.flags & spill_flags::XTS != 0, codec.cipher.is_some());
        if codec.cipher.is_some() || header.flags & spill_flags::ZSTD != 0 {
            assert_ne!(
                &object[SPILL_HEADER_LEN..],
                &data[..],
                "payload is transformed"
            );
        }

        let mut dst = vec![0xAAu8; STRIPE_SIZE];
        let n = codec
            .decode_into(3, &object, &mut dst, Some(&counters))
            .unwrap();
        assert_eq!(n, STRIPE_SIZE);
        assert_eq!(dst, data);
        assert!(counters.encode_ns.load(Ordering::Relaxed) > 0);
        assert!(counters.decode_ns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn header_roundtrip() {
        let header = SpillObjectHeader {
            flags: spill_flags::ZSTD | spill_flags::XTS,
            stripe_index: 0x0102_0304_0506_0708,
            uncompressed_len: 4096,
            payload_len: 1536,
            crc32: 0xDEAD_BEEF,
        };
        let bytes = header.encode();
        assert_eq!(bytes.len(), SPILL_HEADER_LEN);
        assert_eq!(&bytes[0..8], SPILL_MAGIC);
        assert_eq!(&bytes[8..10], &SPILL_OBJECT_VERSION.to_le_bytes());
        assert_eq!(&bytes[12..16], &[0, 0, 0, 0]);
        assert_eq!(SpillObjectHeader::decode(&bytes).unwrap(), header);
        // Trailing payload bytes do not disturb the header.
        let mut with_payload = bytes.to_vec();
        with_payload.extend_from_slice(&[1, 2, 3]);
        assert_eq!(SpillObjectHeader::decode(&with_payload).unwrap(), header);
    }

    #[test]
    fn header_rejects_bad_magic_version_reserved() {
        let header = SpillObjectHeader {
            flags: 0,
            stripe_index: 1,
            uncompressed_len: 512,
            payload_len: 512,
            crc32: 0,
        };
        let good = header.encode();

        let mut bad_magic = good;
        bad_magic[0] = b'X';
        let err = SpillObjectHeader::decode(&bad_magic)
            .unwrap_err()
            .to_string();
        assert!(err.contains("magic mismatch"), "{err}");

        let mut bad_version = good;
        bad_version[8..10].copy_from_slice(&(SPILL_OBJECT_VERSION + 1).to_le_bytes());
        let err = SpillObjectHeader::decode(&bad_version)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported spill object version 2"), "{err}");

        let mut bad_reserved = good;
        bad_reserved[13] = 1;
        let err = SpillObjectHeader::decode(&bad_reserved)
            .unwrap_err()
            .to_string();
        assert!(err.contains("reserved field"), "{err}");

        let err = SpillObjectHeader::decode(&good[..SPILL_HEADER_LEN - 1])
            .unwrap_err()
            .to_string();
        assert!(err.contains("too short"), "{err}");
    }

    #[test]
    fn codec_roundtrip_none() {
        roundtrip(ArchiveCompressionAlgorithm::None, None);
    }

    #[test]
    fn codec_roundtrip_zstd() {
        roundtrip(zstd(), None);
    }

    #[test]
    fn codec_roundtrip_xts() {
        roundtrip(ArchiveCompressionAlgorithm::None, Some(cipher()));
    }

    #[test]
    fn codec_roundtrip_zstd_xts() {
        roundtrip(zstd(), Some(cipher()));
    }

    #[test]
    fn codec_rejects_crc_flip() {
        let mut codec = SpillCodec::new(zstd(), Some(cipher()), STRIPE_SECTORS);
        let data = stripe_data(1);
        let mut object = codec.encode(2, &data, None).unwrap();
        let mut dst = vec![0u8; STRIPE_SIZE];

        // A flipped payload bit.
        object[SPILL_HEADER_LEN + 10] ^= 0x01;
        let err = codec
            .decode_into(2, &object, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("crc32 mismatch"), "{err}");
        object[SPILL_HEADER_LEN + 10] ^= 0x01;

        // A flipped header bit (uncompressed_len) is caught by the CRC too.
        object[24] ^= 0x01;
        let err = codec
            .decode_into(2, &object, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("crc32 mismatch"), "{err}");
        object[24] ^= 0x01;

        assert!(codec.decode_into(2, &object, &mut dst, None).is_ok());
    }

    #[test]
    fn codec_rejects_stripe_index_mismatch_before_decrypt() {
        let mut codec = SpillCodec::new(
            ArchiveCompressionAlgorithm::None,
            Some(cipher()),
            STRIPE_SECTORS,
        );
        let data = stripe_data(2);
        let object = codec.encode(3, &data, None).unwrap();
        let mut dst = vec![0xAAu8; STRIPE_SIZE];

        let err = codec
            .decode_into(4, &object, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("for stripe 3 but stripe 4 was requested"),
            "{err}"
        );
        assert!(dst.iter().all(|&b| b == 0xAA), "no plaintext was produced");

        // Decrypting under the wrong tweak would be silently wrong; the index
        // check is what stops it.
        assert!(codec.decode_into(3, &object, &mut dst, None).is_ok());
        assert_eq!(dst, data);
    }

    #[test]
    fn codec_rejects_truncated_object() {
        let mut codec = SpillCodec::new(zstd(), None, STRIPE_SECTORS);
        let object = codec.encode(1, &stripe_data(3), None).unwrap();
        let mut dst = vec![0u8; STRIPE_SIZE];

        let err = codec
            .decode_into(1, &object[..object.len() - 1], &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match header + payload"), "{err}");

        let mut longer = object.clone();
        longer.push(0);
        let err = codec
            .decode_into(1, &longer, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match header + payload"), "{err}");

        let err = codec
            .decode_into(1, &object[..20], &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("too short"), "{err}");
    }

    #[test]
    fn codec_zero_fills_short_last_stripe() {
        for compression in [ArchiveCompressionAlgorithm::None, zstd()] {
            let mut codec = SpillCodec::new(compression, Some(cipher()), STRIPE_SECTORS);
            let short = &stripe_data(4)[..3 * SECTOR_SIZE];
            let object = codec.encode(9, short, None).unwrap();

            let mut dst = vec![0xAAu8; STRIPE_SIZE];
            let n = codec.decode_into(9, &object, &mut dst, None).unwrap();
            assert_eq!(n, 3 * SECTOR_SIZE);
            assert_eq!(&dst[..n], short);
            assert!(dst[n..].iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn codec_rejects_uncompressed_len_mismatch() {
        let mut codec = SpillCodec::new(zstd(), None, STRIPE_SECTORS);
        let data = stripe_data(5);
        let object = codec.encode(1, &data, None).unwrap();
        let mut dst = vec![0u8; STRIPE_SIZE];

        // Rewrite the header to claim one sector less, with a matching CRC, so
        // only the length check can catch it.
        let mut header = SpillObjectHeader::decode(&object).unwrap();
        header.uncompressed_len -= SECTOR_SIZE as u32;
        header.crc32 = object_crc32(&header.encode(), &object[SPILL_HEADER_LEN..]);
        let mut forged = header.encode().to_vec();
        forged.extend_from_slice(&object[SPILL_HEADER_LEN..]);
        let err = codec
            .decode_into(1, &forged, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("but its header says"), "{err}");

        // A destination that cannot hold the stripe is refused up front.
        let mut small = vec![0u8; STRIPE_SIZE - SECTOR_SIZE];
        let err = codec
            .decode_into(1, &object, &mut small, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is invalid for a"), "{err}");
    }

    #[test]
    fn codec_rejects_bad_length_input_and_unknown_flags() {
        let mut codec = SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS);
        assert!(codec.encode(0, &[], None).is_err());
        assert!(codec.encode(0, &[0u8; SECTOR_SIZE + 1], None).is_err());

        let object = codec.encode(1, &stripe_data(6), None).unwrap();
        let mut header = SpillObjectHeader::decode(&object).unwrap();
        header.flags |= 1 << 5;
        header.crc32 = object_crc32(&header.encode(), &object[SPILL_HEADER_LEN..]);
        let mut forged = header.encode().to_vec();
        forged.extend_from_slice(&object[SPILL_HEADER_LEN..]);
        let mut dst = vec![0u8; STRIPE_SIZE];
        let err = codec
            .decode_into(1, &forged, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown flags"), "{err}");
    }

    #[test]
    fn codec_key_presence_must_match_object() {
        let data = stripe_data(8);
        let mut plain_codec =
            SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS);
        let mut keyed_codec = SpillCodec::new(
            ArchiveCompressionAlgorithm::None,
            Some(cipher()),
            STRIPE_SECTORS,
        );
        let plain_object = plain_codec.encode(1, &data, None).unwrap();
        let keyed_object = keyed_codec.encode(1, &data, None).unwrap();
        let mut dst = vec![0u8; STRIPE_SIZE];

        let err = keyed_codec
            .decode_into(1, &plain_object, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not encrypted but this device has a spill key"),
            "{err}"
        );
        let err = plain_codec
            .decode_into(1, &keyed_object, &mut dst, None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("encrypted but this device has no spill key"),
            "{err}"
        );

        // A different key fails on the compressed framing rather than
        // returning garbage.
        let other = XtsBlockCipher::new(vec![0x33; 32], vec![0x44; 32]).unwrap();
        let mut wrong_key = SpillCodec::new(zstd(), Some(other), STRIPE_SECTORS);
        let mut right_key = SpillCodec::new(zstd(), Some(cipher()), STRIPE_SECTORS);
        let object = right_key.encode(1, &data, None).unwrap();
        assert!(wrong_key.decode_into(1, &object, &mut dst, None).is_err());
    }

    #[test]
    fn codec_decodes_by_object_flags_not_current_setting() {
        let data = stripe_data(9);
        let mut writer = SpillCodec::new(zstd(), None, STRIPE_SECTORS);
        let object = writer.encode(1, &data, None).unwrap();

        let mut reader = SpillCodec::new(ArchiveCompressionAlgorithm::None, None, STRIPE_SECTORS);
        let mut dst = vec![0u8; STRIPE_SIZE];
        reader.decode_into(1, &object, &mut dst, None).unwrap();
        assert_eq!(dst, data);
    }

    #[test]
    fn object_name_roundtrip() {
        assert_eq!(spill_object_name("fork-3f9c", 17), "fork-3f9c/17");
        assert_eq!(spill_key_object_name("fork-3f9c"), "fork-3f9c/spill-key");
        assert_eq!(parse_spill_object_name("fork-3f9c/17"), Some(17));
        assert_eq!(
            parse_spill_object_name(&spill_object_name("dev", 0)),
            Some(0)
        );
        assert_eq!(parse_spill_object_name("fork-3f9c/spill-key"), None);
        assert_eq!(parse_spill_object_name("fork-3f9c/"), None);
        assert_eq!(parse_spill_object_name("42"), Some(42));
    }

    #[test]
    fn spill_key_wrap_roundtrip() {
        let kek = spill_kek(&[0x5Au8; 32]);
        assert_eq!(kek.method, CipherMethod::Aes256Gcm);
        assert_eq!(kek.auth_data.as_deref(), Some(&b"ubiblk_spill"[..]));

        let cipher = XtsBlockCipher::random().unwrap();
        let wrapped = wrap_spill_key(&kek, &cipher).unwrap();
        assert_ne!(wrapped.len(), 64, "wrapped key carries nonce and tag");
        let unwrapped = unwrap_spill_key(&kek, &wrapped).unwrap();
        assert_eq!(unwrapped, cipher);

        // Two wraps differ (random nonce) but unwrap to the same key.
        let again = wrap_spill_key(&kek, &cipher).unwrap();
        assert_ne!(again, wrapped);
        assert_eq!(unwrap_spill_key(&kek, &again).unwrap(), cipher);
    }

    #[test]
    fn spill_key_unwrap_rejects_wrong_kek() {
        let cipher = XtsBlockCipher::random().unwrap();
        let wrapped = wrap_spill_key(&spill_kek(&[0x5Au8; 32]), &cipher).unwrap();

        assert!(unwrap_spill_key(&spill_kek(&[0x5Bu8; 32]), &wrapped).is_err());
        // The archive KEK uses different auth data, so it cannot unwrap a
        // spill key even with the same bytes.
        let archive_kek = KeyEncryptionCipher {
            method: CipherMethod::Aes256Gcm,
            key: Some(vec![0x5Au8; 32]),
            auth_data: Some(b"ubiblk_archive".to_vec()),
        };
        assert!(unwrap_spill_key(&archive_kek, &wrapped).is_err());
        assert!(
            unwrap_spill_key(&spill_kek(&[0x5Au8; 32]), &wrapped[..wrapped.len() - 1]).is_err()
        );
    }
}
