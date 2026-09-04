use crate::{
    backends::SECTOR_SIZE,
    block_device::{shared_buffer, wait_for_completion, BlockDevice, UbiMetadata},
    Result,
};
use log::info;
use ubiblk_macros::error_context;

impl UbiMetadata {
    #[error_context("Failed to load metadata")]
    pub fn load_from_bdev(bdev: &dyn BlockDevice) -> Result<Box<Self>> {
        info!("Loading metadata from device");

        let mut io_channel = bdev.create_channel()?;
        let sector_count = bdev.sector_count();

        let buf = shared_buffer(sector_count as usize * SECTOR_SIZE);
        let sector_count_u32 = sector_count.try_into().map_err(|_| {
            crate::ubiblk_error!(InvalidParameter {
                description: "Metadata file too large".to_string(),
            })
        })?;

        io_channel.add_read(0, sector_count_u32, buf.clone(), 0);
        io_channel.submit()?;

        wait_for_completion(io_channel.as_mut(), 0, std::time::Duration::from_secs(30))?;

        let metadata = UbiMetadata::from_bytes(buf.borrow().as_slice())?;

        info!("Metadata loaded successfully");

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use crate::block_device::{
        bdev_lazy::metadata::types::{
            METADATA_VERSION_MAJOR, METADATA_VERSION_MINOR, METADATA_VERSION_MINOR_MIN,
        },
        bdev_test::TestBlockDevice,
        metadata_flags,
    };

    use super::*;

    /// A saved metadata file claiming version `major.minor`, with a few
    /// distinctive header bytes so a loader can be checked for keeping them.
    fn save_with_version(device: &TestBlockDevice, major: u16, minor: u16) -> Box<UbiMetadata> {
        let mut metadata = UbiMetadata::new(11, 16, 16);
        metadata.set_stripe_header(1, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(
            2,
            metadata_flags::WRITTEN | metadata_flags::FETCHED | metadata_flags::HAS_SOURCE,
        );
        metadata.version_major = major.to_le_bytes();
        metadata.version_minor = minor.to_le_bytes();
        metadata.save_to_bdev(device).expect("save metadata");
        metadata
    }

    #[test]
    fn loads_2_0() {
        let device = TestBlockDevice::new(1024 * 1024);
        let saved = save_with_version(&device, 2, 0);

        let loaded = UbiMetadata::load_from_bdev(&device).expect("2.0 must load");
        assert_eq!(loaded.version_major_u16(), 2);
        assert_eq!(loaded.version_minor_u16(), 0);
        assert_eq!(loaded.stripe_headers, saved.stripe_headers);
    }

    #[test]
    fn loads_2_1() {
        let device = TestBlockDevice::new(1024 * 1024);
        let mut metadata = UbiMetadata::new(11, 16, 16);
        assert_eq!(metadata.version_minor_u16(), METADATA_VERSION_MINOR);
        metadata.set_stripe_header(
            3,
            metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE,
        );
        metadata.set_stripe_header(4, metadata_flags::PUSHED | metadata_flags::HAS_SOURCE);
        metadata.save_to_bdev(&device).expect("save metadata");

        let loaded = UbiMetadata::load_from_bdev(&device).expect("2.1 must load");
        assert_eq!(loaded.version_minor_u16(), 1);
        assert_eq!(loaded.stripe_headers, metadata.stripe_headers);
        assert_eq!(loaded.evicted_stripe_ids(), vec![3]);
    }

    #[test]
    fn rejects_2_2() {
        let device = TestBlockDevice::new(1024 * 1024);
        save_with_version(&device, 2, 2);

        let err = UbiMetadata::load_from_bdev(&device).unwrap_err().to_string();
        assert!(err.contains("Metadata version mismatch"), "{err}");
        assert!(err.contains("Expected: 2.0..=2.1"), "{err}");
    }

    #[test]
    fn rejects_1_x() {
        let device = TestBlockDevice::new(1024 * 1024);
        save_with_version(&device, 1, METADATA_VERSION_MINOR_MIN);

        let err = UbiMetadata::load_from_bdev(&device).unwrap_err().to_string();
        assert!(err.contains("Metadata version mismatch"), "{err}");
    }

    #[test]
    fn rejects_reserved_bits() {
        let device = TestBlockDevice::new(1024 * 1024);
        let mut metadata = UbiMetadata::new(11, 16, 16);
        metadata.set_stripe_header(5, metadata_flags::HAS_SOURCE | 0b0100_0000);
        metadata.save_to_bdev(&device).expect("save metadata");

        let err = UbiMetadata::load_from_bdev(&device).unwrap_err().to_string();
        assert!(err.contains("stripe header 5 has reserved bits set"), "{err}");
    }

    #[test]
    fn upgrade_version_sector_rewrites_only_sector_0() {
        let device = TestBlockDevice::new(1024 * 1024);
        let saved = save_with_version(&device, METADATA_VERSION_MAJOR, 0);
        let header_sectors_before = device.mem.read().unwrap()[SECTOR_SIZE..].to_vec();
        let writes_before = device.metrics.read().unwrap().writes;
        let flushes_before = device.metrics.read().unwrap().flushes;

        assert!(UbiMetadata::upgrade_version_sector(&device).expect("upgrade"));

        let loaded = UbiMetadata::load_from_bdev(&device).expect("load upgraded");
        assert_eq!(loaded.version_major_u16(), METADATA_VERSION_MAJOR);
        assert_eq!(loaded.version_minor_u16(), METADATA_VERSION_MINOR);
        assert_eq!(loaded.stripe_headers, saved.stripe_headers);
        assert_eq!(
            device.mem.read().unwrap()[SECTOR_SIZE..].to_vec(),
            header_sectors_before,
            "header sectors must not be touched"
        );
        assert_eq!(device.metrics.read().unwrap().writes, writes_before + 1);
        assert_eq!(device.metrics.read().unwrap().flushes, flushes_before + 1);

        // Already current: nothing to do, nothing written.
        assert!(!UbiMetadata::upgrade_version_sector(&device).expect("upgrade"));
        assert_eq!(device.metrics.read().unwrap().writes, writes_before + 1);
    }

    #[test]
    fn test_loads_metadata() {
        let device = TestBlockDevice::new(1024 * 1024);
        let mut metadata = UbiMetadata::new(11, 16, 16);

        for (i, header) in metadata.stripe_headers.iter_mut().enumerate() {
            *header = (i as u8) % 5;
        }

        metadata.save_to_bdev(&device).expect("save metadata");

        let loaded_metadata = UbiMetadata::load_from_bdev(&device).expect("load metadata");

        assert_eq!(metadata.magic, loaded_metadata.magic);
        assert_eq!(metadata.version_major, loaded_metadata.version_major);
        assert_eq!(metadata.version_minor, loaded_metadata.version_minor);
        assert_eq!(
            metadata.stripe_sector_count_shift,
            loaded_metadata.stripe_sector_count_shift
        );
        assert_eq!(
            metadata.stripe_headers,
            loaded_metadata.stripe_headers[..metadata.stripe_headers.len()]
        );
    }

    #[test]
    fn test_invalid_magic() {
        let device = TestBlockDevice::new(1024 * 1024);
        let mut metadata = UbiMetadata::new(11, 16, 16);
        metadata.magic.copy_from_slice(b"BAD_MAGIC");
        metadata.save_to_bdev(&device).expect("save metadata");

        let result = UbiMetadata::load_from_bdev(&device);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Metadata magic mismatch"));
    }

    #[test]
    fn test_invalid_version() {
        let device = TestBlockDevice::new(1024 * 1024);
        let mut metadata = UbiMetadata::new(11, 16, 16);
        metadata.version_minor = (METADATA_VERSION_MINOR + 1).to_le_bytes();
        metadata.save_to_bdev(&device).expect("save metadata");

        let result = UbiMetadata::load_from_bdev(&device);
        assert!(
            result.is_err()
                && result
                    .unwrap_err()
                    .to_string()
                    .contains("Metadata version mismatch")
        );
    }
}
