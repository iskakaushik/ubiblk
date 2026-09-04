use log::info;
use ubiblk_macros::error_context;

use crate::{
    backends::SECTOR_SIZE,
    block_device::{
        bdev_lazy::metadata::types::{METADATA_VERSION_MAJOR, METADATA_VERSION_MINOR},
        shared_buffer, wait_for_completion, BlockDevice, UbiMetadata,
    },
    Result,
};

pub const METADATA_WRITE_ID: usize = 0;
pub const METADATA_FLUSH_ID: usize = 1;
pub const DEFAULT_STRIPE_SECTOR_COUNT_SHIFT: u8 = 11;

impl UbiMetadata {
    #[error_context("Failed to save metadata to block device")]
    pub fn save_to_bdev(&self, bdev: &dyn BlockDevice) -> Result<()> {
        let mut ch = bdev.create_channel()?;
        let metadata_size = self.metadata_size();
        let sector_count: u32 = bdev.sector_count().try_into().map_err(|_| {
            crate::ubiblk_error!(InvalidParameter {
                description: "Device sector count exceeds u32".to_string(),
            })
        })?;

        let total_size = bdev
            .sector_count()
            .checked_mul(SECTOR_SIZE as u64)
            .and_then(|size| usize::try_from(size).ok())
            .ok_or(crate::ubiblk_error!(InvalidParameter {
                description: "Metadata device too large".to_string(),
            }))?;

        if metadata_size > total_size {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "Metadata size {metadata_size} exceeds device capacity {total_size}"
                ),
            }));
        }

        let buf = shared_buffer(total_size);

        self.write_to_buf(&mut buf.borrow_mut().as_mut_slice()[..metadata_size])?;

        let timeout = std::time::Duration::from_secs(30);

        info!(
            "Initializing metadata device with {} sectors",
            bdev.sector_count()
        );

        ch.add_write(0, sector_count, buf.clone(), METADATA_WRITE_ID);
        ch.submit()?;
        wait_for_completion(ch.as_mut(), METADATA_WRITE_ID, timeout)?;

        ch.add_flush(METADATA_FLUSH_ID);
        ch.submit()?;
        wait_for_completion(ch.as_mut(), METADATA_FLUSH_ID, timeout)?;

        info!("Metadata device initialized successfully");

        Ok(())
    }

    /// Rewrite sector 0 with the current version constants if the loaded file
    /// is older. Header sectors are untouched. Called once at spill startup
    /// (before any eviction) so a pre-spill binary refuses a file whose header
    /// bytes may carry EVICTED rather than reading a hole as FETCHED data.
    /// Same write-then-flush pattern as `save_to_bdev`. Returns `Ok(true)` if
    /// the sector was rewritten.
    #[error_context("Failed to upgrade metadata version sector")]
    pub fn upgrade_version_sector(bdev: &dyn BlockDevice) -> Result<bool> {
        let mut metadata = UbiMetadata::load_from_bdev(bdev)?;
        if metadata.version_major_u16() == METADATA_VERSION_MAJOR
            && metadata.version_minor_u16() == METADATA_VERSION_MINOR
        {
            return Ok(false);
        }

        info!(
            "Upgrading metadata version {}.{} to {}.{}",
            metadata.version_major_u16(),
            metadata.version_minor_u16(),
            METADATA_VERSION_MAJOR,
            METADATA_VERSION_MINOR
        );
        metadata.version_major = METADATA_VERSION_MAJOR.to_le_bytes();
        metadata.version_minor = METADATA_VERSION_MINOR.to_le_bytes();

        let buf = shared_buffer(SECTOR_SIZE);
        metadata.write_header_sector(buf.borrow_mut().as_mut_slice())?;

        let timeout = std::time::Duration::from_secs(30);
        let mut ch = bdev.create_channel()?;
        ch.add_write(0, 1, buf.clone(), METADATA_WRITE_ID);
        ch.submit()?;
        wait_for_completion(ch.as_mut(), METADATA_WRITE_ID, timeout)?;

        ch.add_flush(METADATA_FLUSH_ID);
        ch.submit()?;
        wait_for_completion(ch.as_mut(), METADATA_FLUSH_ID, timeout)?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::block_device::NullBlockDevice;

    use super::*;

    #[test]
    fn test_errors_if_metadata_too_large() {
        let bdev = NullBlockDevice::new();
        let metadata = UbiMetadata::new(DEFAULT_STRIPE_SECTOR_COUNT_SHIFT, 4, 16);
        let result = metadata.save_to_bdev(bdev.as_ref());
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("exceeds device capacity"));
    }

    #[test]
    fn test_sector_count_exceeds_u32() {
        let bdev = NullBlockDevice::new_with_sector_count(u64::from(u32::MAX) + 1);
        let metadata = UbiMetadata::new(DEFAULT_STRIPE_SECTOR_COUNT_SHIFT, 4, 16);
        let result = metadata.save_to_bdev(bdev.as_ref());
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Device sector count exceeds u32"));
    }
}
