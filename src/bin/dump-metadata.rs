use clap::Parser;
use log::error;
use ubiblk::backends::{build_block_device, SECTOR_SIZE};
use ubiblk::block_device::{self, metadata_flags, BlockDevice, UbiMetadata};
use ubiblk::cli::{load_config, CommonArgs};
use ubiblk::config::v2;
use ubiblk::Result;

#[derive(Parser)]
#[command(
    name = "dump-metadata",
    version,
    author,
    about = "Dump metadata information"
)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

fn format_list(list: &[usize]) -> String {
    if list.is_empty() {
        return String::new();
    }

    let mut formatted = Vec::new();
    let mut start = list[0];
    let mut prev = list[0];

    for &value in &list[1..] {
        if value == prev + 1 {
            prev = value;
            continue;
        }

        push_range(&mut formatted, start, prev);
        start = value;
        prev = value;
    }

    push_range(&mut formatted, start, prev);

    formatted.join(", ")
}

fn push_range(output: &mut Vec<String>, start: usize, end: usize) {
    match end - start {
        0 => output.push(start.to_string()),
        1 => {
            output.push(start.to_string());
            output.push(end.to_string());
        }
        _ => output.push(format!("{start}-{end}")),
    }
}

/// Stripe ids whose header has `flag` set.
fn stripes_with(metadata: &UbiMetadata, flag: u8) -> Vec<usize> {
    metadata
        .stripe_headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| (h & flag != 0).then_some(i))
        .collect()
}

/// The metadata's own lines of the report: version, geometry and one list per
/// header bit. Separate from the device and source lines so a test can check
/// them without a device.
fn describe_metadata(metadata: &UbiMetadata) -> Vec<String> {
    let lists = [
        ("fetched", metadata_flags::FETCHED),
        ("written", metadata_flags::WRITTEN),
        ("has-source", metadata_flags::HAS_SOURCE),
        ("evicted", metadata_flags::EVICTED),
        ("in-s3", metadata_flags::IN_S3),
        ("pushed", metadata_flags::PUSHED),
    ];
    let mut lines = vec![
        format!(
            "metadata version: {}.{}",
            metadata.version_major_u16(),
            metadata.version_minor_u16()
        ),
        format!("stripe size: {} bytes", metadata.stripe_size()),
    ];
    lines.extend(lists.iter().map(|(name, flag)| {
        format!(
            "{name} stripes: {}",
            format_list(&stripes_with(metadata, *flag))
        )
    }));
    lines
}

fn format_source_info(config: &v2::Config) -> Result<String> {
    match config.stripe_source.as_ref() {
        Some(v2::stripe_source::StripeSourceConfig::Raw { image_path, .. }) => {
            let dev = block_device::UringBlockDevice::new(
                image_path.clone(),
                config.tuning.queue_size,
                true,
                true,
                config.tuning.write_through,
            )?;
            let image_size = dev.sector_count() * SECTOR_SIZE as u64;
            Ok(format!(
                "raw (path: {}, size: {} bytes)",
                image_path.display(),
                image_size
            ))
        }
        Some(v2::stripe_source::StripeSourceConfig::Remote(config)) => Ok(format!(
            "remote (address: {}, psk_identity: {})",
            config.address,
            config
                .psk
                .as_ref()
                .map(|psk| psk.identity.as_str())
                .unwrap_or("None")
        )),
        Some(v2::stripe_source::StripeSourceConfig::Archive(config)) => match config {
            v2::stripe_source::ArchiveStorageConfig::Filesystem { path, .. } => {
                Ok(format!("archive filesystem (path: {})", path.display()))
            }
            v2::stripe_source::ArchiveStorageConfig::S3 {
                bucket,
                prefix,
                region,
                ..
            } => Ok(format!(
                "archive s3 (bucket: {bucket}, prefix: {}, region: {})",
                prefix.as_deref().unwrap_or("None"),
                region.as_deref().unwrap_or("None")
            )),
        },
        None => Ok(String::from("None")),
    }
}

fn main() {
    env_logger::builder().format_timestamp(None).init();

    if let Err(err) = run() {
        error!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    let config = load_config(&args.common)?;

    // base data device
    let base_dev = build_block_device(&config.device.data_path, &config, true)?;
    let data_size = base_dev.sector_count() * SECTOR_SIZE as u64;

    let source_info = format_source_info(&config)?;

    // metadata device
    let metadata_path = config.device.metadata_path.as_ref().ok_or_else(|| {
        ubiblk::ubiblk_error!(InvalidParameter {
            description: "metadata_path is none".to_string(),
        })
    })?;
    let metadata_dev = build_block_device(metadata_path, &config, true)?;
    let metadata = UbiMetadata::load_from_bdev(metadata_dev.as_ref())?;

    println!(
        "data file: {} ({} bytes)",
        config.device.data_path.display(),
        data_size
    );
    println!("source: {source_info}");
    for line in describe_metadata(&metadata) {
        println!("{line}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_metadata_lists_new_bits() {
        let mut metadata = UbiMetadata::new(11, 8, 6);
        assert_eq!(metadata.version_minor_u16(), 1, "a 2.1 file");
        metadata.stripe_headers[0] |= metadata_flags::FETCHED;
        metadata.stripe_headers[1] |= metadata_flags::FETCHED | metadata_flags::WRITTEN;
        metadata.stripe_headers[2] |= metadata_flags::EVICTED | metadata_flags::IN_S3;
        metadata.stripe_headers[3] |= metadata_flags::EVICTED | metadata_flags::IN_S3;
        metadata.stripe_headers[4] |= metadata_flags::EVICTED | metadata_flags::PUSHED;
        metadata.stripe_headers[7] |= metadata_flags::PUSHED | metadata_flags::WRITTEN;

        let lines = describe_metadata(&metadata);

        assert_eq!(
            lines,
            vec![
                "metadata version: 2.1".to_string(),
                "stripe size: 1048576 bytes".to_string(),
                "fetched stripes: 0, 1".to_string(),
                "written stripes: 1, 7".to_string(),
                "has-source stripes: 0-5".to_string(),
                "evicted stripes: 2-4".to_string(),
                "in-s3 stripes: 2, 3".to_string(),
                "pushed stripes: 4, 7".to_string(),
            ]
        );
    }

    #[test]
    fn dump_metadata_prints_empty_lists_for_a_clean_file() {
        let metadata = UbiMetadata::new(11, 4, 0);
        let lines = describe_metadata(&metadata);
        assert!(lines.contains(&"evicted stripes: ".to_string()));
        assert!(lines.contains(&"in-s3 stripes: ".to_string()));
        assert!(lines.contains(&"pushed stripes: ".to_string()));
    }

    #[test]
    fn format_list_coalesces_runs() {
        assert_eq!(format_list(&[]), "");
        assert_eq!(format_list(&[3]), "3");
        assert_eq!(format_list(&[3, 4]), "3, 4");
        assert_eq!(format_list(&[3, 4, 5, 9]), "3-5, 9");
    }
}
