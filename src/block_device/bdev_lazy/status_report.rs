use std::sync::atomic::Ordering;

use super::{
    metadata::shared_state::{GATE_FAIL, GATE_HOLD},
    SharedMetadataState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct StripesRecord {
    total: u64,
    source: u64,
    fetched: u64,
}

/// The spill counters as reported over RPC. Present only when `[spill]` is
/// configured.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpillRecord {
    pub resident: u64,
    pub resident_bytes: u64,
    pub max_local_bytes: u64,
    pub evicted: u64,
    pub evicted_clean: u64,
    pub evicted_dirty: u64,
    pub evictions_aborted: u64,
    pub in_s3: u64,
    pub puts: u64,
    pub put_failures: u64,
    pub gets: u64,
    pub get_failures: u64,
    pub put_bytes: u64,
    pub get_bytes: u64,
    pub punches: u64,
    pub punch_failures: u64,
    pub startup_punches: u64,
    pub stalls: u64,
    /// "open" | "hold" | "fail"
    pub gate: String,
    pub degraded: bool,
    pub degraded_reasons: u64,
    pub clean_unrecoverable: u64,
    pub free_bytes: u64,
    pub source_live: bool,
    pub clean_eviction: bool,
    pub encode_ns: u64,
    pub decode_ns: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusReport {
    stripes: StripesRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spill: Option<SpillRecord>,
}

impl StatusReport {
    pub fn new(total: u64, source: u64, fetched: u64) -> Self {
        StatusReport {
            stripes: StripesRecord {
                total,
                source,
                fetched,
            },
            spill: None,
        }
    }

    pub fn spill(&self) -> Option<&SpillRecord> {
        self.spill.as_ref()
    }
}

/// The configured values the report shows next to the live counters.
#[derive(Debug, Clone, Copy)]
pub struct SpillReportConfig {
    pub max_local_bytes: u64,
    pub clean_eviction: bool,
}

#[derive(Debug, Clone)]
pub struct StatusReporter {
    shared_state: SharedMetadataState,
    target_sector_count: u64,
    spill: Option<SpillReportConfig>,
}

impl StatusReporter {
    pub fn new(
        shared_state: SharedMetadataState,
        target_sector_count: u64,
        spill: Option<SpillReportConfig>,
    ) -> Self {
        StatusReporter {
            shared_state,
            target_sector_count,
            spill,
        }
    }

    pub fn report(&self) -> StatusReport {
        let stripe_sector_count = self.shared_state.stripe_sector_count();
        let total_stripes = self.target_sector_count.div_ceil(stripe_sector_count);
        let mut report = StatusReport::new(
            total_stripes,
            self.shared_state.source_stripes(),
            self.shared_state.fetched_stripes(),
        );
        report.spill = self.spill.map(|config| self.spill_record(config));
        report
    }

    fn spill_record(&self, config: SpillReportConfig) -> SpillRecord {
        let state = &self.shared_state;
        let counters = state.spill();
        let load = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed);
        let resident = state.resident_stripes();
        let stripe_size = state.stripe_sector_count() * crate::backends::SECTOR_SIZE as u64;
        SpillRecord {
            resident,
            resident_bytes: resident * stripe_size,
            max_local_bytes: config.max_local_bytes,
            evicted: state.evicted_stripes(),
            evicted_clean: load(&counters.evicted_clean),
            evicted_dirty: load(&counters.evicted_dirty),
            evictions_aborted: load(&counters.evictions_aborted),
            in_s3: state.in_s3_stripes(),
            puts: load(&counters.puts),
            put_failures: load(&counters.put_failures),
            gets: load(&counters.gets),
            get_failures: load(&counters.get_failures),
            put_bytes: load(&counters.put_bytes),
            get_bytes: load(&counters.get_bytes),
            punches: load(&counters.punches),
            punch_failures: load(&counters.punch_failures),
            startup_punches: load(&counters.startup_punches),
            stalls: load(&counters.stalls),
            gate: match state.write_gate() {
                GATE_HOLD => "hold",
                GATE_FAIL => "fail",
                _ => "open",
            }
            .to_string(),
            degraded: counters.degraded.load(Ordering::Relaxed),
            degraded_reasons: load(&counters.degraded_reasons),
            clean_unrecoverable: load(&counters.clean_unrecoverable),
            free_bytes: load(&counters.free_bytes),
            source_live: state.source_live(),
            clean_eviction: config.clean_eviction,
            encode_ns: load(&counters.encode_ns),
            decode_ns: load(&counters.decode_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_device::{metadata_flags, UbiMetadata};

    fn state() -> SharedMetadataState {
        let mut metadata = UbiMetadata::new(3, 8, 4);
        metadata.set_stripe_header(0, metadata_flags::FETCHED | metadata_flags::HAS_SOURCE);
        metadata.set_stripe_header(
            1,
            metadata_flags::EVICTED | metadata_flags::IN_S3 | metadata_flags::HAS_SOURCE,
        );
        metadata.set_stripe_header(5, metadata_flags::WRITTEN);
        SharedMetadataState::new(&metadata)
    }

    #[test]
    fn spill_absent_without_config() {
        let reporter = StatusReporter::new(state(), 64, None);
        let report = reporter.report();
        assert!(report.spill().is_none());

        let json = serde_json::to_value(&report).unwrap();
        assert!(json.get("spill").is_none(), "absent, not null: {json}");
        assert_eq!(json["stripes"]["total"], 8);
        assert_eq!(json["stripes"]["source"], 4);
        assert_eq!(json["stripes"]["fetched"], 1);
        let back: StatusReport = serde_json::from_value(json).unwrap();
        assert_eq!(back, report);
    }

    #[test]
    fn spill_present_with_config() {
        let state = state();
        state.spill().puts.fetch_add(3, Ordering::Relaxed);
        state.spill().put_bytes.fetch_add(4096, Ordering::Relaxed);
        state.spill().free_bytes.store(1 << 30, Ordering::Relaxed);
        state.set_source_live(true);
        let reporter = StatusReporter::new(
            state.clone(),
            64,
            Some(SpillReportConfig {
                max_local_bytes: 1 << 20,
                clean_eviction: true,
            }),
        );

        let report = reporter.report();
        let spill = report.spill().expect("spill record");
        assert_eq!(spill.resident, 2, "one fetched, one written NoSource");
        assert_eq!(spill.resident_bytes, 2 * 8 * 512);
        assert_eq!(spill.max_local_bytes, 1 << 20);
        assert_eq!(spill.evicted, 1);
        assert_eq!(spill.in_s3, 1);
        assert_eq!(spill.puts, 3);
        assert_eq!(spill.put_bytes, 4096);
        assert_eq!(spill.free_bytes, 1 << 30);
        assert_eq!(spill.gate, "open");
        assert!(spill.source_live);
        assert!(spill.clean_eviction);
        assert!(!spill.degraded);
        assert_eq!(spill.stalls, 0);

        state.set_write_gate(GATE_HOLD);
        assert_eq!(reporter.report().spill().unwrap().gate, "hold");
        assert_eq!(reporter.report().spill().unwrap().stalls, 1);
        state.set_write_gate(GATE_FAIL);
        assert_eq!(reporter.report().spill().unwrap().gate, "fail");

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["spill"]["gate"], "open");
        assert_eq!(json["spill"]["evicted"], 1);
        let back: StatusReport = serde_json::from_value(json).unwrap();
        assert_eq!(back, report);
    }
}
