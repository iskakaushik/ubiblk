# Spill end-to-end tests

End-to-end tests for `[spill]` (see `docs/spill.md`): a device whose sparse
`device.raw` sits on a filesystem too small to hold it, driven past the ceiling
with pattern I/O, crashed at the eviction commit points and restarted, and run
against a store that stops accepting objects. They are written in Python and
drive the `ublk-backend`, `init-metadata` and `dump-metadata` binaries; nothing
here is a Rust `cargo test`.

## What is covered

Spec sections 5.8 and 7.3. Every case starts from a fresh 256 MiB sparse
`device.raw` with 128 KiB stripes (2048 of them), a random 256 MiB raw base
image with `copy_on_read = true`, and

```toml
[spill]
max_local_bytes = 33554432      # 32 MiB
low_water_bytes = 4194304
hard_margin_bytes = 4194304
min_free_bytes = 8388608
```

| Case | What it checks |
|------|----------------|
| `steady_state` | 200 MiB of stripe-sized pattern writes in a seeded order, paced at 48 stripes/s, a read back of all of them, a 60 s random 8 KiB read/write mix, a full read of the device. No I/O error; the allocated size of `device.raw` (sampled every 200 ms) never exceeds `max_local_bytes + hard_margin_bytes + 2 stripes`; every byte verifies; `evicted_dirty > 0`, `puts >= in_s3`, `objects <= puts <= objects + gets`, `evicted_dirty + evicted_clean == punches + punch_failures` and `evicted_dirty <= puts - put_failures <= evicted_dirty + evictions_aborted` at quiescence (an eviction a guest Fetch aborts during its PUT counts the PUT and never punches, so `punches == puts` is not an invariant), `stalls == 0`; after a clean stop `dump-metadata` shows every EVICTED stripe with IN_S3, an object and no allocated blocks (`SEEK_DATA`), every FETCHED stripe with allocated blocks, and the counters agreeing with the header bits. |
| `burst_stalls` | `max_concurrent_evictions = 1` and eight unpaced writer threads. The gate closes (`stalls > 0`), nothing errors, every byte verifies, allocation stays within the ceiling plus one stripe per writer thread. |
| `crash_after_put`, `crash_after_header_flush`, `crash_after_punch`, `crash_during_refetch` | With `UBIBLK_SPILL_CRASH_AT` set the backend aborts at that point (`during_refetch` first fills 64 MiB, then touches the written stripes at random until an evicted one is fetched back). The exit is SIGABRT; `dump-metadata` on the dead device shows EVICTED only with IN_S3 and an object; after the restart `startup_punches` equals the number of runs of EVICTED stripes and no EVICTED stripe has allocated blocks; every acknowledged byte verifies; a short random mix and a second full verify pass; `degraded_reasons`, `put_failures` and `get_failures` stay 0. For `after_header_flush` and `during_refetch` the case also confirms that an EVICTED stripe did have allocated blocks before the restart, so the startup pass had work to do. Every acknowledged stripe must have a durable FETCHED or EVICTED header at the crash: with spill the coordinator lands every stripe only once its FETCHED header is durable, so a stripe found with neither is reported as a lost write, not modelled as the base image. |
| `degraded_store_stall` | `on_full = "stall"`. Once evictions are under way the store directory is made immutable (`chattr +i`, which refuses root too), so the next PUT fails and the store goes degraded. The writer blocks with no error, a read of a resident stripe completes while the gate holds, and after `chattr -i` the writer resumes within 5 s. `stalls >= 1`, `put_failures >= 1`, the eviction accounting above, not degraded at the end, every byte verifies, metadata consistent. |
| `degraded_store_fail` | `on_full = "fail"`. Same outage; the writer sees I/O errors while the gate is `fail`, a resident stripe still reads, the gate reopens after the restore and the writer finishes. Then a clean stop, the metadata checks (nothing punched without an object), a restart and a full verify of every acknowledged byte. |
| `s3_steady_state_with_keys`, `s3_crash_after_header_flush_with_keys`, `s3_steady_state_env_keys`, `s3_crash_after_header_flush_env_keys` | The steady-state and crash cases against an S3 endpoint, once with `access_key_id`/`secret_access_key` in the config (via env secrets) and once with both omitted and `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the backend's environment (the default provider chain). Skipped unless `SPILL_E2E_S3_ENDPOINT` is set. |

"Every byte verifies" is literal: the harness remembers the generation of the
last acknowledged write for every 4 KiB block, and a read must return exactly
that pattern, or the base image where nothing was written. A write that
returned an error is not acknowledged and is not expected back.

The clean-eviction scenario of spec 7.3 step 7 (a fork of a live snapshot
with `clean_eviction = true`) is not implemented; it needs a replica.

## How it drives the device

The spec sketches a libblkio client (`tests/blkio/client.c`) over vhost-user.
This harness instead serves the device with `ublk-backend` and does O_DIRECT
`pread`/`pwrite` on `/dev/ublkbN` from Python (page-aligned `mmap` buffers,
one file descriptor per thread), so it needs no C toolchain or libblkio on
the box. Nothing under test depends on the transport: the ublk queue hands
the same `LazyIoChannel` the same reads and writes.

A backend that aborts leaves its ublk device behind until `ublk_drv` is
unloaded; the launcher reloads the module at exit when nothing else is using
it. A clean stop is SIGINT (the backend's Ctrl-C handler deletes the device).

## Files

- `run_all.py` - launcher. Checks for root and the binaries, creates the ext4
  image on a loop device, the work directory on the repo filesystem and the
  base image, runs the cases, and unmounts and deletes everything on exit
  (pass, fail or cancel). `--only <substring>` selects cases, `--keep` keeps
  the work directory with the backend logs.
- `cases.py` - the cases and their harness: the device config, backend
  process control (start, RPC `status`, SIGINT stop), the pattern model and
  O_DIRECT guest, the allocation monitor, `dump-metadata` parsing and the
  `SEEK_DATA` hole check.
- `../common/harness.py`, `../common/util.py` - shared with the other suites
  (`Suite`, `wait_for`, `r`, `toml_dump`).

## Running

Linux with `ublk_drv` (`modprobe ublk_drv`), root, `mkfs.ext4` and `chattr`
(e2fsprogs), python3. Build with the crash hooks compiled in:

```sh
cargo build --features fault-injection --bin ublk-backend --bin init-metadata --bin dump-metadata
sudo -E python3 tests/spill/run_all.py
```

Without `--features fault-injection` the four crash cases fail with
"expected SIGABRT": the hook does not exist in that build. Binaries are taken
from `$CARGO_TARGET_DIR/debug` or `target/debug` (`--target-dir` overrides);
the work directory defaults to `<target-dir>/spill-e2e`. A full run takes
about 15 minutes.

The filesystem store is `FileSystemStore`, whose PUT is a synchronous write,
fsync and rename on the coordinator thread. That, plus one metadata fsync per
eviction on a loop device, caps evictions at a few hundred per second here,
which is why the steady-state writer is paced: an unpaced single writer
outruns the evictor and the gate closes once per eviction (that is what
`burst_stalls` asserts).

### S3 variant

Point the harness at any S3-compatible endpoint and the four `s3_*` cases
run; they need the `aws` CLI to list and purge the run's objects:

```sh
SPILL_E2E_S3_ENDPOINT=http://127.0.0.1:9000 \
SPILL_E2E_S3_BUCKET=spill-e2e \
SPILL_E2E_S3_ACCESS_KEY_ID=... SPILL_E2E_S3_SECRET_ACCESS_KEY=... \
  sudo -E python3 tests/spill/run_all.py --only s3_
```

Optional: `SPILL_E2E_S3_REGION` (default `us-east-1`) and
`SPILL_E2E_S3_PREFIX` (default `spill-e2e/<random>`). A local MinIO is enough:

```sh
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /tmp/minio --address 127.0.0.1:9000 &
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://spill-e2e
```

## CI

`.github/workflows/spill-e2e.yaml` runs the suite on manual dispatch (it is
not part of the default CI matrix): it builds with `--features
fault-injection`, loads `ublk_drv`, starts a MinIO for the S3 cases and runs
the launcher as root.
