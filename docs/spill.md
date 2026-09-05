# Spilling a fork's overlay to an object store

A fork runs on a `bdev_lazy` device whose `data_path` is a sparse overlay that
may be larger than the disk under it. It fills with fetched stripes (clean
copies of a read-only snapshot served by the replica), pushed stripes
(pre-images the replica pushed before overwriting them) and the fork's own
writes. Without `[spill]` a full disk kills the fork with ENOSPC. With it the
local file is a cache with a ceiling: clean stripes are dropped and pulled
again on demand, dirty and pushed stripes are uploaded to the store and then
punched out of the file, and a read of an evicted stripe comes back through
the fetch path from the store or from the replica.

Configuration is in `docs/config.md` (`[spill]`); counters are in `docs/rpc.md`
(`status.spill`). This document is about what the backend does with them.

## Where the data lives

Every stripe is in exactly one of these places, and the metadata says which:

| On-disk header bits | In-memory state | Data is |
|---|---|---|
| `FETCHED` (or `WRITTEN` without a source) | Fetched, NoSource | local, in `data_path` |
| neither `FETCHED` nor `EVICTED`, with a source | NotFetched | in the source; fetched on first access |
| `EVICTED` and `IN_S3` | Evicted | in the store, at `<prefix>/<device_id>/<stripe_index>` |
| `EVICTED` without `IN_S3` | Evicted | in the live snapshot on the replica (a clean eviction) |
| any | Evicting | local, being evicted; guest I/O waits |
| any | Failed | nowhere reachable; guest I/O gets an error |

`PUSHED` marks a stripe the replica pushed. The replica refuses to serve such
a stripe again, so a pushed stripe always counts as dirty and is uploaded
rather than dropped.

The metadata file is format 2.1. A 2.0 file (written by a pre-spill binary)
loads as "nothing evicted, nothing in the store, nothing pushed". At startup
with `[spill]` configured the backend rewrites the version sector to 2.1
before anything else, so a pre-spill binary refuses the file instead of
reading a punched hole as fetched data. `dump-metadata` prints the evicted,
in-s3 and pushed lists next to the fetched and written ones.

## What is evicted, and when

The evictor runs on the background worker thread and checks two limits on
every tick:

- `resident_bytes > max_local_bytes`, where `resident_bytes` is the number of
  stripes occupying local blocks times the stripe size, or
- `free_bytes < min_free_bytes`, from a `statfs` of the filesystem holding
  `data_path`, refreshed at most every 250 ms.

Under either it evicts until `resident_bytes <= max_local_bytes -
low_water_bytes` and `free_bytes >= min_free_bytes`, at most
`max_concurrent_evictions` at a time. Victims come from a CLOCK hand over the
stripe table: a stripe referenced since the hand last passed is skipped once,
a stripe touched by the guest less than a second ago is skipped, a stripe
with guest I/O in flight is drained first.

A stripe is evicted clean only when all of these hold: `clean_eviction = true`,
the stripe is not `WRITTEN` and not `PUSHED`, it has a source, it became
resident in this process while the snapshot subscription was up, and the
subscription is still up. Everything else is evicted dirty, which means an
upload.

## Order of operations

The invariants, in priority order: the metadata never claims more than is
true (never "local" once blocks are punched, never `IN_S3` before the upload
succeeded); nothing is punched while guest I/O to the stripe is in flight;
only the background worker moves a stripe into or out of eviction; a stripe
whose header says `EVICTED` on disk was never handed back to the guest as
resident after that header was written; `IN_S3` is authoritative only while
`EVICTED` is set.

Dirty stripe:

1. Claim the stripe (a compare-and-swap to Evicting). From here new guest I/O
   to it queues on the channel and asks the coordinator for the stripe.
2. Wait until the in-flight count for the stripe is zero.
3. Read the stripe from the local device (through the encryption layer, so
   the object holds plaintext before the spill codec runs).
4. Compress, encrypt with the spill key if a KEK is configured, add the
   object header, upload. Wait for the upload to succeed.
5. Write the header byte `EVICTED | IN_S3` with `FETCHED` cleared, and
   fsync the metadata file. Wait for that to be durable.
6. `fallocate(PUNCH_HOLE | KEEP_SIZE)` over the stripe's byte range.
7. Mark the stripe Evicted in memory and count it.

Clean stripe: steps 3 and 4 are skipped and step 5 writes `EVICTED` with
`FETCHED` and `IN_S3` cleared.

Until step 5 is issued, a guest request for the stripe aborts the eviction
and the stripe stays local. After step 5 the eviction completes even if the
guest asks; the request is replayed once the stripe is Evicted and is served
from the store or the replica. A header write whose outcome is uncertain (the
sector was written but the fsync failed) is retried indefinitely and never
aborted, because the disk may already say `EVICTED`.

Re-materialising an evicted stripe is durable-first: the fetched or pushed
data is written to the local device, the header clearing `EVICTED` is
written and fsynced, and only then is the stripe handed back to guest I/O.
A crash before that fsync leaves a stripe that says `EVICTED` on disk with
its data present; the startup pass punches it and the next access fetches
it again, and no acknowledged write can have landed in the gap.

With `[spill]` configured, a stripe fetched or pushed for the first time
waits for its `FETCHED` header the same way. Without that, a guest write to a
stripe that is resident in memory while its header is still queued would be
acknowledged, and a crash before the header reached the disk would restart
the stripe as missing and fetch the base image over the write. The cost is one
header write and fsync per landed stripe before the guest sees it, shared
between landings that queue while a write for the same metadata sector is in
flight: 508 stripes share a sector, so a burst of neighbouring demand fetches
pays for one or two rather than one each.

Stripes are taken in on the background worker's own thread only: a `[spill]`
section is refused with `tuning.ingest_workers > 1`. A pool worker dequeues
its requests on its own schedule, so a pushed pre-image can be written after
the worker's own pull of the same stripe has landed and the stripe has been
released to the guest. That write is unpinned and lands over whatever the
guest wrote meanwhile, or is uploaded as the fork's data. The coordinator sees
a push only when it forwards it and cannot close that window, so the
configuration is refused instead.

### Crash points

| Crash between | On restart |
|---|---|
| claim and upload | header says local, blocks intact: the stripe is resident, unchanged |
| upload and header write | as above; the object is an orphan, overwritten by a later eviction or removed by `spill-purge` |
| header sector write and fsync | the 512-byte sector holds the old or the new byte, CRC-protected; a torn sector fails the CRC and the device refuses to start rather than guess |
| header fsync and punch | `EVICTED` on disk with blocks allocated: the startup pass punches them |
| punch and in-memory update | as above; the punch is repeated (a no-op) |
| re-fetch data write and header fsync | `EVICTED` on disk with data present: punched at startup, fetched again on access |

The startup pass runs on the background worker thread before the backend
reports itself started, so a device is never offered with allocated blocks
under an `EVICTED` header. It coalesces runs of consecutive evicted stripes
into one `fallocate` call and counts them in `startup_punches`.

## The write gate

Evicting takes time, and a guest can write faster than stripes leave. The
gate is a second, harder limit:

| Condition | Gate |
|---|---|
| `resident_bytes > max_local_bytes + hard_margin_bytes` or `free_bytes < min_free_bytes / 2` | closes: `hold` with `on_full = "stall"`, `fail` with `on_full = "fail"` |
| neither, and `free_bytes >= min_free_bytes` | opens |

While the gate is `hold`, every new guest write queues (resident or not: a
write into a partially allocated stripe allocates too) and reads of resident
stripes pass; requests for non-resident stripes are held and replayed when
the gate opens. While it is `fail`, new writes and queued requests waiting on
a non-resident stripe get an I/O error. Each transition from open is counted
in `stalls` and logged once at warn.

The gate also closes when the store is failing: after an upload fails the
evictor marks itself `degraded`, backs off (1 s doubling to 60 s) and retries;
with `on_full = "stall"` the guest blocks until the store is back, with
`on_full = "fail"` it sees I/O errors. Nothing is punched without a
successful upload either way.

## Clean eviction and the liveness window

A clean stripe can be pulled again only while the replica's snapshot is live
and the stripe has not been pushed. `source_live` starts false, becomes true
on the first successful subscription in this process, is cleared when the
subscription ends for any reason, and is never set again in this process: a
reconnect attaches to whatever generation is live, and pushes missed in the
gap were copied out and are refused on pull. Only stripes that became
resident while `source_live` was true may be evicted clean.

When `source_live` drops with clean-evicted stripes outstanding, their
re-pull is refused fast and counted in `clean_unrecoverable`; the guest sees
an I/O error for those stripes rather than wrong bytes. This window is why
`clean_eviction` defaults to off. With it off, and no store configured,
nothing is ever evicted and only the gate acts.

## Objects and keys

An object is a 36-byte little-endian header followed by the payload: magic
`UBISPILL`, version 1, flags (`ZSTD`, `XTS`), stripe index, plaintext length,
payload length and a CRC32 over header and payload. The payload is the
stripe, compressed with the configured algorithm and then, when a KEK is
configured, encrypted with AES-XTS under a per-device spill key using the
stripe's first sector as the tweak. A mixed-up or corrupted object fails on
the stripe index or the CRC before any plaintext is produced.

The spill key is generated by `init-metadata` when `spill.kek` is set,
wrapped under the KEK with AES-256-GCM and stored at
`<prefix>/<device_id>/spill-key`. The backend fetches and unwraps it once at
startup and refuses to start if it is missing or does not decrypt. Re-running
`init-metadata` writes a fresh key; forks do that with a truncated metadata
file, so no object written under the old key is read again.

`device_id` is part of every object key, which is why the default `"ubiblk"`
is rejected: two devices sharing a bucket and prefix must not share a
namespace.

## Operations

Starting a device with `[spill]` logs one summary line:

```
spill: ceiling 12884901888, low water 536870912, hard margin 268435456, min free 536870912, store s3 pg-ubicloud-ci-forks/forks, clean_eviction off, on_full stall, kek set
```

Watch `status.spill` over RPC. Healthy steady state: `gate == "open"`,
`degraded == false`, `degraded_reasons == 0`, `puts` growing with
`evicted_dirty`, `punches == evicted_dirty + evicted_clean` at quiescence
(every completed eviction was punched; `puts` may run a little ahead of
`evicted_dirty`, by at most `evictions_aborted`, for uploads whose eviction a
guest read then aborted), `resident_bytes` hovering between `max_local_bytes -
low_water_bytes` and `max_local_bytes`.

Things worth an alert:

- `gate != "open"` for more than a few seconds, or `stalls` climbing: the
  evictor cannot keep up with the guest, or the store is slow. Raise
  `max_concurrent_evictions` or the store's `connections`, or lower the
  write rate.
- `degraded == true` or `put_failures` climbing: the store is rejecting
  uploads; check credentials, bucket policy and the store's health.
- `degraded_reasons > 0`: an invariant was violated or an anomaly seen (a
  header with both `FETCHED` and `EVICTED`, an uncertain header write, a
  punch failure, a lost completion). The log has the details at error
  level. The device keeps running conservatively, but the cause needs a
  look.
- `punch_failures > 0` with a filesystem that does not support hole punching
  (`EOPNOTSUPP`): the device stops evicting; move `data_path` to ext4 or
  xfs.
- `clean_unrecoverable > 0`: clean-evicted stripes were lost because the
  snapshot ended. The fork saw I/O errors on those stripes.

`spill-purge --config <toml> [--dry-run]` deletes every object a device may
have written (all stripe indices plus `spill-key`, including orphans left by
a crash between upload and header write). Run it when the fork is destroyed;
it never touches `data_path`.

A `[spill]` section can be added to an existing device: the next start
rewrites the metadata version to 2.1 and eviction begins once the limits are
exceeded. Removing the section from a device that has evicted stripes is
refused at startup (`metadata has N evicted stripe(s) but the config has no
[spill] section`): without it the base image would be fetched over the
punched holes in place of the evicted stripes' data. Restore the section, or
read every evicted stripe back first.

## Metrics reference

All `status.spill` counters are cumulative since the process started, except
`resident`, `resident_bytes`, `evicted`, `in_s3`, `gate`, `degraded`,
`free_bytes` and `source_live`, which are current values. `encode_ns` and
`decode_ns` measure the codec on the background worker and the fetchers
respectively; a 256 KiB stripe costs about a millisecond of zstd, so these
tell you when a thread is warranted before one is added.
