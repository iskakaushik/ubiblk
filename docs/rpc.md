# RPC Commands

When `rpc_socket` is configured, the backend accepts newline-delimited JSON
requests on the Unix socket and returns one JSON object per line.

## Request format

All requests use the same shape:

```json
{"command": "<name>"}
```

## `version`

Returns the backend version.

**Request**

```json
{"command": "version"}
```

**Output spec**

- Top-level object with:
  - `version` (string): backend version string.

**Example response**

```json
{"version":"0.1.0"}
```

## `status`

Returns the background worker status report.

**Request**

```json
{"command": "status"}
```

**Output spec**

- Top-level object with:
  - `status` (object or `null`):
    - `null` when no background worker is active.
    - Otherwise, a status object from the backend reporter:
      - `stripes` (object):
        - `total` (u64): stripes on the device.
        - `source` (u64): stripes the stripe source holds.
        - `fetched` (u64): source stripes currently resident on the local
          device. With `[spill]` configured this moves both ways: an
          eviction decrements it and a re-fetch increments it again.
      - `spill` (object): present only when `[spill]` is configured. See
        below.

**Example response**

```json
{
  "status": {
    "stripes": {
      "fetched": 265,
      "source": 3584,
      "total": 40960
    }
  }
}
```

**`status.spill` fields**

| Field | Type | Meaning |
|-------|------|---------|
| `resident` | u64 | Stripes occupying local blocks (fetched, or written without a source) |
| `resident_bytes` | u64 | `resident` times the stripe size |
| `max_local_bytes` | u64 | The configured ceiling |
| `evicted` | u64 | Stripes currently evicted from the local device |
| `evicted_clean` | u64 | Evictions that dropped a clean stripe (no upload) |
| `evicted_dirty` | u64 | Evictions that uploaded the stripe first |
| `evictions_aborted` | u64 | Evictions abandoned because the guest touched the stripe in time |
| `in_s3` | u64 | Evicted stripes whose data is in the store |
| `puts`, `put_failures`, `put_bytes` | u64 | Uploads started, failed, and bytes uploaded |
| `gets`, `get_failures`, `get_bytes` | u64 | Downloads started, failed, and bytes downloaded |
| `punches`, `punch_failures` | u64 | `fallocate` hole punches after an eviction, and failures |
| `startup_punches` | u64 | Runs of evicted stripes punched again by the startup pass |
| `stalls` | u64 | Times the write gate closed |
| `gate` | string | `"open"`, `"hold"` (writes queue) or `"fail"` (writes fail) |
| `degraded` | bool | The store is failing uploads; evictions back off |
| `degraded_reasons` | u64 | Anomalies logged at error level (see `docs/spill.md`) |
| `clean_unrecoverable` | u64 | Re-pulls of clean-evicted stripes refused because the snapshot ended or the stripe was pushed |
| `free_bytes` | u64 | Last `statfs` of the filesystem holding `data_path` |
| `source_live` | bool | The snapshot subscription is up, so clean stripes can be re-pulled |
| `clean_eviction` | bool | The configured `clean_eviction` flag |
| `encode_ns`, `decode_ns` | u64 | Time spent compressing and encrypting objects, and the reverse |

```json
{
  "status": {
    "stripes": { "fetched": 265, "source": 3584, "total": 40960 },
    "spill": {
      "resident": 12288,
      "resident_bytes": 12884901888,
      "max_local_bytes": 12884901888,
      "evicted": 512,
      "evicted_clean": 0,
      "evicted_dirty": 512,
      "evictions_aborted": 3,
      "in_s3": 512,
      "puts": 515,
      "put_failures": 0,
      "gets": 40,
      "get_failures": 0,
      "put_bytes": 71303168,
      "get_bytes": 5242880,
      "punches": 512,
      "punch_failures": 0,
      "startup_punches": 0,
      "stalls": 0,
      "gate": "open",
      "degraded": false,
      "degraded_reasons": 0,
      "clean_unrecoverable": 0,
      "free_bytes": 3221225472,
      "source_live": true,
      "clean_eviction": false,
      "encode_ns": 612345678,
      "decode_ns": 40123456
    }
  }
}
```

## `queues`

Returns a per-queue snapshot of recently observed I/O activity.

**Request**

```json
{"command": "queues"}
```

**Output spec**

- Top-level object with:
  - `queues` (array): one entry per queue.
    - Each queue entry is an array of I/O events.
    - Event shapes:
      - `["read", offset, length]`
      - `["write", offset, length]`
      - `["flush"]`

**Example response**

```json
{
  "queues": [
    [
      ["read", 0, 4096],
      ["write", 8192, 4096],
      ["flush"]
    ],
    [
      ["read", 16384, 4096]
    ]
  ]
}
```

## `stats`

Returns cumulative counters for each queue.

**Request**

```json
{"command": "stats"}
```

**Output spec**

- Top-level object with:
  - `stats` (object):
    - `queues` (array): one object per queue, each containing:
      - `bytes_read` (u64)
      - `bytes_written` (u64)
      - `read_ops` (u64)
      - `write_ops` (u64)
      - `flush_ops` (u64)

**Example response**

```json
{
  "stats": {
    "queues": [
      {
        "bytes_read": 4096,
        "bytes_written": 8192,
        "read_ops": 1,
        "write_ops": 2,
        "flush_ops": 1
      }
    ]
  }
}
```

## Unknown command handling

If `command` is not recognized, the backend returns an error object.

**Example response**

```json
{"error":"unknown command: destroy_world"}
```
