# Configuration Reference

ubiblk uses a TOML configuration file. The root file is typically named
`config.toml` and may include additional files for secrets or stripe source
settings.

## Sections

A config file has these top-level sections:

| Section | Required | Description |
|---------|----------|-------------|
| `[device]` | yes | Device paths and identity |
| `[tuning]` | no | I/O performance knobs (all fields have defaults) |
| `[encryption]` | yes* | Encryption key reference |
| `[danger_zone]` | no | Safety overrides for development |
| `[stripe_source]` | no | Where to fetch stripes from |
| `[spill]` | no | Treat the local disk as a cache with a ceiling; spill the rest to an object store |
| `[secrets.*]` | no | Named secret definitions |

\* Encryption is required unless `danger_zone.allow_unencrypted_disk = true`.

## Include System

The root config can pull in additional TOML files:

```toml
include = ["secrets.toml", "stripe_source.toml"]
```

Rules:
- Paths are relative to the directory containing `config.toml`.
- Append `?` to make an include optional (silently skipped if missing):
  `"stripe_source.toml?"`.
- Included files must not declare their own `include` (no nesting).
- Duplicate key paths across files are rejected.
- Each included file contributes disjoint top-level sections that are merged
  into the root config.

## `[device]`

Core device paths and identity.

```toml
[device]
data_path = "/dev/sda"
metadata_path = "/var/lib/ubiblk/meta"   # optional
vhost_socket = "/var/run/ubiblk.sock"    # optional
rpc_socket = "/var/run/ubiblk-rpc.sock"  # optional
device_id = "vm123"                      # optional, default: "ubiblk"
track_written = false                    # optional, default: false
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `data_path` | path | yes | — | Base block device or file |
| `metadata_path` | path | no | — | Stripe metadata file |
| `vhost_socket` | path | no | — | vhost-user socket path (required for `vhost-backend`) |
| `rpc_socket` | path | no | — | RPC Unix socket path |
| `device_id` | string | no | `"ubiblk"` | Identifier returned to the guest |
| `track_written` | boolean | no | `false` | Track which stripes have been written |

## `[tuning]`

Performance tuning. All fields are optional with sensible defaults.

```toml
[tuning]
num_queues = 4
queue_size = 128
seg_size_max = 65536
seg_count_max = 4
poll_timeout_us = 1000
cpus = [0, 1, 2, 3]
io_engine = "io_uring"
write_through = false
```

| Field | Type | Default | Valid range | Description |
|-------|------|---------|-------------|-------------|
| `num_queues` | integer | 1 | 1–63 | Number of virtqueues |
| `queue_size` | integer | 64 | power of 2, max 65536 | Queue depth |
| `seg_size_max` | integer | 65536 | 1–1048576 | Max I/O segment size (bytes) |
| `seg_count_max` | integer | 4 | 1–256 | Max segments per I/O |
| `poll_timeout_us` | integer | 1000 | 0–10000000 | Poll timeout in microseconds |
| `cpus` | list of integers | none | — | CPU pinning (length must match `num_queues`) |
| `io_engine` | string | `"io_uring"` | `"io_uring"`, `"sync"` | I/O engine |
| `write_through` | boolean | false | — | Enable write-through mode |

## `[encryption]`

Encryption settings. The `xts_key` field must reference a named secret using
the `ref` sub-key.

```toml
[encryption]
xts_key.ref = "xts-key"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `xts_key.ref` | string | yes | Reference to a 64-byte XTS key |

The referenced secret must resolve to exactly 64 bytes (two 32-byte AES keys:
data key and tweak key).

## `[danger_zone]`

Safety overrides for development and testing. The `enabled` flag must be `true`
for any individual bypass to take effect.

```toml
[danger_zone]
enabled = true
allow_unencrypted_disk = true
allow_inline_plaintext_secrets = true
allow_secret_over_regular_file = true
allow_unencrypted_connection = true
allow_env_secrets = true
```

| Flag | Default | Effect when enabled |
|------|---------|---------------------|
| `enabled` | false | Master switch — all other flags are ignored unless this is true |
| `allow_unencrypted_disk` | false | Allow omitting the `[encryption]` section |
| `allow_inline_plaintext_secrets` | false | Allow `source.inline` secrets without a KEK |
| `allow_secret_over_regular_file` | false | Allow reading `source.file` secrets from regular files (not just pipes) |
| `allow_unencrypted_connection` | false | Allow remote connections without PSK |
| `allow_env_secrets` | false | Allow `source.env` secrets (environment variables persist in `/proc/PID/environ`) |

## Secrets

Secrets are declared as sub-tables under `[secrets]`. Each secret has a
`source` and an optional `encrypted_by` reference for envelope encryption.

```toml
[secrets.config-kek]
source.file = "/run/secrets/kek.pipe"

[secrets.xts-key]
source.inline = "TmVjZXNzYXJ5IGJ5dGVzIGhlcmU..."
encoding = "base64"
encrypted_by.ref = "config-kek"
```

### Source types

Each source is specified as a sub-key of `source`:

| Sub-key | Format | Description |
|---------|--------|-------------|
| `source.file` | path | Read secret from a file. Regular files are rejected unless `allow_secret_over_regular_file` is set; prefer named pipes. |
| `source.inline` | string | Inline secret data. Without `encrypted_by`, requires `allow_inline_plaintext_secrets`. |
| `source.env` | string | Read secret from an environment variable. Requires `allow_env_secrets`. |

Secret data must not exceed 8192 bytes after decoding.

**Security note on `source.env`:** Environment variables remain in the process
environment for its entire lifetime and are readable via `/proc/PID/environ` by
the same UID or root. Unlike pipe-based secrets which are consumed and discarded,
env var secrets persist. For this reason, `source.env` requires
`danger_zone.allow_env_secrets` to be enabled. For production use, prefer
`source.file` with a named pipe, which delivers the secret through a
one-time-read channel that leaves no trace in the process environment.

### Secret encoding

Each secret can set an `encoding` field (defaults to `plaintext` when omitted):

| Value | Description |
|-------|-------------|
| `plaintext` | Use the loaded bytes as-is. |
| `base64` | Decode the loaded bytes as base64 to obtain the final secret bytes. |

### KEK encryption

The `encrypted_by` field references another secret that holds a 32-byte AES-256-GCM key.
The source data is then treated as encrypted:

    [12-byte nonce || ciphertext || 16-byte GCM tag]

The secret's key name (e.g., `"xts-key"`) is used as additional authenticated
data (AAD) during decryption. This binds the ciphertext to the specific secret
name.

### Secret references

Config fields reference secrets using a `ref` sub-key:

```toml
[encryption]
xts_key.ref = "xts-key"
```

References inside `[secrets]` (the `encrypted_by` field) are handled through topological
sorting to resolve dependencies in the correct order.

### Resolution rules

1. All secrets are topologically sorted by KEK dependencies.
2. Each secret's source bytes are loaded.
3. If `encrypted_by` is specified, the raw bytes are decrypted using the resolved KEK.
4. Circular KEK dependencies are detected and rejected.

## `[stripe_source]`

Configures where to fetch stripes from. Discriminated by the `type` field.

### Raw

A local raw disk image:

```toml
[stripe_source]
type = "raw"
image_path = "/path/to/image.raw"
autofetch = false         # optional, default: false
copy_on_read = false      # optional, default: false
```

### Archive (filesystem)

An archive stored on the local filesystem:

```toml
[stripe_source]
type = "archive"
storage = "filesystem"
path = "/path/to/archive/root"
archive_kek.ref = "archive-kek"
autofetch = false         # optional, default: false
```

### Archive (S3)

An archive stored in an S3-compatible object store:

```toml
[stripe_source]
type = "archive"
storage = "s3"
bucket = "encrypted-stripes"
prefix = "v1/"                              # optional
region = "eu-west-1"                        # optional
access_key_id.ref = "aws-access-key"
secret_access_key.ref = "aws-secret-key"
session_token.ref = "aws-session-token"      # optional
archive_kek.ref = "archive-kek"
endpoint = "https://s3.example.com"         # optional
connections = 16                            # optional, default: 16
autofetch = false                           # optional, default: false
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bucket` | string | yes | — | S3 bucket name |
| `prefix` | string | no | — | Key prefix (must not contain `.` or `..` path components) |
| `region` | string | no | — | AWS region |
| `access_key_id.ref` | string | no | — | Reference to AWS access key ID secret. Set together with `secret_access_key.ref`, or omit both to use the SDK's default provider chain (instance role, `AWS_*` in the environment) |
| `secret_access_key.ref` | string | no | — | Reference to AWS secret access key secret |
| `session_token.ref` | string | no | — | Reference to AWS session token secret (for temporary credentials); needs both keys |
| `archive_kek.ref` | string | yes | — | Reference to 32-byte archive KEK secret |
| `endpoint` | string | no | — | Custom S3 endpoint URL |
| `connections` | integer | no | 16 | Number of S3 connections (must be > 0) |
| `autofetch` | boolean | no | false | Fetch stripes in the background |
| `connect_timeout_ms` | integer | no | 5000 | S3 connection timeout in milliseconds |
| `operation_attempt_timeout_ms` | integer | no | 20000 | S3 operation attempt timeout in milliseconds |
| `max_attempts` | integer | no | 3 | Max S3 operation attempts (initial attempt + retries) |
| `rate_limited_retry` | table | no | disabled | Jittered retry delay for rate-limited responses. See below. |

#### `rate_limited_retry`

By default, retries use the AWS SDK's jittered exponential backoff: retry _n_
waits a random duration in `[0, min(1s · 2ⁿ, 20s))`, so the first retry can fire
almost immediately. When the object store rate-limits rapid retries to the same
key, that near-instant retry can be rejected (some return `429`, which the SDK
does not retry by default) and fail the whole archive.

When `enabled`, a response with a transient status (`500`/`502`/`503`/`504`) or a
throttling status (`429`) is instead retried after `min_delay_ms + rand[0,
jitter_ms)` — a constant floor plus jitter, rather than exponential backoff.

```toml
[stripe_source.rate_limited_retry]
enabled      = true
min_delay_ms = 1500
jitter_ms    = 1500
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | false | Turn the rate-limited retry delay on |
| `min_delay_ms` | integer | 0 | Floor: minimum delay before a retry (must be > 0 when enabled) |
| `jitter_ms` | integer | 0 | Width of the random jitter added on top of `min_delay_ms` (0 = no jitter) |

It applies to every S3 operation of the client it is set on. Only responses with
an HTTP status are delayed — client-side
timeouts and connection failures use the SDK's normal backoff. The delay is flat,
not exponential: a retry classifier cannot see the attempt number, so it cannot
grow the delay per attempt — but a constant floor is what avoids the rate limit.

### Remote

A remote stripe server over TLS-PSK:

```toml
[stripe_source]
type = "remote"
address = "1.2.3.4:4555"
autofetch = false         # optional, default: false

[stripe_source.psk]
identity = "client1"
secret.ref = "psk-secret"
```

PSK is required unless `danger_zone.allow_unencrypted_connection` is enabled.
The PSK secret must be at least 16 bytes.

## `[spill]`

Turns the local `data_path` into a cache with a ceiling for a device that
forks another one (see `docs/spill.md`). Once resident stripes exceed the
ceiling, or the filesystem holding `data_path` runs low on free space, the
background worker evicts stripes: clean copies of the live snapshot are
dropped and pulled again on demand (only with `clean_eviction = true`), and
everything else (written stripes, pushed pre-images) is uploaded to
`[spill.store]` and then punched out of the file with `fallocate`. Reads of an
evicted stripe come back through the fetch path, from the store or from the
snapshot source.

```toml
[device]
device_id = "fork-3f9c"          # required: part of every object key
track_written = true             # required
snapshot_source = "10.0.1.20:9500"

[spill]
max_local_bytes = 12884901888    # required; ceiling on resident stripes * stripe size
low_water_bytes = 536870912      # default 512 MiB; evict down to max_local_bytes - low_water_bytes
hard_margin_bytes = 268435456    # default 256 MiB; gate writes above max_local_bytes + hard_margin_bytes
min_free_bytes = 536870912       # default 512 MiB; statfs watermark on data_path's filesystem
on_full = "stall"                # default; or "fail"
clean_eviction = false           # default; needs snapshot_source
max_concurrent_evictions = 4     # default 4; also bounds uploads in flight
compression = { zstd = { level = 3 } }   # default; or "none"
# kek = { ref = "spill-kek" }    # optional; 32 bytes; encrypts objects with AES-XTS

[spill.store]                    # optional; absent means clean-only
storage = "s3"
bucket = "pg-ubicloud-ci-forks"
prefix = "forks"
region = "us-west-2"
connections = 16
# access_key_id / secret_access_key omitted: default provider chain
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `max_local_bytes` | integer (bytes) | yes | none | Ceiling on resident stripes times the stripe size |
| `low_water_bytes` | integer (bytes) | no | 536870912 | Once over the ceiling, evict down to `max_local_bytes - low_water_bytes`; must be below `max_local_bytes` |
| `hard_margin_bytes` | integer (bytes) | no | 268435456 | Guest writes are gated above `max_local_bytes + hard_margin_bytes` |
| `min_free_bytes` | integer (bytes) | no | 536870912 | Evict while the filesystem has less free space than this; gate writes below half of it |
| `on_full` | string | no | `"stall"` | What a guest write meets while the gate is closed: `"stall"` queues it, `"fail"` returns an I/O error |
| `clean_eviction` | boolean | no | false | Drop clean stripes the live snapshot can serve again instead of uploading them. Needs `device.snapshot_source` |
| `max_concurrent_evictions` | integer | no | 4 | Evictions, and so uploads, in flight at once (1 to 64) |
| `compression` | table or string | no | `{ zstd = { level = 3 } }` | Compression applied to objects before encryption; `"none"` to disable |
| `kek.ref` | string | no | none | 32-byte key-encryption key. `init-metadata` generates a random XTS key, wraps it under the KEK and stores it at `<prefix>/<device_id>/spill-key`; the backend unwraps it at startup |
| `store` | table | no | none | Where dirty stripes go. Absent means clean-only, which requires `clean_eviction = true` |

Sizes are plain integers in bytes; there is no unit suffix parser.

### `[spill.store]`

The same shape as an archive storage config (`storage = "s3"` or
`storage = "filesystem"` with `path`), except that `archive_kek` and
`autofetch` are not accepted. Objects are written as
`<prefix>/<device_id>/<stripe_index>`. For S3, `connections` is the number
of download workers each fetcher gets; the uploader gets
`min(connections, max_concurrent_evictions)`.

The S3 credentials are optional: omit both `access_key_id` and
`secret_access_key` to use the SDK's default provider chain, which is the
instance role through IMDS on EC2 or `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` in the environment. Note that the IMDS default hop
limit of 1 does not reach a bridge-networked container; raise the hop limit
on the instance or pass explicit keys there.

A `storage = "filesystem"` store is meant for tests; put it on a different
filesystem than `data_path`, or evicting frees no space.

### Validation

Every rule below is rejected when the config is loaded:

- `device.metadata_path` must be set, `device.track_written` must be `true`,
  and `device.device_id` must not be the default `"ubiblk"`.
- `device.snapshot_server` must not be set on the same device: a served
  stripe may be a hole.
- A `[stripe_source]`, if present, must have `copy_on_read = true` and
  `autofetch = false`.
- `clean_eviction = true` needs `device.snapshot_source`; a missing `store`
  needs `clean_eviction = true`.
- `low_water_bytes < max_local_bytes`; `1 <= max_concurrent_evictions <= 64`.
- `kek` must resolve to exactly 32 bytes.
- `tuning.num_queues * tuning.queue_size` must not exceed 65535.

At startup the backend additionally refuses a `data_path` that is not a
regular file (a block device has nothing to punch), and `init-metadata`
preallocates the metadata file so header writes never meet ENOSPC.

## Example Configs

### Development (plaintext, no encryption)

A single-file config for local development:

```toml
[device]
data_path = "/tmp/dev-disk.raw"

[danger_zone]
enabled = true
allow_unencrypted_disk = true
allow_inline_plaintext_secrets = true
allow_secret_over_regular_file = true
```

### Production (KEK-encrypted secrets, layered files)

Split secrets into a separate file:

**config.toml:**
```toml
include = ["secrets.toml"]

[device]
data_path = "/dev/sda"
metadata_path = "/var/lib/ubiblk/meta"
vhost_socket = "/var/run/ubiblk.sock"
rpc_socket = "/var/run/ubiblk-rpc.sock"

[tuning]
num_queues = 4
queue_size = 128

[encryption]
xts_key.ref = "xts-key"
```

**secrets.toml:**
```toml
[secrets.config-kek]
source.file = "/run/secrets/kek.pipe"

[secrets.xts-key]
source.inline = "<AES-256-GCM encrypted XTS key>"
encoding = "base64"
encrypted_by.ref = "config-kek"
```

### Archive stripe source with S3

**config.toml:**
```toml
include = ["secrets.toml", "stripe_source.toml"]

[device]
data_path = "/dev/nvme0n1"
metadata_path = "/var/lib/ubiblk/meta"
vhost_socket = "/var/run/ubiblk.sock"
rpc_socket = "/var/run/ubiblk-rpc.sock"

[encryption]
xts_key.ref = "xts-key"
```

**secrets.toml:**
```toml
[secrets.config-kek]
source.file = "/run/secrets/kek.pipe"

[secrets.xts-key]
source.inline = "<encrypted XTS key>"
encoding = "base64"
encrypted_by.ref = "config-kek"

[secrets.archive-kek]
source.inline = "<encrypted archive KEK>"
encoding = "base64"
encrypted_by.ref = "config-kek"

[secrets.aws-access-key]
source.env = "AWS_ACCESS_KEY_ID"

[secrets.aws-secret-key]
source.env = "AWS_SECRET_ACCESS_KEY"
```

The S3 credentials above use `source.env`, so the config must include:

```toml
[danger_zone]
enabled = true
allow_env_secrets = true
```

**stripe_source.toml:**
```toml
[stripe_source]
type = "archive"
storage = "s3"
bucket = "encrypted-stripes"
prefix = "v1/"
region = "eu-west-1"
access_key_id.ref = "aws-access-key"
secret_access_key.ref = "aws-secret-key"
session_token.ref = "aws-session-token"      # optional
archive_kek.ref = "archive-kek"
autofetch = true
```

### Remote stripe source

**config.toml:**
```toml
include = ["secrets.toml", "stripe_source.toml"]

[device]
data_path = "/dev/sda"
metadata_path = "/var/lib/ubiblk/meta"
vhost_socket = "/var/run/ubiblk.sock"
rpc_socket = "/var/run/ubiblk-rpc.sock"

[encryption]
xts_key.ref = "xts-key"
```

**stripe_source.toml:**
```toml
[stripe_source]
type = "remote"
address = "10.0.0.1:4555"
autofetch = true

[stripe_source.psk]
identity = "client1"
secret.ref = "psk-secret"
```
