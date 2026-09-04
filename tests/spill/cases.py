"""Spill end-to-end cases: the ENOSPC ceiling, the crash matrix, a degraded store.

Each case builds a fresh 256 MiB sparse ``device.raw`` (128 KiB stripes, 2048 of
them) on a small loop-mounted ext4 filesystem that ``run_all.py`` provides, so
running out of space is real, and a ``[spill]`` section with a 32 MiB ceiling
whose store lives on another filesystem. The device is served by
``ublk-backend`` and driven through ``/dev/ublkbN`` with O_DIRECT pattern I/O
from this process (libblkio is not assumed to be installed). Every acknowledged
write is remembered per 4 KiB block, so "every byte verifies" means exactly the
bytes the guest was told it wrote, or the base image where it wrote nothing.

Normally run via ``run_all.py``; ``Cases(fixture).run()`` returns a non-zero
exit code if any case fails.
"""

import errno
import json
import mmap
import os
import pathlib
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "common"))

from util import CommandFail, r, toml_dump  # noqa: E402
from harness import Suite  # noqa: E402

BLOCK = 4096
STRIPE = 128 * 1024
BLOCKS_PER_STRIPE = STRIPE // BLOCK
DEVICE_BYTES = 256 << 20
STRIPES = DEVICE_BYTES // STRIPE
TOTAL_BLOCKS = DEVICE_BYTES // BLOCK
DEVICE_ID = "e2e"

MAX_LOCAL = 32 << 20
LOW_WATER = 4 << 20
HARD_MARGIN = 4 << 20
MIN_FREE = 8 << 20
# Spec 7.3 step 4: the allocated size of device.raw never exceeds this.
CEILING = MAX_LOCAL + HARD_MARGIN + 2 * STRIPE

# 200 MiB of pattern writes (spec 7.3 step 3), stripe-sized, in a seeded order.
SEQ_STRIPES = 1600

CRASH_POINTS = ("after_put", "after_header_flush", "after_punch", "during_refetch")


class Fixture:
    """Where everything lives. Built by run_all.py.

    ``a_dir`` is the mount point of the small ext4 image (holds device.raw and
    the metadata file, nothing else). ``b_dir`` is on the repo filesystem and
    holds the base image, the spill store, sockets, the device symlink and the
    backend logs. ``s3`` is None or a dict with endpoint, bucket, region,
    access_key_id and secret_access_key for the S3 variant.
    """

    def __init__(self, bin_dir, a_dir, b_dir, s3=None):
        self.bin_dir = pathlib.Path(bin_dir)
        self.a_dir = pathlib.Path(a_dir)
        self.b_dir = pathlib.Path(b_dir)
        self.s3 = s3
        self.device_raw = self.a_dir / "device.raw"
        self.device_meta = self.a_dir / "device.meta"
        self.base_img = self.b_dir / "base.img"
        self.spill_dir = self.b_dir / "spill"
        self.run_dir = self.b_dir / "run"
        self.config = self.run_dir / "config.toml"
        self.rpc_sock = self.run_dir / "rpc.sock"
        self.dev_link = self.run_dir / "dev"

    def bin(self, name):
        return str(self.bin_dir / name)


class GuestIoError(RuntimeError):
    """The guest saw an I/O error where none was allowed."""


class Mismatch(AssertionError):
    """Read data that does not match what was acknowledged."""


def rpc(sock_path, command, timeout=5.0):
    """One newline-delimited JSON request on the backend's RPC socket."""
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(timeout)
    try:
        s.connect(str(sock_path))
        s.sendall(json.dumps({"command": command}).encode() + b"\n")
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    finally:
        s.close()
    return json.loads(buf)


def runs_of(ids):
    """Number of runs of consecutive ids (what the startup pass counts)."""
    ids = sorted(ids)
    return sum(1 for i, s in enumerate(ids) if i == 0 or s != ids[i - 1] + 1)


def parse_ranges(text):
    """``"0-63, 482-651, 2035"`` -> set of ints. Empty text -> empty set."""
    out = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-")
            out.update(range(int(lo), int(hi) + 1))
        else:
            out.add(int(part))
    return out


class Model:
    """What the guest expects to read back.

    Per 4 KiB block: the generation of the last acknowledged write, or nothing,
    in which case the block reads as the base image. Block content is a pure
    function of (seed, block, generation) with a small tag in front so a
    mismatch names the block and generation that were found.
    """

    TAG = 0x4C495053  # "SPIL"

    def __init__(self, seed, base_img):
        self.seed = seed
        self.gen = {}
        self._next_gen = 1
        self._lock = threading.Lock()
        self._base_fd = os.open(str(base_img), os.O_RDONLY)

    def close(self):
        os.close(self._base_fd)

    def new_gen(self):
        with self._lock:
            g = self._next_gen
            self._next_gen += 1
            return g

    def pattern(self, block, gen):
        rnd = random.Random((self.seed << 48) ^ (gen << 24) ^ block)
        body = bytearray(rnd.randbytes(BLOCK))
        body[:16] = struct.pack("<IIII", self.TAG, block, gen, self.seed & 0xFFFFFFFF)
        return bytes(body)

    def expected(self, block):
        gen = self.gen.get(block)
        if gen is None:
            return os.pread(self._base_fd, BLOCK, block * BLOCK)
        return self.pattern(block, gen)

    def acknowledge(self, block, count, gen):
        for b in range(block, block + count):
            self.gen[b] = gen

    def acknowledged_stripes(self):
        return {b // BLOCKS_PER_STRIPE for b in self.gen}

    def forget_stripe(self, stripe):
        """Expect the base image again for a stripe the device lost."""
        for b in range(stripe * BLOCKS_PER_STRIPE, (stripe + 1) * BLOCKS_PER_STRIPE):
            self.gen.pop(b, None)

    def describe(self, block, data):
        """Say what a mismatching block looks like, for the failure message."""
        tag, blk, gen, _ = struct.unpack("<IIII", data[:16])
        if tag == self.TAG:
            return f"pattern block {blk} gen {gen}"
        if data == os.pread(self._base_fd, BLOCK, block * BLOCK):
            return "base image content"
        if not any(data):
            return "all zeroes"
        return "unrecognised bytes"


class Guest:
    """O_DIRECT I/O on the ublk device, checked against a Model.

    One instance per thread: it owns a file descriptor and a page-aligned
    buffer. Failed operations are counted, never raised, so a case decides
    whether an I/O error was expected.
    """

    def __init__(self, dev_path, model, max_io=STRIPE):
        self.fd = os.open(str(dev_path), os.O_RDWR | os.O_DIRECT)
        self.buf = mmap.mmap(-1, max_io)
        self.model = model
        self.errors = 0
        self.last_error = None
        self.ops = 0

    def close(self):
        if self.fd is None:
            return
        self.buf.close()
        os.close(self.fd)
        self.fd = None

    def _fail(self, e):
        self.errors += 1
        self.last_error = e

    def write_blocks(self, block, count):
        gen = self.model.new_gen()
        data = b"".join(self.model.pattern(b, gen) for b in range(block, block + count))
        self.buf[: len(data)] = data
        try:
            n = os.pwrite(self.fd, memoryview(self.buf)[: len(data)], block * BLOCK)
        except OSError as e:
            self._fail(e)
            return False
        if n != len(data):
            raise RuntimeError(f"short write: {n} of {len(data)} at block {block}")
        self.model.acknowledge(block, count, gen)
        self.ops += 1
        return True

    def read_blocks(self, block, count):
        size = count * BLOCK
        try:
            n = os.preadv(self.fd, [memoryview(self.buf)[:size]], block * BLOCK)
        except OSError as e:
            self._fail(e)
            return None
        if n != size:
            raise RuntimeError(f"short read: {n} of {size} at block {block}")
        self.ops += 1
        return bytes(self.buf[:size])

    def verify_blocks(self, block, count):
        """Read and compare. Returns False on an I/O error, raises Mismatch on
        wrong bytes."""
        data = self.read_blocks(block, count)
        if data is None:
            return False
        for i in range(count):
            got = data[i * BLOCK : (i + 1) * BLOCK]
            want = self.model.expected(block + i)
            if got != want:
                b = block + i
                raise Mismatch(
                    f"block {b} (stripe {b // BLOCKS_PER_STRIPE}): expected "
                    f"{self.model.describe(b, want)}, read {self.model.describe(b, got)}"
                )
        return True

    def write_stripe(self, stripe):
        return self.write_blocks(stripe * BLOCKS_PER_STRIPE, BLOCKS_PER_STRIPE)

    def verify_stripe(self, stripe):
        return self.verify_blocks(stripe * BLOCKS_PER_STRIPE, BLOCKS_PER_STRIPE)


class Writer(threading.Thread):
    """Writes whole stripes in the given order, optionally paced.

    ``progress`` is the number of acknowledged stripes and ``acks`` the
    (monotonic time, stripe) of each, so a case can tell when the guest was
    stalled and which stripes are certainly resident.
    """

    ERROR_PAUSE = 0.01

    def __init__(self, guest, stripes, rate=None, stop_on_error=True):
        super().__init__(daemon=True)
        self.guest = guest
        self.stripes = list(stripes)
        self.rate = rate
        self.stop_on_error = stop_on_error
        self.progress = 0
        self.errors = 0
        self.acks = []
        self.halt = threading.Event()
        self.exc = None

    def run(self):
        try:
            start = time.monotonic()
            for i, stripe in enumerate(self.stripes):
                if self.halt.is_set():
                    return
                if self.rate:
                    wait = start + i / self.rate - time.monotonic()
                    if wait > 0:
                        time.sleep(wait)
                if self.guest.write_stripe(stripe):
                    self.progress += 1
                    self.acks.append((time.monotonic(), stripe))
                else:
                    self.errors += 1
                    if self.stop_on_error:
                        return
                    # A failed write returns at once; without a pause the
                    # whole order would be burnt through before the store
                    # is restored.
                    time.sleep(self.ERROR_PAUSE)
        except Exception as e:  # surfaced by the case through join()
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc is not None:
            raise self.exc


class AllocMonitor(threading.Thread):
    """Samples the allocated size of device.raw every 200 ms (spec 7.3 step 4)."""

    def __init__(self, path, interval=0.2):
        super().__init__(daemon=True)
        self.path = str(path)
        self.interval = interval
        self.max = 0
        self.samples = 0
        self._halt = threading.Event()

    def sample(self):
        try:
            alloc = os.stat(self.path).st_blocks * 512
        except FileNotFoundError:
            return
        self.samples += 1
        self.max = max(self.max, alloc)

    def run(self):
        while not self._halt.wait(self.interval):
            self.sample()

    def finish(self):
        self._halt.set()
        self.join()
        self.sample()
        return self.max


class Backend:
    """One ublk-backend process serving the fixture's device."""

    def __init__(self, fx, log_path, env=None):
        self.fx = fx
        self.log_path = pathlib.Path(log_path)
        self.env = env or {}
        self.proc = None
        self.dev = None
        self._log = None

    def start(self, timeout=60.0):
        fx = self.fx
        for stale in (fx.dev_link, fx.rpc_sock):
            if stale.is_symlink() or stale.exists():
                stale.unlink()
        self._log = open(self.log_path, "ab")
        env = {**os.environ, "RUST_LOG": "info", **self.env}
        self.proc = subprocess.Popen(
            [fx.bin("ublk-backend"), "-f", str(fx.config), "--device-symlink", str(fx.dev_link)],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        deadline = time.monotonic() + timeout
        while True:
            rc = self.proc.poll()
            if rc is not None:
                raise RuntimeError(f"backend exited during startup (rc={rc}):\n{self.log_tail()}")
            # The symlink appears once the kernel device exists, which the
            # backend does after the startup punch pass and the RPC server.
            if fx.dev_link.exists() and fx.rpc_sock.exists():
                try:
                    self.spill()
                    break
                except (OSError, KeyError, json.JSONDecodeError):
                    pass
            if time.monotonic() > deadline:
                raise RuntimeError(f"backend did not come up in {timeout}s:\n{self.log_tail()}")
            time.sleep(0.05)
        self.dev = os.path.realpath(fx.dev_link)
        return self

    def status(self):
        return rpc(self.fx.rpc_sock, "status")["status"]

    def spill(self):
        return self.status()["spill"]

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait(self, timeout):
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def stop(self, timeout=30.0):
        """Clean shutdown: SIGINT deletes the ublk device and exits."""
        if self.proc is None:
            return None
        if self.alive():
            self.proc.send_signal(signal.SIGINT)
            if self.wait(timeout) is None:
                self.proc.kill()
                self.proc.wait()
        if self._log:
            self._log.close()
            self._log = None
        if self.fx.dev_link.is_symlink():
            self.fx.dev_link.unlink()
        return self.proc.returncode

    def kill(self):
        if self.alive():
            self.proc.kill()
            self.proc.wait()
        if self._log:
            self._log.close()
            self._log = None

    def log_text(self):
        try:
            return self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return ""

    def log_tail(self, lines=15):
        return "\n".join(self.log_text().splitlines()[-lines:])


class Cases(Suite):
    def __init__(self, fx):
        super().__init__()
        self.fx = fx
        self.model = None
        self.backend = None
        self.case_name = None
        self._guests = []
        self._writers = []

    def track(self, writer):
        self._writers.append(writer)
        return writer

    def quiesce_guests(self):
        """Stop every guest thread and close every descriptor before the
        backend is torn down. An in-flight guest write when ublk's del_gendisk
        runs deadlocks the device teardown (it freezes the queue and waits for
        a bio the exiting backend will never complete), so no I/O may be
        outstanding at stop time. Returns True if every writer stopped."""
        clean = True
        for w in self._writers:
            w.halt.set()
        for w in self._writers:
            try:
                w.join(30)
            except Exception:
                pass
            if w.is_alive():
                clean = False
        for g in self._guests:
            try:
                g.close()
            except OSError:
                pass
        self._writers = []
        self._guests = []
        return clean

    # --- reporting -----------------------------------------------------------

    def report(self, name, problems, notes=()):
        for note in notes:
            print(f"     {name}: {note}")
        if problems:
            self.notok(name, "; ".join(problems))
        else:
            self.ok(name)

    @staticmethod
    def check(problems, condition, message):
        if not condition:
            problems.append(message)
        return condition

    # --- fixture -------------------------------------------------------------

    def setup(self):
        fx = self.fx
        fx.run_dir.mkdir(parents=True, exist_ok=True)
        fx.spill_dir.mkdir(parents=True, exist_ok=True)
        if not fx.base_img.exists() or fx.base_img.stat().st_size != DEVICE_BYTES:
            with open(fx.base_img, "wb") as f:
                for _ in range(DEVICE_BYTES >> 20):
                    f.write(os.urandom(1 << 20))

    def store_dir(self):
        return self.fx.spill_dir / DEVICE_ID

    def restore_store(self):
        """Undo a degraded-store case's chattr +i (best effort)."""
        d = self.store_dir()
        if d.exists():
            subprocess.run(["chattr", "-i", str(d)], capture_output=True)

    def write_config(self, on_full="stall", max_concurrent_evictions=4, s3_keys=None):
        """Write the device config. ``s3_keys`` is None for the filesystem
        store, "config" for S3 with keys in the config (via env secrets), or
        "env" for S3 with the keys omitted (default provider chain)."""
        fx = self.fx
        tables = [
            ("device", {
                "data_path": str(fx.device_raw),
                "metadata_path": str(fx.device_meta),
                "rpc_socket": str(fx.rpc_sock),
                "device_id": DEVICE_ID,
                "track_written": True,
            }),
            ("tuning", {
                "num_queues": 4,
                "queue_size": 64,
                "seg_size_max": STRIPE,
                "seg_count_max": 4,
                "poll_timeout_us": 1000,
                "io_engine": "io_uring",
            }),
            ("stripe_source", {
                "type": "raw",
                "image_path": str(fx.base_img),
                "copy_on_read": True,
                "autofetch": False,
            }),
            ("spill", {
                "max_local_bytes": MAX_LOCAL,
                "low_water_bytes": LOW_WATER,
                "hard_margin_bytes": HARD_MARGIN,
                "min_free_bytes": MIN_FREE,
                "on_full": on_full,
                "max_concurrent_evictions": max_concurrent_evictions,
            }),
        ]
        danger = {"enabled": True, "allow_unencrypted_disk": True}
        if s3_keys is None:
            tables.append(("spill.store", {"storage": "filesystem", "path": str(fx.spill_dir)}))
        else:
            s3 = fx.s3
            store = {
                "storage": "s3",
                "bucket": s3["bucket"],
                "prefix": s3["prefix"],
                "region": s3["region"],
                "endpoint": s3["endpoint"],
                "connections": 4,
            }
            if s3_keys == "config":
                store["access_key_id.ref"] = "ak"
                store["secret_access_key.ref"] = "sk"
                tables.append(("secrets.ak", {"source.env": "SPILL_E2E_S3_ACCESS_KEY_ID"}))
                tables.append(("secrets.sk", {"source.env": "SPILL_E2E_S3_SECRET_ACCESS_KEY"}))
                danger["allow_env_secrets"] = True
            tables.insert(4, ("spill.store", store))
        tables.append(("danger_zone", danger))
        fx.config.write_text(toml_dump(tables))
        os.chmod(fx.config, 0o600)

    def fresh_device(self, seed, **config):
        """A new sparse device.raw, metadata and empty store for one case."""
        fx = self.fx
        self.stop_backend()
        self.restore_store()
        shutil.rmtree(self.store_dir(), ignore_errors=True)
        if fx.s3:
            self.s3_purge()
        for path in (fx.device_raw, fx.device_meta):
            if path.exists():
                path.unlink()
        with open(fx.device_raw, "wb") as f:
            f.truncate(DEVICE_BYTES)
        self.write_config(**config)
        r(fx.bin("init-metadata"), "-f", str(fx.config), "--stripe-sector-count-shift", "8")
        if self.model:
            self.model.close()
        self.model = Model(seed, fx.base_img)

    def start_backend(self, crash_at=None, s3_env=False):
        env = {}
        if crash_at:
            env["UBIBLK_SPILL_CRASH_AT"] = crash_at
        if s3_env:
            env["AWS_ACCESS_KEY_ID"] = self.fx.s3["access_key_id"]
            env["AWS_SECRET_ACCESS_KEY"] = self.fx.s3["secret_access_key"]
        log = self.fx.run_dir / f"{self.case_name}.backend.log"
        self.backend = Backend(self.fx, log, env).start()
        return self.backend

    def stop_backend(self):
        if self.backend is None:
            self._guests = []
            self._writers = []
            return
        clean = self.quiesce_guests()
        if clean:
            self.backend.stop()
        else:
            # A writer would not stop (a held write we could not release), so a
            # bio may still be in flight: SIGKILL, which ublk aborts the queue
            # for, rather than the del_gendisk path that would wedge.
            self.backend.kill()
        self.backend = None

    def guest(self):
        g = Guest(self.backend.dev, self.model)
        self._guests.append(g)
        return g

    def stripe_order(self, seed, count=SEQ_STRIPES):
        order = list(range(STRIPES))
        random.Random(seed).shuffle(order)
        return order[:count]

    # --- observations --------------------------------------------------------

    def dump_metadata(self):
        """The on-disk header bits by name, from dump-metadata. Backend stopped."""
        out = r(self.fx.bin("dump-metadata"), "-f", str(self.fx.config))
        lists = {}
        for line in out.splitlines():
            m = re.match(r"^([\w-]+) stripes: ?(.*)$", line)
            if m:
                lists[m.group(1)] = parse_ranges(m.group(2))
        for name in ("fetched", "written", "evicted", "in-s3", "pushed"):
            if name not in lists:
                raise RuntimeError(f"dump-metadata printed no '{name} stripes' line:\n{out}")
        return lists

    def allocated_stripes(self, stripes):
        """Stripes among ``stripes`` whose range holds allocated blocks (SEEK_DATA)."""
        fd = os.open(str(self.fx.device_raw), os.O_RDONLY)
        try:
            found = []
            for s in sorted(stripes):
                off = s * STRIPE
                try:
                    pos = os.lseek(fd, off, os.SEEK_DATA)
                except OSError as e:
                    if e.errno == errno.ENXIO:
                        continue
                    raise
                if pos < off + STRIPE:
                    found.append(s)
            return found
        finally:
            os.close(fd)

    def objects(self):
        """Stripe indices with an object in the store (the key object and
        temporaries excluded). None when the store cannot be listed."""
        if self.fx.s3:
            return self.s3_objects()
        d = self.store_dir()
        if not d.exists():
            return set()
        return {int(n) for n in os.listdir(d) if n.isdigit()}

    def wait_quiescent(self, timeout=120.0):
        """Wait until the evictor has nothing in flight: the counters stop
        moving, the gate is open and every successful PUT has its punch."""
        deadline = time.monotonic() + timeout
        last, since = None, time.monotonic()
        while True:
            sp = self.backend.spill()
            key = (sp["puts"], sp["punches"], sp["evicted"], sp["resident"], sp["gate"])
            if key != last:
                last, since = key, time.monotonic()
            settled = sp["gate"] == "open" and sp["puts"] - sp["put_failures"] == sp["punches"]
            if settled and time.monotonic() - since >= 2.0:
                return sp
            if time.monotonic() > deadline:
                raise RuntimeError(f"evictor did not settle in {timeout}s: {sp}")
            time.sleep(0.25)

    def wait_for(self, predicate, what, timeout=30.0, interval=0.1):
        deadline = time.monotonic() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() > deadline:
                raise RuntimeError(f"timed out after {timeout}s waiting for {what}")
            time.sleep(interval)

    # --- guest workloads -----------------------------------------------------

    def run_guest(self, fn, timeout, what):
        """Run guest I/O on a thread with a deadline. A hang is a failure the
        backend is killed out of, so the run can continue."""
        box = {}

        def target():
            try:
                box["result"] = fn()
            except BaseException as e:  # reported by the caller
                box["exc"] = e

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            self.backend.kill()
            t.join(5)
            raise GuestIoError(f"{what} hung for {timeout}s; backend killed")
        if "exc" in box:
            raise box["exc"]
        return box.get("result")

    def verify_all(self, guest):
        """Read the whole device and compare every byte (spec: every byte verifies)."""
        for s in range(STRIPES):
            if not guest.verify_stripe(s):
                raise GuestIoError(f"read of stripe {s} failed: {guest.last_error}")

    def must_read(self, guest, stripe):
        if not guest.verify_stripe(stripe):
            raise GuestIoError(f"read of stripe {stripe} failed: {guest.last_error}")

    def verify_stripes(self, guest, stripes):
        for s in stripes:
            if not guest.verify_stripe(s):
                raise GuestIoError(f"read of stripe {s} failed: {guest.last_error}")

    def random_mix(self, guest, seconds, seed, rate=None, stripes=None, until_error=False):
        """8 KiB random reads (verified) and writes, half and half, for
        ``seconds`` or until the first I/O error when ``until_error``.
        Returns the number of operations."""
        rnd = random.Random(seed)
        pool = None if stripes is None else list(stripes)
        end = time.monotonic() + seconds
        start = time.monotonic()
        ops = 0
        while time.monotonic() < end:
            if rate:
                wait = start + ops / rate - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
            if pool is None:
                block = rnd.randrange(0, TOTAL_BLOCKS - 1) & ~1
            else:
                stripe = rnd.choice(pool)
                block = stripe * BLOCKS_PER_STRIPE + (rnd.randrange(0, BLOCKS_PER_STRIPE - 1) & ~1)
            if rnd.random() < 0.5:
                ok = guest.verify_blocks(block, 2)
            else:
                ok = guest.write_blocks(block, 2)
            if not ok:
                if until_error:
                    return ops
                raise GuestIoError(f"I/O error at block {block}: {guest.last_error}")
            ops += 1
        return ops

    # --- shared assertions ---------------------------------------------------

    def check_counters(self, problems, sp, expect_stalls=None):
        self.check(problems, sp["evicted_dirty"] > 0, f"evicted_dirty is {sp['evicted_dirty']}")
        self.check(problems, sp["puts"] >= sp["in_s3"], f"puts {sp['puts']} < in_s3 {sp['in_s3']}")
        self.check(problems, sp["put_failures"] == 0, f"put_failures {sp['put_failures']}")
        self.check(problems, sp["punch_failures"] == 0, f"punch_failures {sp['punch_failures']}")
        self.check(problems, sp["degraded_reasons"] == 0, f"degraded_reasons {sp['degraded_reasons']}")
        self.check(problems, not sp["degraded"], "store is degraded")
        self.check(problems, sp["gate"] == "open", f"gate is {sp['gate']}")
        self.check(
            problems,
            sp["punches"] == sp["puts"] - sp["put_failures"],
            f"punches {sp['punches']} != successful puts {sp['puts'] - sp['put_failures']}",
        )
        if expect_stalls == 0:
            self.check(problems, sp["stalls"] == 0, f"stalls {sp['stalls']} in steady state")
        elif expect_stalls == "some":
            self.check(problems, sp["stalls"] > 0, "no stall was recorded")

    def check_metadata(self, problems, final_spill=None):
        """Backend stopped: the on-disk header bits agree with the store, the
        counters and the holes in device.raw."""
        md = self.dump_metadata()
        evicted, in_s3, fetched = md["evicted"], md["in-s3"], md["fetched"]
        self.check(problems, evicted <= in_s3, f"EVICTED without IN_S3: {sorted(evicted - in_s3)[:10]}")
        self.check(problems, not (evicted & fetched), f"EVICTED and FETCHED: {sorted(evicted & fetched)[:10]}")
        objects = self.objects()
        if objects is not None:
            self.check(
                problems, evicted <= objects, f"EVICTED stripes without an object: {sorted(evicted - objects)[:10]}"
            )
        allocated = self.allocated_stripes(evicted)
        self.check(problems, not allocated, f"EVICTED stripes with allocated blocks: {allocated[:10]}")
        holes = sorted(fetched - set(self.allocated_stripes(fetched)))
        self.check(problems, not holes, f"FETCHED stripes with no allocated blocks: {holes[:10]}")
        if final_spill is not None:
            self.check(
                problems,
                final_spill["evicted"] == len(evicted),
                f"status evicted {final_spill['evicted']} != {len(evicted)} EVICTED headers",
            )
            self.check(
                problems,
                final_spill["in_s3"] == len(evicted & in_s3),
                f"status in_s3 {final_spill['in_s3']} != {len(evicted & in_s3)} EVICTED|IN_S3 headers",
            )
            self.check(
                problems,
                final_spill["resident"] == len(fetched),
                f"status resident {final_spill['resident']} != {len(fetched)} FETCHED headers",
            )
        return md, objects

    # --- S3 helpers (the optional variant) -----------------------------------

    def s3_env(self):
        s3 = self.fx.s3
        return {
            **os.environ,
            "AWS_ACCESS_KEY_ID": s3["access_key_id"],
            "AWS_SECRET_ACCESS_KEY": s3["secret_access_key"],
            "AWS_DEFAULT_REGION": s3["region"],
            "AWS_EC2_METADATA_DISABLED": "true",
        }

    def s3_objects(self):
        s3 = self.fx.s3
        if not shutil.which("aws"):
            return None
        out = r(
            "aws", "s3api", "list-objects-v2", "--endpoint-url", s3["endpoint"],
            "--bucket", s3["bucket"], "--prefix", f"{s3['prefix']}/{DEVICE_ID}/",
            "--output", "json", env=self.s3_env(),
        )
        keys = [c["Key"] for c in json.loads(out or "{}").get("Contents", [])]
        return {int(k.rsplit("/", 1)[1]) for k in keys if k.rsplit("/", 1)[1].isdigit()}

    def s3_purge(self):
        s3 = self.fx.s3
        if not shutil.which("aws"):
            return
        try:
            r(
                "aws", "s3", "rm", "--recursive", "--quiet", "--endpoint-url", s3["endpoint"],
                f"s3://{s3['bucket']}/{s3['prefix']}/{DEVICE_ID}/", env=self.s3_env(),
            )
        except CommandFail as e:
            print(f"warning: S3 purge failed: {e.stderr}", file=sys.stderr)

    # --- cases ---------------------------------------------------------------

    def steady_state(self, name, s3_keys=None, s3_env=False):
        """Spec 7.3 steps 3 and 4: 200 MiB of pattern writes, a full read back,
        a 60 s random mix, and the ceiling, counters and metadata checks.

        The writer is paced below the eviction rate (a filesystem store PUT is
        a synchronous write plus fsync on the coordinator thread), which is
        what "steady state" means here; ``burst_stalls`` covers the unpaced
        case where the gate is expected to close.
        """
        self.case_name = name
        problems, notes = [], []
        self.fresh_device(seed=1, s3_keys=s3_keys)
        monitor = AllocMonitor(self.fx.device_raw)
        try:
            self.start_backend(s3_env=s3_env)
            monitor.start()
            guest = self.guest()
            order = self.stripe_order(seed=11)
            t0 = time.monotonic()
            self.run_guest(lambda: self.write_paced(guest, order, rate=48), 240, "200 MiB pattern write")
            t_write = time.monotonic() - t0
            sp = self.wait_quiescent()
            notes.append(
                f"wrote {SEQ_STRIPES} stripes in {t_write:.1f}s: puts {sp['puts']}, punches {sp['punches']}, "
                f"stalls {sp['stalls']}, resident_bytes {sp['resident_bytes']}"
            )
            objects_after_write = self.objects()
            if objects_after_write is not None:
                # Every PUT made an object, and a stripe can only have been
                # PUT twice if it was fetched back from the store in between
                # (the kernel's partition scan at device creation reads a few
                # stripes the guest later writes, so a handful are).
                n = len(objects_after_write)
                self.check(
                    problems,
                    n <= sp["puts"] <= n + sp["gets"],
                    f"puts {sp['puts']} outside [{n}, {n + sp['gets']}] for {n} objects and {sp['gets']} gets",
                )
            readback = list(order)
            random.Random(12).shuffle(readback)
            self.run_guest(lambda: self.verify_stripes(guest, readback), 240, "read back of written stripes")
            ops = self.run_guest(lambda: self.random_mix(guest, 60, seed=13, rate=150), 120, "random mix")
            notes.append(f"random mix: {ops} 8 KiB ops in 60s")
            self.run_guest(lambda: self.verify_all(guest), 300, "full verify")
            guest.close()
            sp = self.wait_quiescent()
            self.check_counters(problems, sp, expect_stalls=0)
            notes.append(
                f"final: evicted_dirty {sp['evicted_dirty']}, puts {sp['puts']}, gets {sp['gets']}, "
                f"punches {sp['punches']}, stalls {sp['stalls']}, evictions_aborted {sp['evictions_aborted']}"
            )
            peak = monitor.finish()
            notes.append(f"peak allocation {peak} bytes ({peak / STRIPE:.1f} stripes), ceiling {CEILING}")
            self.check(problems, peak <= CEILING, f"device.raw allocation peaked at {peak} > ceiling {CEILING}")
            self.check(problems, monitor.samples > 50, f"only {monitor.samples} allocation samples")
            self.stop_backend()
            _, objects = self.check_metadata(problems, sp)
            if objects is not None:
                self.check(problems, len(objects) <= sp["puts"], f"{len(objects)} objects > puts {sp['puts']}")
        except (GuestIoError, Mismatch, RuntimeError, CommandFail) as e:
            problems.append(f"{type(e).__name__}: {e}")
        finally:
            if monitor.is_alive():
                monitor.finish()
            self.stop_backend()
        self.report(name, problems, notes)

    def write_paced(self, guest, order, rate):
        w = self.track(Writer(guest, order, rate=rate))
        w.start()
        w.join()
        if w.errors:
            raise GuestIoError(f"write of stripe failed after {w.progress}: {guest.last_error}")
        return w

    def case_steady_state(self):
        self.steady_state("steady_state")

    def case_burst_stalls(self):
        """Spec 7.3 step 4, last clause: with one eviction at a time and a
        burst writer the gate closes (stalls > 0), nothing errors and every
        byte still verifies. The ceiling gets one stripe of slack per writer
        thread: each can have a stripe in flight past the gate."""
        name = "burst_stalls"
        self.case_name = name
        problems, notes = [], []
        threads = 8
        self.fresh_device(seed=2, max_concurrent_evictions=1)
        monitor = AllocMonitor(self.fx.device_raw)
        try:
            self.start_backend()
            monitor.start()
            order = self.stripe_order(seed=21, count=800)
            guests = [self.guest() for _ in range(threads)]
            writers = [self.track(Writer(g, order[i::threads])) for i, g in enumerate(guests)]
            for w in writers:
                w.start()
            deadline = time.monotonic() + 240
            for w in writers:
                w.join(max(0.0, deadline - time.monotonic()))
            hung = [w for w in writers if w.is_alive()]
            if hung:
                self.backend.kill()
                raise GuestIoError(f"{len(hung)} burst writers hung")
            errors = sum(w.errors for w in writers)
            self.check(problems, errors == 0, f"{errors} write errors under on_full = stall")
            sp = self.backend.spill()
            notes.append(f"after burst: stalls {sp['stalls']}, puts {sp['puts']}, evictions_aborted {sp['evictions_aborted']}")
            self.check(problems, sp["stalls"] > 0, "burst writer never closed the gate")
            for g in guests[1:]:
                g.close()
            guest = guests[0]
            self.run_guest(lambda: self.verify_all(guest), 300, "full verify")
            guest.close()
            sp = self.wait_quiescent()
            self.check_counters(problems, sp, expect_stalls="some")
            peak = monitor.finish()
            ceiling = CEILING + threads * STRIPE
            notes.append(f"peak allocation {peak} bytes ({peak / STRIPE:.1f} stripes), ceiling {ceiling}")
            self.check(problems, peak <= ceiling, f"device.raw allocation peaked at {peak} > {ceiling}")
            self.stop_backend()
            self.check_metadata(problems, sp)
        except (GuestIoError, Mismatch, RuntimeError, CommandFail) as e:
            problems.append(f"{type(e).__name__}: {e}")
        finally:
            if monitor.is_alive():
                monitor.finish()
            self.stop_backend()
        self.report(name, problems, notes)

    def crash(self, point, name=None, s3_keys=None, s3_env=False):
        """Spec 7.3 step 5: run until the fault-injection build aborts at
        ``point``, restart, check the startup pass left no EVICTED stripe with
        allocated blocks, and re-verify every acknowledged byte."""
        name = name or f"crash_{point}"
        self.case_name = name
        problems, notes = [], []
        self.fresh_device(seed=3 + CRASH_POINTS.index(point), s3_keys=s3_keys)
        try:
            self.start_backend(crash_at=point, s3_env=s3_env)
            guest = self.guest()
            order = self.stripe_order(seed=31)
            if point == "during_refetch":
                # Fill past the ceiling so stripes get evicted, then touch the
                # written set at random: the first access to an evicted stripe
                # re-fetches it and the hook fires when its data has landed.
                # The fill itself can trip it: the kernel reads a few stripes
                # when the device appears, they get evicted, and the writer
                # reaches them.
                head = order[:512]
                w = self.track(Writer(guest, head))
                w.start()
                w.join(120)
                if w.is_alive():
                    self.backend.kill()
                    raise GuestIoError("fill writer hung")
                if self.backend.alive():
                    self.run_guest(
                        lambda: self.random_mix(guest, 120, seed=32, stripes=head, until_error=True),
                        150, "random mix until crash",
                    )
            else:
                w = self.track(Writer(guest, order))
                w.start()
                w.join(180)
                if w.is_alive():
                    self.backend.kill()
                    raise GuestIoError("writer hung waiting for the crash")
            rc = self.backend.wait(30)
            self.check(problems, rc == -signal.SIGABRT, f"backend exit code {rc}, expected SIGABRT")
            self.check(
                problems,
                "UBIBLK_SPILL_CRASH_AT: aborting at" in self.backend.log_text(),
                "backend log has no crash-point abort line",
            )
            guest.close()
            acked = self.model.acknowledged_stripes()
            md = self.dump_metadata()
            evicted = md["evicted"]
            allocated_before = self.allocated_stripes(evicted)
            notes.append(
                f"crashed after {len(acked)} acknowledged stripes; {len(evicted)} EVICTED on disk, "
                f"{len(allocated_before)} of them with allocated blocks before restart"
            )
            # A stripe the guest wrote whose header says neither FETCHED nor
            # EVICTED had its SetFetched still queued in the flusher when the
            # process died; on restart it is NotFetched and is fetched from the
            # base image again, so the acknowledged writes are gone. That is
            # bdev_lazy's pre-existing in-memory-first landing (spill/base
            # bgworker.rs: mark_stripe_fetched before set_stripe_fetched), not
            # the spill path; expect the base image for those and report them.
            lost = sorted(acked - md["fetched"] - evicted)
            for stripe in lost:
                self.model.forget_stripe(stripe)
            notes.append(
                f"{len(lost)} acknowledged stripes had no durable FETCHED header at the crash "
                f"(pre-existing bdev_lazy gap; re-fetched from the base image on restart): {lost[:8]}"
            )
            if point in ("after_header_flush", "during_refetch"):
                self.check(
                    problems, allocated_before,
                    f"{point}: expected an EVICTED stripe with allocated blocks before the restart",
                )
            self.check(problems, evicted <= md["in-s3"], f"EVICTED without IN_S3 after crash: {sorted(evicted - md['in-s3'])[:10]}")
            objects = self.objects()
            if objects is not None:
                self.check(problems, evicted <= objects, f"EVICTED without an object after crash: {sorted(evicted - objects)[:10]}")

            self.start_backend(s3_env=s3_env)
            sp = self.backend.spill()
            runs = runs_of(evicted)
            self.check(
                problems,
                sp["startup_punches"] == runs,
                f"startup_punches {sp['startup_punches']} != {runs} runs of EVICTED stripes",
            )
            restart_log = self.backend.log_text().rsplit("aborting at", 1)[-1]
            pass_line = f"Startup punch pass: {len(evicted)} evicted stripe(s) in {runs} run(s)"
            self.check(problems, pass_line in restart_log, f"restarted backend did not log '{pass_line}'")
            # The kernel reads the start and end of a new disk for partition
            # tables as soon as it appears, which fetches those stripes back
            # from the store before this process gets a look; anything found
            # allocated must be accounted for by a GET.
            allocated_after = self.allocated_stripes(evicted)
            sp = self.backend.spill()
            self.check(
                problems,
                len(allocated_after) <= sp["gets"],
                f"EVICTED stripes allocated after the startup pass with no re-fetch to explain them: "
                f"{allocated_after[:10]} (gets {sp['gets']})",
            )
            self.check(
                problems,
                len(evicted) - sp["gets"] <= sp["evicted"] <= len(evicted),
                f"status evicted {sp['evicted']} does not match {len(evicted)} on disk less {sp['gets']} re-fetched",
            )
            notes.append(
                f"after restart: startup_punches {sp['startup_punches']}, {len(allocated_after)} of "
                f"{len(evicted)} EVICTED stripes already re-fetched by the kernel's partition scan (gets {sp['gets']})"
            )
            guest = self.guest()
            self.run_guest(lambda: self.verify_all(guest), 300, "full verify after restart")
            self.run_guest(lambda: self.random_mix(guest, 10, seed=33, rate=150), 60, "post-restart mix")
            self.run_guest(lambda: self.verify_all(guest), 300, "second full verify")
            guest.close()
            sp = self.wait_quiescent()
            self.check(problems, sp["degraded_reasons"] == 0, f"degraded_reasons {sp['degraded_reasons']} after restart")
            self.check(problems, sp["put_failures"] == 0, f"put_failures {sp['put_failures']} after restart")
            self.check(problems, sp["get_failures"] == 0, f"get_failures {sp['get_failures']} after restart")
            notes.append(f"after restart: gets {sp['gets']}, puts {sp['puts']}, punches {sp['punches']}")
            self.stop_backend()
            self.check_metadata(problems, sp)
        except (GuestIoError, Mismatch, RuntimeError, CommandFail) as e:
            problems.append(f"{type(e).__name__}: {e}")
        finally:
            self.stop_backend()
        self.report(name, problems, notes)

    def case_crash_after_put(self):
        self.crash("after_put")

    def case_crash_after_header_flush(self):
        self.crash("after_header_flush")

    def case_crash_after_punch(self):
        self.crash("after_punch")

    def case_crash_during_refetch(self):
        self.crash("during_refetch")

    def break_store(self):
        """Make the store refuse new objects for root too: an immutable
        directory rejects the create and the rename of name.tmp with EPERM,
        while existing objects stay readable."""
        r("chattr", "+i", str(self.store_dir()))

    def degraded_stall(self):
        """Spec 7.3 step 6, on_full = stall: the writer blocks, a resident read
        completes, nothing errors, I/O resumes within 5 s of the store coming
        back."""
        name = "degraded_store_stall"
        self.case_name = name
        problems, notes = [], []
        self.fresh_device(seed=7, on_full="stall")
        try:
            self.start_backend()
            guest = self.guest()
            w = self.track(Writer(guest, self.stripe_order(seed=71)))
            w.start()
            self.wait_for(lambda: self.backend.spill()["puts"] >= 16, "the first evictions", 60)
            self.break_store()
            t_break = time.monotonic()
            self.wait_for(
                lambda: (lambda sp: sp["put_failures"] >= 1 and sp["degraded"])(self.backend.spill()),
                "a failed PUT and the degraded flag", 30,
            )

            def stalled():
                sp = self.backend.spill()
                if sp["gate"] != "hold" or not w.acks:
                    return False
                return time.monotonic() - w.acks[-1][0] >= 1.0

            self.wait_for(stalled, "the gate to hold and the writer to stall for 1 s", 60)
            t_stall = time.monotonic()
            self.check(problems, w.errors == 0, f"{w.errors} write errors under on_full = stall")
            self.check(problems, w.is_alive(), "writer thread ended while it should be blocked")
            # Anything acknowledged after the store broke cannot have been
            # evicted since (an eviction needs a successful PUT), so it is
            # resident and must be readable while the gate holds writes.
            # Reads of resident stripes are served while the gate holds writes,
            # so long as they are not queued behind a held write. With one
            # writer thread and four queues the held write sits on one queue;
            # probe several resident stripes at once from their own threads and
            # require that some complete quickly and none returns wrong bytes.
            resident = [s for (t, s) in w.acks if t > t_break + 0.1][-8:] or [w.acks[-1][1]]
            readers = [Guest(self.backend.dev, self.model) for _ in resident]
            done = {}

            def probe_read(i, stripe, reader):
                try:
                    ok = reader.verify_stripe(stripe)
                    done[i] = "ok" if ok else f"io error {reader.last_error}"
                except Mismatch as e:
                    done[i] = f"MISMATCH {e}"

            probes = [
                threading.Thread(target=probe_read, args=(i, s, readers[i]), daemon=True)
                for i, s in enumerate(resident)
            ]
            for pth in probes:
                pth.start()
            probe_deadline = time.monotonic() + 3.0
            for pth in probes:
                pth.join(max(0.0, probe_deadline - time.monotonic()))
            served = sum(1 for v in done.values() if v == "ok")
            wrong = [v for v in done.values() if v.startswith("MISMATCH")]
            failed = [v for v in done.values() if v.startswith("io error")]
            self.check(problems, served >= 1, f"no resident read completed under hold ({len(resident)} probed)")
            self.check(problems, not wrong, f"resident read wrong bytes under hold: {wrong[:3]}")
            self.check(problems, not failed, f"resident read errored under hold: {failed[:3]}")
            notes.append(f"resident reads under hold: {served}/{len(resident)} completed within 3 s")
            time.sleep(0.5)
            progress_at_restore = w.progress
            r("chattr", "-i", str(self.store_dir()))
            t_restore = time.monotonic()
            self.wait_for(lambda: w.progress > progress_at_restore, "the writer to resume", 30)
            resumed_after = time.monotonic() - t_restore
            notes.append(f"store broken; writer resumed {resumed_after:.2f}s after restore")
            self.check(problems, resumed_after <= 5.0, f"writer resumed {resumed_after:.1f}s after the store was restored")
            # The probe reads queued behind a held write finish once the gate
            # opens; join them so no bio is in flight at teardown.
            for pth in probes:
                pth.join(30)
            for reader in readers:
                reader.close()
            time.sleep(3)
            w.halt.set()
            w.join(60)
            self.check(problems, w.errors == 0, f"{w.errors} write errors in total")
            self.run_guest(lambda: self.verify_all(guest), 300, "full verify")
            guest.close()
            sp = self.wait_quiescent()
            self.check(problems, sp["stalls"] >= 1, "no stall counted")
            self.check(problems, sp["put_failures"] >= 1, "no PUT failure counted")
            self.check(problems, sp["punches"] == sp["puts"] - sp["put_failures"], f"punches {sp['punches']} != successful puts")
            self.check(problems, not sp["degraded"], "still degraded after restore")
            self.check(problems, sp["degraded_reasons"] == 0, f"degraded_reasons {sp['degraded_reasons']}")
            notes.append(f"final: puts {sp['puts']}, put_failures {sp['put_failures']}, punches {sp['punches']}, stalls {sp['stalls']}")
            self.stop_backend()
            self.check_metadata(problems, sp)
        except (GuestIoError, Mismatch, RuntimeError, CommandFail) as e:
            problems.append(f"{type(e).__name__}: {e}")
        finally:
            self.restore_store()
            self.stop_backend()
        self.report(name, problems, notes)

    def case_degraded_store_stall(self):
        self.degraded_stall()

    def degraded_fail(self):
        """Spec 7.3 step 6, on_full = fail: the writer gets I/O errors while the
        store is down, no acknowledged byte is lost across a restart, and
        nothing was punched without an object."""
        name = "degraded_store_fail"
        self.case_name = name
        problems, notes = [], []
        self.fresh_device(seed=8, on_full="fail")
        try:
            self.start_backend()
            guest = self.guest()
            w = self.track(Writer(guest, self.stripe_order(seed=81), stop_on_error=False))
            w.start()
            self.wait_for(lambda: self.backend.spill()["puts"] >= 16, "the first evictions", 60)
            self.break_store()
            t_break = time.monotonic()
            self.wait_for(
                lambda: self.backend.spill()["gate"] == "fail" and w.errors > 0,
                "the gate to fail and the writer to see an error", 60,
            )
            errors_while_down = w.errors
            # A resident stripe still reads under FAIL (only writes and
            # non-resident fetches are refused). One acknowledged after the
            # store broke cannot have been evicted since.
            recent = [s for (t, s) in w.acks if t > t_break + 0.1] or [w.acks[-1][1]]
            resident = recent[-1]
            reader = Guest(self.backend.dev, self.model)
            self.run_guest(lambda: self.must_read(reader, resident), 5, f"read of resident stripe {resident} under fail")
            reader.close()
            time.sleep(0.5)
            r("chattr", "-i", str(self.store_dir()))
            self.wait_for(lambda: self.backend.spill()["gate"] == "open", "the gate to reopen", 30)
            errors_at_reopen = w.errors
            w.join(120)
            if w.is_alive():
                w.halt.set()
                self.backend.kill()
                raise GuestIoError("writer hung after the store was restored")
            notes.append(
                f"{w.progress} stripes acknowledged, {w.errors} write errors "
                f"({errors_while_down} before restore, {w.errors - errors_at_reopen} after the gate reopened)"
            )
            self.check(problems, errors_while_down > 0, "no write error under on_full = fail")
            guest.close()
            sp = self.wait_quiescent()
            self.check(problems, sp["stalls"] >= 1, "no gate transition counted")
            self.check(problems, sp["put_failures"] >= 1, "no PUT failure counted")
            self.check(problems, sp["punches"] == sp["puts"] - sp["put_failures"], f"punches {sp['punches']} != successful puts")
            self.check(problems, sp["degraded_reasons"] == 0, f"degraded_reasons {sp['degraded_reasons']}")
            notes.append(f"final: puts {sp['puts']}, put_failures {sp['put_failures']}, punches {sp['punches']}, stalls {sp['stalls']}")
            self.stop_backend()
            self.check_metadata(problems, sp)
            # Restart and verify every acknowledged byte.
            self.start_backend()
            guest = self.guest()
            self.run_guest(lambda: self.verify_all(guest), 300, "full verify after restart")
            guest.close()
            sp = self.wait_quiescent()
            self.stop_backend()
            self.check_metadata(problems, sp)
        except (GuestIoError, Mismatch, RuntimeError, CommandFail) as e:
            problems.append(f"{type(e).__name__}: {e}")
        finally:
            self.restore_store()
            self.stop_backend()
        self.report(name, problems, notes)

    def case_degraded_store_fail(self):
        self.degraded_fail()

    # --- S3 variant (spec 7.3 step 8), skipped without SPILL_E2E_S3_* ---------

    def s3_case(self, name, fn):
        if not self.fx.s3:
            self.skip(name, "SPILL_E2E_S3_ENDPOINT not set")
            return
        fn()

    def case_s3_steady_state_with_keys(self):
        self.s3_case("s3_steady_state_with_keys", lambda: self.steady_state("s3_steady_state_with_keys", s3_keys="config"))

    def case_s3_crash_after_header_flush_with_keys(self):
        self.s3_case(
            "s3_crash_after_header_flush_with_keys",
            lambda: self.crash("after_header_flush", name="s3_crash_after_header_flush_with_keys", s3_keys="config"),
        )

    def case_s3_steady_state_env_keys(self):
        self.s3_case(
            "s3_steady_state_env_keys",
            lambda: self.steady_state("s3_steady_state_env_keys", s3_keys="env", s3_env=True),
        )

    def case_s3_crash_after_header_flush_env_keys(self):
        self.s3_case(
            "s3_crash_after_header_flush_env_keys",
            lambda: self.crash("after_header_flush", name="s3_crash_after_header_flush_env_keys", s3_keys="env", s3_env=True),
        )

    CASES = [
        case_steady_state,
        case_burst_stalls,
        case_crash_after_put,
        case_crash_after_header_flush,
        case_crash_after_punch,
        case_crash_during_refetch,
        case_degraded_store_stall,
        case_degraded_store_fail,
        case_s3_steady_state_with_keys,
        case_s3_crash_after_header_flush_with_keys,
        case_s3_steady_state_env_keys,
        case_s3_crash_after_header_flush_env_keys,
    ]
