#!/usr/bin/env python3
"""Run the spill end-to-end tests (cases.py) on a Linux box, as root.

Sets up what the cases need and tears it down afterwards, whether the run
passes, fails or is cancelled:

- filesystem A: a small ext4 image (default 64 MiB) on a loop device, mounted
  under the work directory; device.raw and the metadata live there and nothing
  else, so the ceiling is enforced against a disk that really fills up;
- filesystem B: a directory on the repo filesystem (under the cargo target
  directory) with the 256 MiB random base image, the filesystem spill store,
  the RPC socket, the device symlink and the backend logs;
- the ublk_drv module, loaded if /dev/ublk-control is missing.

Needs root (ublk devices and loop mounts), python3, mkfs.ext4, chattr, and the
binaries ublk-backend, init-metadata and dump-metadata built with
``--features fault-injection`` (the crash cases abort the backend through
UBIBLK_SPILL_CRASH_AT and fail with "expected SIGABRT" against a build without
it):

    cargo build --features fault-injection \\
        --bin ublk-backend --bin init-metadata --bin dump-metadata
    sudo -E python3 tests/spill/run_all.py [--only steady --only crash]

Binaries are taken from ``$CARGO_TARGET_DIR/debug`` (or ``target/debug``);
``--target-dir`` overrides. The optional S3 variant runs when
SPILL_E2E_S3_ENDPOINT, SPILL_E2E_S3_BUCKET, SPILL_E2E_S3_ACCESS_KEY_ID and
SPILL_E2E_S3_SECRET_ACCESS_KEY are set (SPILL_E2E_S3_REGION defaults to
us-east-1); it needs the aws CLI to list and purge the objects, against a
MinIO started for the run or any other S3-compatible endpoint.
"""

import argparse
import os
import pathlib
import secrets
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "common"))

from util import CommandFail, r  # noqa: E402
from harness import install_exit_handler  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]


def target_dir(arg):
    if arg:
        return pathlib.Path(arg)
    env = os.environ.get("CARGO_TARGET_DIR")
    return pathlib.Path(env) if env else ROOT / "target"


def check_binaries(bin_dir):
    missing = [name for name in ("ublk-backend", "init-metadata", "dump-metadata") if not (bin_dir / name).exists()]
    if missing:
        sys.exit(
            f"missing binaries in {bin_dir}: {', '.join(missing)}\n"
            "build them first: cargo build --features fault-injection "
            "--bin ublk-backend --bin init-metadata --bin dump-metadata"
        )
    for tool in ("mkfs.ext4", "mount", "umount", "chattr"):
        if not shutil.which(tool):
            sys.exit(f"{tool} not found on PATH")


def s3_from_env():
    endpoint = os.environ.get("SPILL_E2E_S3_ENDPOINT")
    if not endpoint:
        return None
    s3 = {
        "endpoint": endpoint,
        "bucket": os.environ.get("SPILL_E2E_S3_BUCKET"),
        "access_key_id": os.environ.get("SPILL_E2E_S3_ACCESS_KEY_ID"),
        "secret_access_key": os.environ.get("SPILL_E2E_S3_SECRET_ACCESS_KEY"),
        "region": os.environ.get("SPILL_E2E_S3_REGION", "us-east-1"),
        "prefix": os.environ.get("SPILL_E2E_S3_PREFIX", f"spill-e2e/{secrets.token_hex(4)}"),
    }
    missing = [k for k, v in s3.items() if not v]
    if missing:
        sys.exit(f"SPILL_E2E_S3_ENDPOINT is set but these are missing: {', '.join(missing)}")
    return s3


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target-dir", help="cargo target dir holding debug/ (default: $CARGO_TARGET_DIR or target/)")
    parser.add_argument("--work-dir", help="scratch directory (default: <target-dir>/spill-e2e)")
    parser.add_argument("--image-size", default="64M", help="size of the ext4 image holding device.raw (default 64M)")
    parser.add_argument("--only", action="append", help="run only cases whose name contains this (repeatable)")
    parser.add_argument("--keep", action="store_true", help="keep the work directory (logs, store) after the run")
    args = parser.parse_args()

    if os.geteuid() != 0:
        sys.exit("run as root: ublk devices and loop mounts need it (sudo -E preserves the env)")
    tgt = target_dir(args.target_dir)
    bin_dir = tgt / "debug"
    check_binaries(bin_dir)
    work = pathlib.Path(args.work_dir) if args.work_dir else tgt / "spill-e2e"
    s3 = s3_from_env()

    if not os.path.exists("/dev/ublk-control"):
        r("modprobe", "ublk_drv")

    if work.exists():
        shutil.rmtree(work)
    a_dir = work / "a"
    b_dir = work / "b"
    a_img = work / "a.img"
    for d in (a_dir, b_dir):
        d.mkdir(parents=True)
    state = {"mounted": False}

    def cleanup():
        # Any backend still running holds device.raw open on A; a cancelled
        # run gets here with one alive.
        subprocess.run(["pkill", "-INT", "-f", f"ublk-backend -f {b_dir}/"], capture_output=True)
        for _ in range(50):
            if subprocess.run(["pgrep", "-f", f"ublk-backend -f {b_dir}/"], capture_output=True).returncode != 0:
                break
            time.sleep(0.1)
        subprocess.run(["pkill", "-KILL", "-f", f"ublk-backend -f {b_dir}/"], capture_output=True)
        store = b_dir / "spill" / "e2e"
        if store.exists():
            subprocess.run(["chattr", "-i", str(store)], capture_output=True)
        if state["mounted"]:
            for _ in range(20):
                if subprocess.run(["umount", str(a_dir)], capture_output=True).returncode == 0:
                    state["mounted"] = False
                    break
                time.sleep(0.25)
            if state["mounted"]:
                subprocess.run(["umount", "-l", str(a_dir)], capture_output=True)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        # A backend that aborted leaves its ublk device behind until the module
        # is unloaded. Reclaim them when nothing else is using ublk.
        if subprocess.run(["pgrep", "-f", "ublk-backend"], capture_output=True).returncode != 0:
            if subprocess.run(["modprobe", "-r", "ublk_drv"], capture_output=True).returncode == 0:
                subprocess.run(["modprobe", "ublk_drv"], capture_output=True)

    install_exit_handler(cleanup)

    try:
        r("truncate", "-s", args.image_size, str(a_img))
        r("mkfs.ext4", "-q", "-F", str(a_img))
        r("mount", "-o", "loop", str(a_img), str(a_dir))
        state["mounted"] = True
    except CommandFail as e:
        sys.exit(f"could not set up the ext4 image: {e}")

    avail = shutil.disk_usage(a_dir).free
    print(f"# kernel {os.uname().release}, binaries {bin_dir}")
    print(f"# A: {a_dir} ({args.image_size} ext4 on loop, {avail >> 20} MiB free); B: {b_dir}")
    print(f"# S3 variant: {'enabled at ' + s3['endpoint'] if s3 else 'skipped (SPILL_E2E_S3_ENDPOINT unset)'}")

    from cases import Cases, Fixture

    fx = Fixture(bin_dir, a_dir, b_dir, s3)
    sys.exit(Cases(fx).run(only=args.only))


if __name__ == "__main__":
    main()
