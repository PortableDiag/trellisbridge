"""`hermes trellis …`: connect this Hermes to Trellis.

`hermes plugins install PortableDiag/trellisbridge --enable` puts this plugin in
place; `hermes trellis setup` does the rest. It gets the bridge (the small Rust
daemon that holds the Trellis key, so Hermes never does), asks for the key, starts
the bridge as a user service and points Hermes at it.

The bridge binary, in order of preference:
  1. `--bin PATH`, a binary you already have;
  2. the release binary for this CPU, checked against the release's SHA256SUMS;
  3. built from the source shipped beside this file, when `cargo` is installed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

RELEASES = "https://github.com/PortableDiag/trellisbridge/releases/download"
HERE = Path(__file__).resolve().parent


def register_cli(parser: argparse.ArgumentParser) -> None:
    subs = parser.add_subparsers(dest="trellis_command", required=False)
    p = subs.add_parser("setup", help="Install or upgrade the bridge and connect this Hermes to Trellis")
    p.add_argument("--key-file", help="the agent's Trellis key in a file (otherwise you are asked for it)")
    p.add_argument("--agent", help="the agent's name in Trellis (default: the name the key is bound to)")
    p.add_argument("--url", help="Trellis server (default https://trellis-cards.com)")
    p.add_argument("--name", help="bridge instance name (default: from the Hermes dir)")
    p.add_argument("--port", help="bridge port (default: the next free from 8791)")
    p.add_argument("--bin", help="use this trellisbridge binary instead of downloading one")
    p.add_argument("--avatar", help="the agent's picture in Trellis channels")
    p.add_argument("--description", help="one line on the agent's card")
    p.add_argument("--no-restart", action="store_true", help="do not restart the Hermes gateway")
    p.add_argument("--pin", default="", help="router threat: route the main model only to these OpenRouter hosts, in order (host1,host2)")
    p.add_argument("--bait-file", default="", help="router threat: the account's bait key (Trellis Agents -> Bait key) in a file")
    p.add_argument("--no-harden", action="store_true", help="skip the router-threat settings (hermes trellis harden)")
    h = subs.add_parser("harden", help="Router-threat settings: no-training rule, pinned hosts, bait key (idempotent)")
    h.add_argument("--pin", default="", help="route the main model only to these OpenRouter hosts, in order (host1,host2)")
    h.add_argument("--bait-file", default="", help="the account's bait key (Trellis Agents -> Bait key) in a file")
    h.add_argument("--dry-run", action="store_true", help="say what would change, save nothing")
    subs.add_parser("status", help="Is the bridge up, and which Trellis is it on?")
    parser.set_defaults(func=dispatch)


def dispatch(args: argparse.Namespace) -> int:
    sub = getattr(args, "trellis_command", None) or "status"
    return {"setup": _setup, "status": _status, "harden": _harden}.get(sub, _unknown)(args)


def _unknown(args) -> int:
    print("usage: hermes trellis {setup,harden,status}", file=sys.stderr)
    return 2


def _harden(args: argparse.Namespace) -> int:
    from .harden import harden
    return harden(pin=args.pin, bait_file=args.bait_file, dry_run=args.dry_run)


def _version() -> str:
    for f in (HERE / "VERSION", HERE.parent.parent / "Cargo.toml"):
        if f.name == "VERSION" and f.exists():
            return f.read_text().strip()
        if f.exists():
            for line in f.read_text().splitlines():
                if line.startswith("version = "):
                    return line.split('"')[1]
    raise SystemExit("trellis: cannot tell which bridge version this plugin goes with (no VERSION file)")


def _first(*paths: Path) -> Path | None:
    return next((p for p in paths if p.exists()), None)


def _download(version: str) -> Path | None:
    """The release binary for this CPU, verified against SHA256SUMS. None when there
    is none (another OS or CPU, no network, no such release)."""
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(platform.machine().lower())
    if platform.system() != "Linux" or not arch:
        return None
    name = f"trellisbridge-{arch}-linux"
    base = f"{RELEASES}/v{version}"
    try:
        sums = urllib.request.urlopen(f"{base}/SHA256SUMS", timeout=30).read().decode()
        want = next(line.split()[0] for line in sums.splitlines() if line.strip().endswith(name))
        data = urllib.request.urlopen(f"{base}/{name}", timeout=120).read()
    except Exception as e:  # noqa: BLE001 — any failure means "build instead"
        print(f"  no release binary ({e}); trying to build from source")
        return None
    got = hashlib.sha256(data).hexdigest()
    if got != want:
        raise SystemExit(f"trellis: {name} does not match SHA256SUMS ({got} != {want}); not installing it")
    out = Path(tempfile.mkdtemp()) / "trellisbridge"
    out.write_bytes(data)
    out.chmod(0o755)
    print(f"  downloaded {name} v{version} (sha256 verified)")
    return out


def _build() -> Path | None:
    manifest = _first(HERE / "bridge" / "Cargo.toml", HERE.parent.parent / "Cargo.toml")
    if not manifest or not shutil.which("cargo"):
        return None
    target = Path(tempfile.mkdtemp()) / "target"
    print("  building the bridge from source (a minute or two)…")
    subprocess.run(["cargo", "build", "--release", "--quiet", "--manifest-path", str(manifest),
                    "--target-dir", str(target)], check=True)
    return target / "release" / "trellisbridge"


def _setup(args: argparse.Namespace) -> int:
    installer = _first(HERE / "install.sh", HERE.parent.parent / "bundle" / "install.sh")
    if not installer:
        print("trellis: install.sh is missing from this plugin", file=sys.stderr)
        return 1
    version = _version()
    print(f"TrellisBridge {version}: setting up the bridge")
    binary = Path(args.bin) if args.bin else (_download(version) or _build())
    if not binary or not binary.exists():
        print("trellis: no bridge binary for this machine: no release build for it, and no "
              "`cargo` to build one (https://rustup.rs). Or pass --bin PATH.", file=sys.stderr)
        return 1
    cmd = ["bash", str(installer), "--bin", str(binary), "--no-plugin",
           "--hermes-home", os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes")]
    for flag in ("key_file", "agent", "url", "name", "port", "avatar", "description"):
        value = getattr(args, flag, None)
        if value:
            cmd += [f"--{flag.replace('_', '-')}", value]
    if args.no_restart:
        cmd.append("--no-restart")
    # The router-threat settings go in before the installer restarts the
    # gateway, so the restart applies them; the bait goes to the bridge after
    # the installer has put the bridge in place.
    if not args.no_harden:
        from .harden import harden
        try:
            harden(pin=args.pin)
        except Exception as e:  # noqa: BLE001 — never block the install on it
            print(f"  harden    skipped: {e}. Run later: hermes trellis harden")
    rc = subprocess.run(cmd).returncode
    if rc == 0 and args.bait_file and not args.no_harden:
        from .harden import _bait
        _bait(args.bait_file)
    return rc


def _status(args) -> int:
    url = (os.environ.get("TRELLISBRIDGE_URL") or "").rstrip("/")
    if not url:
        env = Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes") / ".env"
        for line in env.read_text().splitlines() if env.exists() else []:
            if line.startswith("TRELLISBRIDGE_URL="):
                url = line.split("=", 1)[1].strip().rstrip("/")
    if not url:
        print("trellis: not set up yet. Run: hermes trellis setup")
        return 1
    try:
        body = urllib.request.urlopen(f"{url}/api/health", timeout=10).read().decode()
    except Exception as e:  # noqa: BLE001
        print(f"trellis: the bridge at {url} is not answering ({e}). "
              "Check: systemctl --user status 'trellisbridge@*'")
        return 1
    print(f"bridge at {url}: {body}")
    return 0
