"""Shared Trellis skills: the operator-approved ones become this agent's own.

trellis-web keeps skills in a document's `Skills` basket (`/api/skills`, web
0.71.0). A skill is live when the operator approved it in a browser (the
approval is bound to its sha256, so a later edit takes it out of service) or,
with the document's auto-approve setting on (0.71.1), when it is a valid
`trellis` skill. The agent uses every live skill (operator, 2026-09-28): both
approved and auto-approved. The text fetched is hashed again here, so what is
installed is exactly what the server lists.

Auto-approve is on by default since web 0.71.3 and lets ANY writer make a skill
live, so an auto-approved skill counts only when its last writer could have
given the agent an order (D11/D12): the operator in a browser or the linked
Telegram chat (`kind: person`, `via: session|telegram`, `from_key_owner: true`,
web 0.72.0), or the agent itself. Since web 0.72.1 a full-access key's skill is
live as approved (`approved_by: "<X-Agent> (full-access key)"`, setting
`trust_full_keys`, on by default); ALICE holds one, so that approval is treated
like auto-approve: it counts only from the operator or the agent. A peer agent's skill stays text, as its messages do.

Trusted skills are written to $HERMES_HOME/skills/trellis-shared/<name>/SKILL.md,
where Hermes lists them like any other skill; one that stops being trusted is
removed. A name this agent already has elsewhere is never shadowed. The sync runs
in the gateway every few minutes and on demand (`trellis_skills_sync`).

To share one: `trellis_api` POST /api/skills {skill: "<SKILL.md>"} — tagged
`trellis`, SKILL.md only. It is live at once while auto-approve is on,
otherwise `proposed` until the operator approves it.
"""

import hashlib
import json
import re
import logging
import os
import shutil
import threading
import time
import urllib.parse

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None

logger = logging.getLogger(__name__)

INTERVAL = 300
CATEGORY = "trellis-shared"
_started = False
_lock = threading.Lock()


def _skills_root() -> str:
    return os.path.join(os.environ.get("HERMES_HOME", "/opt/data"), "skills")


def _front_matter(src: str) -> dict:
    """The skill's front matter: PyYAML when Hermes has it (the Docker image
    does), else the top-level `key: value` subset a SKILL.md uses — plain,
    quoted and block (`|`, `>`) scalars, parsed the way YAML does, since the
    sha256 depends on the exact text."""
    try:
        import yaml
    except ImportError:
        yaml = None
    if yaml is not None:
        return yaml.safe_load(src) or {}
    meta, lines, i = {}, src.split("\n"), 0
    while i < len(lines):
        m = re.match(r"^([A-Za-z_][\w-]*):(?:\s+(.*))?$", lines[i])
        i += 1
        if not m:
            continue
        key, val = m.group(1), (m.group(2) or "").strip()
        more = []
        while i < len(lines) and (lines[i].startswith((" ", "\t")) or not lines[i].strip()):
            more.append(lines[i])
            i += 1
        while more and not more[-1].strip():
            more.pop()
        if val[:1] in ("|", ">"):
            ind = min((len(x) - len(x.lstrip()) for x in more if x.strip()), default=0)
            body = [x[ind:] for x in more]
            out = "\n".join(body) if val[0] == "|" else _fold(body)
            meta[key] = out if val.endswith("-") else (out + "\n" if out else "")
        elif val.startswith('"'):
            meta[key] = json.loads(" ".join([val] + [x.strip() for x in more]))
        elif val.startswith("'"):
            meta[key] = " ".join([val] + [x.strip() for x in more])[1:-1].replace("''", "'")
        else:
            meta[key] = _fold([val] + [x.strip() for x in more]).strip()
    return meta


def _fold(lines: list) -> str:
    """YAML folding: single breaks become spaces, a blank line a newline."""
    out, para = [], []
    for x in lines:
        if x.strip():
            para.append(x.strip() if not x.startswith((" ", "\t")) else x)
        else:
            out.append(" ".join(para))
            para = []
    out.append(" ".join(para))
    return "\n".join(out)


def canonical_sha256(skill_md: str) -> str:
    """LANAgent's hash, as trellis-web computes it: SHA-256 of the compact JSON
    `[name, description with line breaks folded to spaces, body trimmed]`."""
    text = skill_md.lstrip("﻿")
    if not text.startswith("---"):
        raise ValueError("no front matter")
    end = text.find("\n---", 3)
    if end < 0:
        raise ValueError("front matter not closed")
    meta = _front_matter(text[3:end])
    body = text[end + 4:].split("\n", 1)[1] if "\n" in text[end + 4:] else ""
    desc = " ".join(str(meta.get("description", "")).split("\n"))
    payload = json.dumps([str(meta.get("name", "")), desc, body.strip()],
                         separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def writer_trusted(w: dict, me: str) -> bool:
    """The last writer of an auto-approved skill: the operator, or the agent."""
    if not isinstance(w, dict) or w.get("from_key_owner") is not True:
        return False
    if w.get("kind") == "person":
        return w.get("via") in ("session", "telegram")
    return bool(me) and w.get("name") == me and w.get("agent_verified") is True


def key_approved(entry: dict) -> bool:
    """Approved by a full-access key writing it (web 0.72.1), not by a person."""
    return str(entry.get("approved_by") or "").endswith("(full-access key)")


def trusted(entry: dict, me: str = "") -> bool:
    """Live, as it is now: approved by the operator, or auto-approved and last
    written by the operator or the agent."""
    if entry.get("status") != "live" or not entry.get("sha256"):
        return False
    if entry.get("auto_approved") is True or key_approved(entry):
        return writer_trusted(entry.get("last_writer"), me)
    return bool(entry.get("approved_by")) and entry.get("approved_sha256") == entry.get("sha256")


def expected_sha256(entry: dict) -> str:
    return entry.get("sha256") if entry.get("auto_approved") is True else entry.get("approved_sha256")


def _get(client, base: str, path: str) -> dict:
    r = client.get(f"{base}/api/trellis-bytes", params={"path": path}, timeout=30.0)
    r.raise_for_status()
    return r.json()


def _local_names(root: str) -> set:
    """Skill names this agent has outside the shared folder."""
    names = set()
    for dirpath, dirnames, filenames in os.walk(root):
        if os.path.relpath(dirpath, root).split(os.sep)[0] == CATEGORY:
            dirnames[:] = []
            continue
        if "SKILL.md" in filenames:
            names.add(os.path.basename(dirpath))
    return names


def sync_once(base: str, key: str) -> dict:
    root = _skills_root()
    shared = os.path.join(root, CATEGORY)
    installed, removed, skipped = [], [], []
    with _lock, httpx.Client(headers={"Authorization": f"Bearer {key}"}) as client:
        listing = _get(client, base, "/api/skills")
        me = client.get(f"{base}/health", timeout=10.0).json().get("agent", "")
        own = _local_names(root)
        keep = set()
        for e in listing.get("skills", []):
            name = e.get("name") or ""
            if not trusted(e, me):
                if e.get("status") == "live" and (e.get("auto_approved") is True or key_approved(e)):
                    w = e.get("last_writer") or {}
                    skipped.append(f"{name}: approved without the operator, last written by {w.get('name') or w.get('kind')} (a peer) — kept as text")
                continue
            if name in own:
                skipped.append(f"{name}: this agent already has a skill by that name")
                continue
            one = _get(client, base, f"/api/skills/{urllib.parse.quote(name)}")
            md = one.get("skill_md") or ""
            try:
                h = canonical_sha256(md)
            except Exception as err:
                skipped.append(f"{name}: {err}")
                continue
            if h != expected_sha256(e):
                skipped.append(f"{name}: text does not match the listed sha256")
                continue
            keep.add(name)
            path = os.path.join(shared, name, "SKILL.md")
            try:
                with open(path, encoding="utf-8") as f:
                    if f.read() == md:
                        continue
            except OSError:
                pass
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(md)
            os.replace(tmp, path)
            installed.append(name)
        if os.path.isdir(shared):
            for name in os.listdir(shared):
                if name not in keep and os.path.isdir(os.path.join(shared, name)):
                    shutil.rmtree(os.path.join(shared, name), ignore_errors=True)
                    removed.append(name)
    if installed or removed or skipped:
        logger.info("[Trellis] skills sync: installed %s, removed %s, skipped %s", installed, removed, skipped)
    return {"installed": installed, "removed": removed, "skipped": skipped,
            "trusted": sorted(keep), "in_basket": listing.get("count", 0)}


def start(base: str, key: str) -> None:
    """Once per process: the gateway's adapter calls this on connect."""
    global _started
    if _started or httpx is None or not key:
        return
    _started = True

    def loop():
        time.sleep(20)
        while True:
            try:
                sync_once(base, key)
            except Exception as err:
                logger.warning("[Trellis] skills sync failed: %s", err)
            time.sleep(INTERVAL)

    threading.Thread(target=loop, name="trellis-skills-sync", daemon=True).start()
