"""`hermes trellis harden`: the router-threat settings a fresh or relaunched Hermes
needs beside the plugin's own guard (DESIGN D15; Trellis #401). Run by
`hermes trellis setup` at the end, and on its own any time; it is idempotent.

- No training: `provider_routing.data_collection: deny` when unset.
- Pinned hosts (C2), only the ones the operator names with `--pin a,b`: the main
  model is routed only to them (in that order), and the side tasks that would
  run on it (compression, titles, curator) are pinned to the same model and
  hosts, so none is routed to an auto-picked model. Each host is first tried
  with a one-token request under the no-training rule; one that refuses stops
  the pin. Without `--pin`, the hosts serving the model are listed.
- Bait key (C1): `--bait-file FILE` hands it to this Hermes's bridge
  (`trellisbridge bait`), when the bridge runs as this user.

Hermes's own writer (`atomic_config_write`) keeps the file's comments and
refuses to write over a config it cannot read.
"""

import json
import os
import subprocess
import urllib.request
from pathlib import Path

SIDE_TASKS = ("compression", "title_generation", "curator")
OPENROUTER = "https://openrouter.ai/api/v1"


def _say(what: str, line: str) -> None:
    print(f"  {what:<9} {line}")


def _endpoints(model: str) -> list:
    try:
        with urllib.request.urlopen(f"{OPENROUTER}/models/{model}/endpoints", timeout=20) as r:
            return json.load(r).get("data", {}).get("endpoints", []) or []
    except Exception:
        return []


def _try_host(model: str, host: str, key: str) -> str:
    """'' when `host` serves `model` under the no-training rule, else why not."""
    body = {"model": model, "max_tokens": 1, "messages": [{"role": "user", "content": "ok"}],
            "provider": {"only": [host], "data_collection": "deny"}}
    req = urllib.request.Request(f"{OPENROUTER}/chat/completions", json.dumps(body).encode(),
                                 {"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60):
            return ""
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read()).get("error", {}).get("message", f"HTTP {e.code}")[:160]
        except Exception:
            return f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        return str(e)[:160]


def _bridge_config() -> Path | None:
    """This Hermes's bridge config, matched by the port in TRELLISBRIDGE_URL."""
    url = os.environ.get("TRELLISBRIDGE_URL", "")
    port = url.rsplit(":", 1)[-1].split("/")[0] if ":" in url else ""
    for cfg in sorted((Path.home() / ".config" / "trellisbridge").glob("*/config.toml")):
        try:
            if port and f"port = {port}" in cfg.read_text():
                return cfg
        except OSError:
            continue
    return None


def _bait(path: str) -> int:
    cfg = _bridge_config()
    binary = Path.home() / ".local" / "bin" / "trellisbridge"
    if not cfg or not binary.exists():
        _say("bait", "this bridge does not run as this user: on its host run "
                     "`trellisbridge bait < FILE` as the bridge's user (README, Bait key)")
        return 1
    with open(path, "rb") as f:
        r = subprocess.run([str(binary), "--config", str(cfg), "bait"], stdin=f, capture_output=True, text=True)
    _say("bait", (r.stdout or r.stderr).strip().replace("bait      ", ""))
    return r.returncode


def harden(pin: str = "", bait_file: str = "", dry_run: bool = False) -> int:
    from hermes_cli.config import atomic_config_write, get_config_path, read_raw_config

    print("Router-threat settings (Trellis #401):")
    raw = read_raw_config() or {}
    changed = False
    pr = raw.setdefault("provider_routing", {})
    if not isinstance(pr, dict):
        print("  provider_routing is not a mapping; leaving the config alone")
        return 1
    dc = pr.get("data_collection")
    if dc is None:
        pr["data_collection"] = "deny"
        changed = True
        _say("training", "data_collection: deny (providers that train on prompts are excluded)")
    elif dc != "deny":
        _say("training", f"data_collection: {dc} — left as set; `deny` keeps prompts out of training")
    else:
        _say("training", "data_collection: deny")

    m = raw.get("model") or {}
    model = (m.get("default") or m.get("model") or "") if isinstance(m, dict) else str(m)
    provider = (m.get("provider") or "") if isinstance(m, dict) else ""
    hosts = [h.strip() for h in pin.split(",") if h.strip()]
    rc = 0
    if provider != "openrouter" or not model:
        _say("hosts", f"main model {model or '?'} on {provider or '?'}: not OpenRouter, nothing to pin")
    elif hosts:
        key = os.environ.get("OPENROUTER_API_KEY", "")
        if key:
            for h in hosts:
                why = _try_host(model, h, key)
                if why:
                    _say("hosts", f"{h} refused {model} under the no-training rule: {why}. Nothing pinned.")
                    return 1
            _say("hosts", f"tried: {', '.join(hosts)} each served {model} with no training")
        else:
            _say("hosts", "no OPENROUTER_API_KEY here, so the hosts were not tried first")
        prefs = {"only": hosts, "order": hosts}
        models = pr.setdefault("models", {})
        if models.get(model) != prefs:
            models[model] = prefs
            changed = True
        aux = raw.setdefault("auxiliary", {})
        for task in SIDE_TASKS:
            t = aux.get(task) if isinstance(aux.get(task), dict) else {}
            eb = t.get("extra_body") if isinstance(t.get("extra_body"), dict) else {}
            want = {**t, "provider": "openrouter", "model": model,
                    "extra_body": {**eb, "provider": {"data_collection": "deny", **prefs}}}
            if t != want:
                aux[task] = want
                changed = True
        _say("hosts", f"{model} only from {' then '.join(hosts)}; side tasks {', '.join(SIDE_TASKS)} on the same")
    else:
        current = (pr.get("models") or {}).get(model) if isinstance(pr.get("models"), dict) else None
        if current:
            _say("hosts", f"{model} pinned: {current}")
        else:
            eps = _endpoints(model)
            names = sorted({e.get("tag", "").split("/")[0] for e in eps if e.get("tag")})
            _say("hosts", f"{model} is NOT pinned: OpenRouter may route it to any of {len(names)} hosts")
            if names:
                _say("", ", ".join(names))
            _say("", "pin the ones you trust: hermes trellis harden --pin host1,host2")

    if changed and not dry_run:
        atomic_config_write(get_config_path(), raw)
        _say("config", f"saved {get_config_path()} (restart the gateway to apply)")
    elif changed:
        _say("config", "dry run: nothing saved")

    if bait_file:
        rc = _bait(bait_file) or rc
    else:
        try:
            from .tools import _bridge
            base, headers = _bridge()
            with urllib.request.urlopen(urllib.request.Request(f"{base}/api/bait", headers=headers), timeout=5) as r:
                b = json.load(r).get("bait")
            _say("bait", f"set on the bridge ({len(b)} characters)" if b else
                 "none: copy Agents → Bait key in Trellis to a file, then --bait-file FILE")
        except Exception:
            _say("bait", "bridge not answering; check with: hermes trellis status")
    _say("guard", "the plugin's gate, secret blanking and hop log are on (trellis_hops)")
    return rc
