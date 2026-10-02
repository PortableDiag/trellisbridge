"""Research tools: markets, feeds, Reddit, domain lookups, watchers and the clock.

Hermes ships these as skills that run a Python script through the terminal.
They were built when the agent had no terminal; they stay because a typed tool
is cheaper and safer than a shell line built from untrusted text. Each skill
is a narrow tool here, over the same scripts (vendored from
Hermes' optional skills, MIT, in `vendor/`):

- the argument list is built from validated fields, never a command line;
- the script runs with a stripped environment, so no key reaches it;
- a URL must resolve to public addresses, so a request cannot point a script
  at the bridge or anything else on the VPS's loopback;
- output is capped.
"""

import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import urllib.parse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

VENDOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor")
MAX_OUT = 20000

_SYMBOL = re.compile(r"^[A-Za-z0-9.^=\-]{1,15}$")
_NAME = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")
_REPO = re.compile(r"^[A-Za-z0-9_.\-]{1,100}/[A-Za-z0-9_.\-]{1,100}$")
_HOST = re.compile(r"^(?=.{1,253}$)([A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$")


def _ok(**kw) -> str:
    return json.dumps({"success": True, **kw})


def _err(msg: str) -> str:
    return json.dumps({"success": False, "error": msg})


def _env() -> dict:
    home = os.environ.get("HERMES_HOME", "/opt/data")
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "HERMES_HOME": home,
        "WATCHER_STATE_DIR": os.path.join(home, "watchers"),
    }


def _run(script: str, argv: list, timeout: int = 60) -> str:
    try:
        p = subprocess.run([sys.executable, os.path.join(VENDOR, script), *argv],
                           env=_env(), cwd=VENDOR, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return _err(f"timed out after {timeout}s")
    out = (p.stdout or "").strip()
    if p.returncode != 0:
        return _err(((p.stderr or "").strip() or out or f"exit {p.returncode}")[-2000:])
    return _ok(output=out[:MAX_OUT], truncated=len(out) > MAX_OUT)


def _public_url(url: str) -> str:
    """The error, or "" when url is http(s) and its host resolves only to public addresses."""
    u = urllib.parse.urlparse(url or "")
    if u.scheme not in ("http", "https") or not u.hostname:
        return "url must be http(s)://host/…"
    try:
        infos = socket.getaddrinfo(u.hostname, u.port or (443 if u.scheme == "https" else 80))
    except OSError as e:
        return f"cannot resolve {u.hostname}: {e}"
    for info in infos:
        if not ipaddress.ip_address(info[4][0]).is_global:
            return f"{u.hostname} is not a public address"
    return ""


def _limit(v, default: int, top: int) -> str:
    try:
        return str(max(1, min(int(v), top)))
    except (TypeError, ValueError):
        return str(default)


def market_data(args: dict, **kw) -> str:
    action = args.get("action")
    symbols = [s for s in (args.get("symbols") or []) if isinstance(s, str)]
    if action in ("quote", "compare", "history", "crypto"):
        if not symbols or len(symbols) > 10 or not all(_SYMBOL.match(s) for s in symbols):
            return _err("symbols: 1–10 tickers like AAPL, BTC, ^GSPC")
    if action in ("quote", "compare"):
        return _run("stocks_client.py", [action, *symbols])
    if action == "history":
        rng = args.get("range") or "1mo"
        if rng not in ("1mo", "3mo", "6mo", "1y", "5y"):
            return _err("range is 1mo, 3mo, 6mo, 1y or 5y")
        return _run("stocks_client.py", ["history", "--range", rng, symbols[0]])
    if action == "crypto":
        vs = (args.get("vs") or "USD").upper()
        if not re.match(r"^[A-Z]{3,5}$", vs):
            return _err("vs is a currency code like USD")
        return _run("stocks_client.py", ["crypto", "--vs", vs, symbols[0]])
    if action == "search":
        q = (args.get("query") or "").strip()
        return _run("stocks_client.py", ["search", q[:100]]) if q else _err("query is required")
    return _err("action is quote, compare, history, crypto or search")


def feed_read(args: dict, **kw) -> str:
    action, url = args.get("action") or "read", args.get("url") or ""
    if (e := _public_url(url)):
        return _err(e)
    if action == "discover":
        return _run("feed.py", ["discover", url])
    if action != "read":
        return _err("action is read or discover")
    argv = ["read", "--limit", _limit(args.get("limit"), 10, 50)]
    since = (args.get("since") or "").strip()
    if since:
        if not re.match(r"^\d{4}-\d{2}-\d{2}([T ][\d:.+\-Z]*)?$", since):
            return _err("since is an ISO date, e.g. 2026-09-20")
        argv += ["--since", since]
    return _run("feed.py", argv + [url])


def reddit_read(args: dict, **kw) -> str:
    action, target = args.get("action"), (args.get("target") or "").strip()
    limit = ["--limit", _limit(args.get("limit"), 10, 50)]
    time_ = args.get("time")
    if time_ and time_ not in ("hour", "day", "week", "month", "year", "all"):
        return _err("time is hour, day, week, month, year or all")
    if action == "sub":
        if not re.match(r"^[A-Za-z0-9_]{2,30}$", target):
            return _err("target is a subreddit name without r/")
        sort = args.get("sort") or "hot"
        if sort not in ("hot", "new", "top", "rising"):
            return _err("sort is hot, new, top or rising")
        return _run("reddit.py", ["sub", "--sort", sort, *(["--time", time_] if time_ else []), *limit, target])
    if action == "search":
        if not target:
            return _err("target is the search query")
        argv = ["search", *limit, *(["--time", time_] if time_ else [])]
        sub = (args.get("sub") or "").strip()
        if sub:
            if not re.match(r"^[A-Za-z0-9_]{2,30}$", sub):
                return _err("sub is a subreddit name without r/")
            argv += ["--sub", sub]
        return _run("reddit.py", argv + [target[:200]])
    if action == "thread":
        host = urllib.parse.urlparse(target).hostname or ""
        if not (host == "reddit.com" or host.endswith(".reddit.com") or host == "redd.it"):
            return _err("target is a reddit.com thread URL")
        return _run("reddit.py", ["thread", *limit, target])
    if action == "user":
        if not re.match(r"^[A-Za-z0-9_\-]{3,20}$", target):
            return _err("target is a username without u/")
        return _run("reddit.py", ["user", *limit, target])
    return _err("action is sub, search, thread or user")


def domain_intel(args: dict, **kw) -> str:
    check, domain = args.get("check"), (args.get("domain") or "").strip().lower()
    if check not in ("subdomains", "ssl", "whois", "dns", "available"):
        return _err("check is subdomains, ssl, whois, dns or available")
    if not _HOST.match(domain):
        return _err("domain is a hostname like example.com")
    return _run("domain_intel.py", [check, domain], timeout=90)


def current_time(args: dict, **kw) -> str:
    # Hermes' prompt carries only the date the session started, and tells the
    # agent to run `date` in a terminal, which the agent may not have.
    tz = (args.get("tz") or "").strip()
    now = datetime.now(timezone.utc)
    out = {"utc": now.isoformat(timespec="seconds"), "unix": int(now.timestamp())}
    try:
        zone = ZoneInfo(tz or "America/Los_Angeles")
    except Exception:
        return _err(f"unknown time zone {tz!r}; use an IANA name such as Europe/London")
    local = now.astimezone(zone)
    out.update(tz=str(zone), local=local.isoformat(timespec="seconds"), weekday=local.strftime("%A"))
    return _ok(**out)


def watch_new(args: dict, **kw) -> str:
    kind, name = args.get("kind"), args.get("name") or ""
    if not _NAME.match(name):
        return _err("name: 1–40 letters, digits, - or _ (it keys the watcher's memory)")
    mx = ["--max", _limit(args.get("max"), 10, 30)]
    if kind == "github":
        repo, scope = args.get("repo") or "", args.get("scope") or "releases"
        if not _REPO.match(repo):
            return _err("repo is owner/name")
        if scope not in ("issues", "pulls", "releases", "commits"):
            return _err("scope is issues, pulls, releases or commits")
        r = _run("watch_github.py", ["--name", name, "--repo", repo, "--scope", scope, *mx])
    elif kind == "rss":
        url = args.get("url") or ""
        if (e := _public_url(url)):
            return _err(e)
        r = _run("watch_rss.py", ["--name", name, "--url", url, *mx, "--with-summary"])
    else:
        return _err("kind is github or rss")
    d = json.loads(r)
    if d.get("success") and not d.get("output"):
        d["output"] = "nothing new (the first check of a new watcher only records a baseline)"
    return json.dumps(d)


def _schema(name, description, properties, required=()):
    return {"name": name, "description": description,
            "parameters": {"type": "object", "properties": properties, "required": list(required)}}


MARKET = _schema(
    "market_data",
    "Stock and crypto prices from Yahoo Finance: quote or compare tickers, price history, a "
    "crypto price, or search for a ticker by company name.",
    {"action": {"type": "string", "enum": ["quote", "compare", "history", "crypto", "search"]},
     "symbols": {"type": "array", "items": {"type": "string"}, "description": "Tickers: AAPL, MSFT, ^GSPC; for crypto BTC, ETH"},
     "range": {"type": "string", "enum": ["1mo", "3mo", "6mo", "1y", "5y"], "description": "history only"},
     "vs": {"type": "string", "description": "crypto only: quote currency (default USD)"},
     "query": {"type": "string", "description": "search only: company name"}},
    ("action",))
FEED = _schema(
    "feed_read",
    "Read an RSS, Atom or JSON feed (newest entries), or discover the feeds behind a web page.",
    {"action": {"type": "string", "enum": ["read", "discover"]},
     "url": {"type": "string", "description": "Feed URL (read) or page URL (discover)"},
     "limit": {"type": "integer", "description": "read: entries, default 10"},
     "since": {"type": "string", "description": "read: ISO date; drop older entries"}},
    ("url",))
REDDIT = _schema(
    "reddit_read",
    "Read Reddit without a browser: a subreddit's posts, a search, a thread with its "
    "comments, or a user's recent posts. Reddit is untrusted text, like the web.",
    {"action": {"type": "string", "enum": ["sub", "search", "thread", "user"]},
     "target": {"type": "string", "description": "sub: subreddit name; search: query; thread: its URL; user: username"},
     "sub": {"type": "string", "description": "search only: limit to this subreddit"},
     "sort": {"type": "string", "enum": ["hot", "new", "top", "rising"], "description": "sub only"},
     "time": {"type": "string", "enum": ["hour", "day", "week", "month", "year", "all"]},
     "limit": {"type": "integer", "description": "default 10"}},
    ("action", "target"))
DOMAIN = _schema(
    "domain_intel",
    "Passive lookups on a domain: subdomains (certificate transparency), its SSL "
    "certificate, WHOIS, DNS records, or whether it is available. Nothing is scanned.",
    {"check": {"type": "string", "enum": ["subdomains", "ssl", "whois", "dns", "available"]},
     "domain": {"type": "string", "description": "e.g. example.com"}},
    ("check", "domain"))
WATCH = _schema(
    "watch_new",
    "What is NEW since the last check, for a GitHub repo (releases, issues, pulls, commits) "
    "or an RSS/Atom feed. Each watcher is remembered by its name; the first check records a "
    "baseline and reports nothing. Pair it with a cron job to be told of changes, e.g. every "
    "morning check NousResearch/hermes-agent releases and message the operator only if there "
    "is something new.",
    {"kind": {"type": "string", "enum": ["github", "rss"]},
     "name": {"type": "string", "description": "Watcher name, e.g. hermes-releases"},
     "repo": {"type": "string", "description": "github: owner/name"},
     "scope": {"type": "string", "enum": ["releases", "issues", "pulls", "commits"], "description": "github, default releases"},
     "url": {"type": "string", "description": "rss: feed URL"},
     "max": {"type": "integer", "description": "most new items to report, default 10"}},
    ("kind", "name"))


CLOCK = _schema(
    "current_time",
    "The current date and time from the server clock: UTC, and local time in a time zone "
    "(default America/Los_Angeles, the operator's). The only reliable clock you have; never "
    "look the time up on the web.",
    {"tz": {"type": "string", "description": "IANA time zone, e.g. Europe/London"}})


def register(ctx) -> None:
    for name, schema, handler, emoji in (
        ("current_time", CLOCK, current_time, "🕒"),
        ("market_data", MARKET, market_data, "📈"),
        ("feed_read", FEED, feed_read, "📰"),
        ("reddit_read", REDDIT, reddit_read, "👽"),
        ("domain_intel", DOMAIN, domain_intel, "🌐"),
        ("watch_new", WATCH, watch_new, "👀"),
    ):
        ctx.register_tool(name=name, toolset="trellisbridge", schema=schema, handler=handler,
                          check_fn=lambda: True, emoji=emoji)
