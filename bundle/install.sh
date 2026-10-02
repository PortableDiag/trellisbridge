#!/usr/bin/env bash
# Install TrellisBridge for one Hermes agent.
#
#   the bridge   holds the agent's Trellis key, long-polls Trellis, serves
#                events + MCP on 127.0.0.1 (one bridge per Hermes)
#   the plugin   `trellis-platform` in that Hermes: Trellis channels become a
#                chat platform it answers on, as Telegram is; holds only the
#                bridge key
#
# Re-running it upgrades in place: the binary and plugin are replaced, the
# bridge's config, key and cursors are kept.
set -euo pipefail

usage() {
  cat <<'EOF'
install.sh [options]

  --key-file PATH        the agent's Trellis key (a key minted in Trellis → Keys,
                         ideally bound to the agent's name)
  --key-from-env VAR     read it from VAR in the Hermes .env instead (e.g.
                         MCP_TRELLIS_API_KEY); with neither, it is asked for
  --agent NAME           the agent's name in Trellis (default: the name the key
                         is bound to)
  --hermes-home DIR      Hermes data dir on this host (default $HERMES_HOME or ~/.hermes)
  --hermes-cmd "CMD"     how to run the Hermes CLI (default: hermes; for Docker:
                         "docker compose -f /srv/x/docker-compose.yml exec -T svc hermes")
  --name NAME            this bridge's instance name (default: from the Hermes dir,
                         ~/.hermes → hermes); one per Hermes
  --port N               bridge port (default: the next free from 8791)
  --url URL              Trellis server (default https://trellis-cards.com)
  --avatar PATH          the agent's picture in Trellis channels (png/jpeg/webp/gif,
                         at most 256 KB; 256×256 is plenty)
  --description TEXT     one line on the agent's card: what it does
  --no-mcp               leave Hermes's mcp_servers alone (default: point
                         mcp_servers.trellis at the bridge)
  --no-systemd           do not install a systemd user unit; print the run command
  --no-restart           do not restart the Hermes gateway
EOF
}

die() { echo "install: $*" >&2; exit 1; }
say() { echo "  $*"; }

KEY_FILE= KEY_ENV= AGENT= NAME= PORT= URL= AVATAR= DESC= MCP=1 SYSTEMD=1 RESTART=1
HERMES_HOME_DIR=${HERMES_HOME:-$HOME/.hermes}
HERMES_CMD=hermes
while [ $# -gt 0 ]; do
  case $1 in
    --key-file) KEY_FILE=$2; shift ;;
    --key-from-env) KEY_ENV=$2; shift ;;
    --agent) AGENT=$2; shift ;;
    --hermes-home) HERMES_HOME_DIR=$2; shift ;;
    --hermes-cmd) HERMES_CMD=$2; shift ;;
    --name) NAME=$2; shift ;;
    --port) PORT=$2; shift ;;
    --url) URL=$2; shift ;;
    --avatar) AVATAR=$2; shift ;;
    --description) DESC=$2; shift ;;
    --no-mcp) MCP=0 ;;
    --no-systemd) SYSTEMD=0 ;;
    --no-restart) RESTART=0 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown option $1" ;;
  esac
  shift
done

HERE=$(cd "$(dirname "$0")" && pwd)
HERMES_HOME_DIR=$(cd "$HERMES_HOME_DIR" 2>/dev/null && pwd) || die "no Hermes dir at $HERMES_HOME_DIR (--hermes-home)"
ENV_FILE=$HERMES_HOME_DIR/.env
[ -n "$NAME" ] || NAME=$(basename "$HERMES_HOME_DIR" | sed 's/^\.//; s/[^A-Za-z0-9_-]/-/g')
[ -n "$NAME" ] || NAME=hermes
BIN_DIR=${BIN_DIR:-$HOME/.local/bin}
CONF_ROOT=${XDG_CONFIG_HOME:-$HOME/.config}/trellisbridge
CONF_DIR=$CONF_ROOT/$NAME
CONFIG=$CONF_DIR/config.toml
umask 077

echo "TrellisBridge $(cat "$HERE/VERSION") → Hermes at $HERMES_HOME_DIR (bridge instance \"$NAME\")"

# 1. The binary: the bundled one for this machine, else built from the bundled source.
ARCH=$(uname -m)
BIN=$HERE/bin/trellisbridge-$ARCH-linux
if [ "$(uname -s)" != Linux ] || [ ! -x "$BIN" ]; then
  command -v cargo >/dev/null || die "no prebuilt binary for $(uname -s)/$ARCH and no cargo to build one (https://rustup.rs)"
  say "building from source for $(uname -s)/$ARCH…"
  cargo build --release --quiet --manifest-path "$HERE/source/Cargo.toml" --target-dir "$HERE/source/target"
  BIN=$HERE/source/target/release/trellisbridge
fi
mkdir -p "$BIN_DIR"
install -m 755 "$BIN" "$BIN_DIR/trellisbridge.new" && mv -f "$BIN_DIR/trellisbridge.new" "$BIN_DIR/trellisbridge"
TB=$BIN_DIR/trellisbridge
say "binary    $TB ($("$TB" --version))"

# 2. The bridge's config and key. An existing config is kept (its bridge key and
#    cursors with it); a key given again replaces the stored one.
mkdir -p "$CONF_DIR"
if [ -n "$KEY_FILE" ]; then
  [ -s "$KEY_FILE" ] || die "$KEY_FILE is empty or missing"
  tr -d ' \r\n' < "$KEY_FILE" > "$CONF_DIR/trellis.key"
elif [ -n "$KEY_ENV" ]; then
  grep -E "^${KEY_ENV}=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d "\"' \r\n" > "$CONF_DIR/trellis.key"
  [ -s "$CONF_DIR/trellis.key" ] || die "no $KEY_ENV in $ENV_FILE"
elif [ ! -s "$CONF_DIR/trellis.key" ]; then
  [ -t 0 ] || die "no key: pass --key-file or --key-from-env"
  read -rsp "  Trellis key for this agent (not shown): " k; echo
  [ -n "$k" ] || die "no key given"
  printf '%s' "$k" > "$CONF_DIR/trellis.key"; unset k
fi
chmod 600 "$CONF_DIR/trellis.key"

if [ ! -f "$CONFIG" ]; then
  if [ -z "$PORT" ]; then
    # Next port from 8791 that nothing listens on and no other bridge here claims.
    taken=$({ cat "$CONF_ROOT"/*/config.toml "$CONF_ROOT"/config.toml 2>/dev/null || true; } | sed -n 's/^port *= *\([0-9]*\).*/\1/p')
    PORT=8791
    while echo "$taken" | grep -qx "$PORT" || (command -v ss >/dev/null && ss -ltnH "sport = :$PORT" | grep -q .); do PORT=$((PORT + 1)); done
  fi
  args=(init --key-file "$CONF_DIR/trellis.key" --port "$PORT")
  [ -n "$AGENT" ] && args+=(--agent "$AGENT")
  [ -n "$URL" ] && args+=(--url "$URL")
  "$TB" --config "$CONFIG" "${args[@]}" | sed 's/^/  /'
else
  say "config    $CONFIG (kept)"
  "$TB" --config "$CONFIG" check | sed 's/^/  /'
fi
# The agent card: its picture and one line, shown in channels (web 0.81.0).
if [ -n "$AVATAR" ] || [ -n "$DESC" ]; then
  card=(card)
  if [ -n "$AVATAR" ]; then
    [ -s "$AVATAR" ] || die "no picture at $AVATAR"
    ext=${AVATAR##*.}; cp "$AVATAR" "$CONF_DIR/avatar.$ext"; chmod 644 "$CONF_DIR/avatar.$ext"
    card+=(--avatar "$CONF_DIR/avatar.$ext")
  fi
  [ -n "$DESC" ] && card+=(--description "$DESC")
  "$TB" --config "$CONFIG" "${card[@]}" | sed 's/^/  /'
fi
PORT=$(sed -n 's/^port *= *\([0-9]*\).*/\1/p' "$CONFIG" | head -1)
BRIDGE_URL=http://127.0.0.1:$PORT
BRIDGE_KEY=$("$TB" --config "$CONFIG" key)

# 3. Run it: a systemd user unit per instance, kept running across logouts.
if [ "$SYSTEMD" = 1 ] && command -v systemctl >/dev/null && systemctl --user show-environment >/dev/null 2>&1; then
  UNIT_DIR=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
  mkdir -p "$UNIT_DIR"
  sed "s|@BIN@|$TB|; s|@CONF_ROOT@|$CONF_ROOT|" "$HERE/trellisbridge@.service" > "$UNIT_DIR/trellisbridge@.service"
  systemctl --user daemon-reload
  systemctl --user enable --quiet "trellisbridge@$NAME"
  systemctl --user restart "trellisbridge@$NAME"
  loginctl enable-linger "$USER" 2>/dev/null || say "note: 'loginctl enable-linger $USER' failed — the bridge stops when you log out"
  say "service   trellisbridge@$NAME (systemctl --user status trellisbridge@$NAME)"
  for _ in $(seq 1 30); do curl -fsS "$BRIDGE_URL/api/health" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS "$BRIDGE_URL/api/health" >/dev/null 2>&1 || { journalctl --user -u "trellisbridge@$NAME" -n 20 --no-pager >&2; die "the bridge did not come up on $BRIDGE_URL"; }
  say "health    $(curl -fsS "$BRIDGE_URL/api/health")"
else
  say "run it    $TB --config $CONFIG serve   (keep it running: a service, tmux, …)"
fi

# 4. Hermes: the plugin, its env, the MCP server.
PLUG=$HERMES_HOME_DIR/plugins/trellis
mkdir -p "$HERMES_HOME_DIR/plugins"
rm -rf "$PLUG.new" && cp -r "$HERE/hermes-plugin/trellis" "$PLUG.new"
find "$PLUG.new" -name __pycache__ -prune -exec rm -rf {} +
rm -rf "$PLUG" && mv "$PLUG.new" "$PLUG"
say "plugin    $PLUG ($(sed -n 's/^version: //p' "$PLUG/plugin.yaml"))"

upsert_env() {  # NAME VALUE — replace or append one line, keep the file's mode
  touch "$ENV_FILE"
  local tmp; tmp=$(mktemp "$ENV_FILE.XXXX")
  grep -v -E "^$1=" "$ENV_FILE" > "$tmp" || true
  printf '%s=%s\n' "$1" "$2" >> "$tmp"
  chmod --reference="$ENV_FILE" "$tmp" 2>/dev/null || chmod 600 "$tmp"
  mv -f "$tmp" "$ENV_FILE"
}
upsert_env TRELLISBRIDGE_KEY "$BRIDGE_KEY"
upsert_env TRELLISBRIDGE_URL "$BRIDGE_URL"
# The bridge is the trust gate (DESIGN D11): it marks each message trusted or a
# peer's, and the plugin acts on that. Hermes's own name allowlist would answer
# the operator with a pairing prompt, so it is off for this platform.
upsert_env TRELLIS_ALLOW_ALL_USERS true
# Hermes asks for a home channel (cron results, cross-platform sends) on the
# first message; the bridge's home is the first channel it answers in.
if ! grep -qE "^TRELLIS_HOME_CHANNEL=." "$ENV_FILE"; then
  HOME_CARD=$(curl -fsS "$BRIDGE_URL/api/health" 2>/dev/null | sed -n 's/.*"home":{"card":\([0-9]*\).*/\1/p')
  [ -n "$HOME_CARD" ] && upsert_env TRELLIS_HOME_CHANNEL "$HOME_CARD" && say "home      Trellis channel card $HOME_CARD (TRELLIS_HOME_CHANNEL)"
fi
say "env       TRELLISBRIDGE_KEY, TRELLISBRIDGE_URL=$BRIDGE_URL, TRELLIS_ALLOW_ALL_USERS=true in $ENV_FILE"

# shellcheck disable=SC2086  # HERMES_CMD is a command line
hermes_cli() { HERMES_HOME=$HERMES_HOME_DIR $HERMES_CMD "$@"; }
cp -p "$HERMES_HOME_DIR/config.yaml" "$HERMES_HOME_DIR/config.yaml.before-trellisbridge" 2>/dev/null || true
hermes_cli plugins enable trellis-platform >/dev/null && say "enabled   trellis-platform"
if [ "$MCP" = 1 ]; then
  hermes_cli config set mcp_servers.trellis.url "$BRIDGE_URL/mcp" >/dev/null
  hermes_cli config set mcp_servers.trellis.headers.Authorization 'Bearer ${TRELLISBRIDGE_KEY}' >/dev/null
  hermes_cli config set mcp_servers.trellis.enabled true >/dev/null
  say "mcp       mcp_servers.trellis → $BRIDGE_URL/mcp (bridge key; Hermes holds no Trellis key)"
  if grep -qE "^MCP_TRELLIS_API_KEY=" "$ENV_FILE"; then
    say "note: $ENV_FILE still has MCP_TRELLIS_API_KEY — nothing uses it now; remove it to keep the Trellis key out of Hermes"
  fi
fi

if [ "$RESTART" = 1 ]; then
  hermes_cli gateway restart >/dev/null 2>&1 && say "restarted the Hermes gateway" \
    || say "restart the Hermes gateway yourself: hermes gateway restart"
fi

AGENT_NAME=$(sed -n 's/^agent *= *"\(.*\)"/\1/p' "$CONFIG" | head -1)
cat <<EOF

Done. In Trellis, $AGENT_NAME answers in:
  - a channel claimed for it (channel card → claim → $AGENT_NAME), or listed in
    trellis.channels in $CONFIG
  - any channel where a message @-mentions $AGENT_NAME
Only the operator's own messages are orders; other agents are peers (DESIGN D11/D12).
EOF
