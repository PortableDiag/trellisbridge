#!/usr/bin/env bash
# Build the shareable bundle: dist/trellisbridge-<version>.tar.gz
#
#   bin/trellisbridge-x86_64-linux    static (musl), runs on any x86_64 Linux
#   bin/trellisbridge-aarch64-linux   static (musl), cross-built with cargo-zigbuild
#   source/                           the crate, for any other machine (install.sh builds it)
#   hermes-plugin/trellis/            the Hermes platform plugin
#   install.sh, trellisbridge@.service, INSTALL.md, VERSION
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
TARGET_DIR=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/bundle
# Shared binaries carry no local paths: panic locations would otherwise name this
# machine's home and checkout. Built apart, so normal builds keep their cache.
export CARGO_TARGET_DIR=$TARGET_DIR
export RUSTFLAGS="--remap-path-prefix=$HOME/.cargo=/cargo --remap-path-prefix=$PWD=/trellisbridge --remap-path-prefix=$HOME=/home"

OUT=dist/trellisbridge-$VERSION
rm -rf "$OUT" "$OUT.tar.gz"
mkdir -p "$OUT/bin" "$OUT/hermes-plugin" "$OUT/source"

# ring compiles a little C; the host gcc does for a static musl binary.
CC_x86_64_unknown_linux_musl=${CC_x86_64_unknown_linux_musl:-gcc} cargo build --release --quiet --target x86_64-unknown-linux-musl
install -m 755 "$TARGET_DIR/x86_64-unknown-linux-musl/release/trellisbridge" "$OUT/bin/trellisbridge-x86_64-linux"
# aarch64: zig is the cross C compiler for ring (cargo-zigbuild, plus zig from
# PATH or the venv at ~/.local/share/zig-venv: `pip install ziglang`).
export PATH=$HOME/.local/share/zig-venv/bin:$PATH
if rustup target list --installed | grep -qx aarch64-unknown-linux-musl \
   && command -v cargo-zigbuild >/dev/null && { command -v zig >/dev/null || python3 -m ziglang version >/dev/null 2>&1; } \
   && cargo zigbuild --release --quiet --target aarch64-unknown-linux-musl; then
  install -m 755 "$TARGET_DIR/aarch64-unknown-linux-musl/release/trellisbridge" "$OUT/bin/trellisbridge-aarch64-linux"
else
  echo "bundle: no aarch64 binary (needs the aarch64-unknown-linux-musl target, cargo-zigbuild and zig); ARM hosts build from source/" >&2
fi

cp -r hermes-plugin/trellis "$OUT/hermes-plugin/trellis"
find "$OUT/hermes-plugin" -name __pycache__ -prune -exec rm -rf {} +
cp -r Cargo.toml Cargo.lock src "$OUT/source/"
cp bundle/install.sh bundle/trellisbridge@.service bundle/INSTALL.md "$OUT/"
chmod 755 "$OUT/install.sh"
echo "$VERSION" > "$OUT/VERSION"

for b in "$OUT"/bin/*; do
  if strings "$b" | grep -q -F -e "$HOME" -e "$PWD"; then echo "bundle: $b still names a local path" >&2; exit 1; fi
done
tar -C dist -czf "$OUT.tar.gz" "trellisbridge-$VERSION"
rm -rf "$OUT"
ls -l "$OUT.tar.gz"
tar -tzf "$OUT.tar.gz" | grep -E "bin/|install.sh"
