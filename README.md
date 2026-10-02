# TrellisBridge

Connect a [Hermes Agent](https://github.com/NousResearch/hermes-agent) to
[Trellis](https://trellis-cards.com). Your agent then answers in Trellis channels,
acts on @mentions, assignments and sign-off requests anywhere in your documents,
and can use the whole Trellis API, the way it already works on Telegram.

- **The bridge** (`trellisbridge`, Rust) holds the agent's Trellis key and listens to
  Trellis's agent event stream. It falls back to long-polling, and needs no public
  port. It hands events to Hermes and serves the Trellis API as MCP tools on
  `127.0.0.1`.
- **The plugin** (`hermes-plugin/trellis`) makes Trellis a chat platform inside
  Hermes. It holds only the bridge's own key, **never the Trellis key**, so a prompt
  injection cannot walk off with it.

One bridge per Hermes agent; several agents on one machine each get their own.

## Install

1. In Trellis, signed in, open **Keys** and mint a key **bound to the agent's
   name** (the name it will post as), scoped to the document or basket it
   should reach.
2. Download `trellisbridge-<version>.tar.gz` from
   [Releases](../../releases/latest) onto the machine that runs Hermes, then:

   ```sh
   tar xzf trellisbridge-*.tar.gz && cd trellisbridge-*/
   ./install.sh --key-file /path/to/trellis.key
   ```

   It installs the bridge as a systemd user service (`trellisbridge@<name>`), puts
   the plugin in `$HERMES_HOME/plugins/trellis`, points Hermes's MCP config at the
   bridge, and restarts the gateway. Static binaries are included for x86_64 and
   aarch64 Linux; anything else builds from the included source (needs
   [Rust](https://rustup.rs)). For Hermes in Docker, see `install.sh --help`.

3. Claim a channel for the agent in Trellis, or @mention it. It answers.

Full details: [`bundle/INSTALL.md`](bundle/INSTALL.md).

## Who the agent listens to

Only the **key owner** is obeyed: a person's message from your own account, typed in
a signed-in browser or your linked Telegram. Another agent's or another person's
message is conversation, not an order: the agent will talk with them, but will not
delete, send or change things on their say-so. A loop guard stops agents talking
in circles. Trellis records who sent each message from the credential, so no
message can claim to be you.

## Build from source

```sh
cargo build --release
scripts/bundle.sh        # dist/trellisbridge-<version>.tar.gz
```

The bridge is a small, blocking Rust program with no async runtime.

## License

MIT. The scripts in `hermes-plugin/trellis/vendor/` are from Hermes Agent's optional
skills (MIT, Nous Research); see the `LICENSE` there.
