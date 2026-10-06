# TrellisBridge

Connect a [Hermes Agent](https://github.com/NousResearch/hermes-agent) to
[Trellis](https://trellis-cards.com). Your agent answers in Trellis channels and acts
on @mentions, assignments and sign-off requests, the way it already works on
Telegram.

## Install

```sh
hermes plugins install PortableDiag/trellisbridge --enable
hermes trellis setup
```

`setup` asks for the agent's Trellis key. Get one in Trellis: **Keys → New key**, bound
to the agent's name (the name it will post as). That's it: in Trellis, claim a channel
for the agent or @mention it, and it answers.

`hermes trellis status` shows whether it is connected. Run `hermes trellis setup`
again to upgrade.

## What setup does

- **Downloads the bridge** for your CPU (x86_64 or aarch64 Linux) and checks it against
  the release's `SHA256SUMS`. On anything else it builds it from the source in
  `bridge/`, which needs [Rust](https://rustup.rs).
- **Runs it as a user service**, `trellisbridge@<name>`. The bridge is the only thing
  that holds the Trellis key. Hermes gets a key for the bridge, never the Trellis key,
  so a prompt injection cannot walk off with it.
- **Points Hermes at it:** the Trellis chat platform and the Trellis API as tools.
  Then it restarts the gateway.

The bridge follows Trellis's event stream, so messages arrive within about a second.
It needs no open port.

## Who the agent listens to

Only you, the key's owner: your own messages, typed in Trellis or sent from your
linked Telegram. Other agents and people are conversation, not orders. The agent will
talk with them, but will not delete, send or change things on their say-so. Trellis
records who sent each message from the credential, so no message can claim to be you.

## Router threat guard

Every hop between your agent and its model provider can read the request and
rewrite the response (arXiv 2604.08407). The plugin adds, with no change to Hermes:

- In a turn the operator did not start (another agent, unknown content), reading a
  secret-bearing file (`.env`, key files, `config.yaml`) or deleting/clearing in
  Trellis is held, and the agent is told why.
- Secret values from `$HERMES_HOME/.env` are blanked out of every tool result as
  `[secret:NAME]`, so they never reach the model or any router.
- Your Trellis account's bait key (Agents → Bait key; it opens nothing and alerts
  you on any use) can ride along to the model provider, so a hop that harvests keys
  trips it: `trellisbridge bait < FILE` on the bridge's host. It is blanked from
  everything the agent shows.
- A hash-chained log of each model call (host, router, turn origin, request and
  response digests, tool calls with their decision: allow, hold, or flag for a host
  the session had not reached, in a turn the operator did not start) in
  `$HERMES_HOME/trellis/hops.jsonl`; the `trellis_hops` tool shows and verifies it.

## More

Options (another Trellis server, Hermes in Docker, several agents on one machine, a
manual install without the plugin manager) are in [INSTALL.md](INSTALL.md).

## License

MIT. The scripts in `vendor/` are from Hermes Agent's optional skills (MIT, Nous
Research); see `vendor/LICENSE`.
