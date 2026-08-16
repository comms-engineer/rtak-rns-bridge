---
name: testing-rtak-rns-bridge
description: How to run and end-to-end test the rtak-rns-bridge CoT<->Reticulum/LXMF daemon (edge bridge TCP listener, throttling, GeoChat routing, real LXMF egress) without ATAK hardware.
---

# Testing the rtak-rns-bridge edge bridge

Headless daemon + TCP socket protocol. There is no UI — test with shell + scripted sockets,
never with a browser, and do not record video.

## Environment

```bash
python3 -m venv .venv && .venv/bin/pip install -e . && .venv/bin/pip install pytest ruff
.venv/bin/pip install rns lxmf          # optional at import time, REQUIRED to run the daemon
```
`RNS`/`LXMF` are imported lazily, so unit tests pass without them, but
`python -m src.edge_bridge` calls `LXMFTransport.start()` at boot and will hard-fail with
`ImportError` if they are missing. PyPI has intermittently returned 502 for these — if the
install fails, retry; if it keeps failing, only the translation stack can be exercised.

Lint: `.venv/bin/ruff check .` — Unit tests: `.venv/bin/pytest -q`.

## Running a disposable instance

Never run against `config/config.json` as-is (`/etc/reticulum` is not writable and port 8087
may be taken). Copy it and override:
- `tcp_listener.port` → a free high port
- `database.sqlite_path` → a temp path
- `reticulum.config_dir` / `identity_keyfile` → a writable temp dir

Minimal writable Reticulum config (`<configdir>/config`) that avoids AutoInterface surprises:
```
[reticulum]
  enable_transport = Yes
  share_instance = No
  instance_name = bridge
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    listen_ip = 127.0.0.1
    listen_port = 43000
```
Start with `setsid .venv/bin/python -m src.edge_bridge --config <cfg> --verbose > log 2>&1 &`
(`--verbose` is required: throttle/dedup/malformed decisions are logged at DEBUG).
Readiness = the lines `LXMF transport online as <hash>` and `CoT listener bound to <host>:<port>`.

Note: `GroupResolver`/`AccessController` always load from the repo's `config/` directory
(`CONFIG_DIR` in src/edge_bridge.py), NOT from the config file you pass — to test a different
group mapping you must temporarily edit `config/groups.json` and restore it afterwards.

## Driving it like ATAK

Events are newline-agnostic; the framing is the literal `</event>` terminator
(`split_events`). Send raw CoT XML on the TCP port; a client receives the catch-up dump
(one CoT `<event>` per line) immediately on connect, before sending anything.

Gotchas that will make a correct build look broken:
- Dedup hashes `uid + int(time) + remarks-text`, so two events with the same `time` attribute
  are treated as duplicates regardless of position. Always vary `time`.
- `max_heartbeat_seconds` is measured against **wall clock**, not the event timestamp; you
  cannot trigger the heartbeat escape hatch by fast-forwarding the CoT `time` attribute. Run a
  second instance with `telemetry_filter.max_heartbeat_seconds` set to ~3 to exercise it.
- The spatial filter compares against the last *sent* position, which persists across client
  reconnects, so a "first" event after reconnect can legitimately be throttled.
- The watchdog opens and immediately closes a TCP connection to the listener every 10s; those
  show up as spurious `TAK client connected` log lines.

## Proving real LXMF egress

Run a second process with its own configdir and a `TCPClientInterface` to 127.0.0.1:43000 that
does `LXMF.LXMRouter(...).register_delivery_identity(...)` + `register_delivery_callback(...)`
and announces in a loop. Its `destination.hash.hex()` is exactly the 16-byte value
`config/groups.json` expects — drop it in as a group and point `default_group` at it. Delivery
over the loopback testnet lands within ~10-20s of the announce. Weaker fallback evidence when
no peer exists: the log line `no path yet for <hash>, requested`.

## Testing the queue and outage behaviour

- Delete the bridge's Reticulum `storage/` dir before boot to force a genuine "no path" state;
  that is the only reliable way to exercise the offline queue with a peer that exists.
- Queued payloads are retried by the 2s periodic flush and only dropped from the queue once the
  send succeeds, so an event ingested during an outage should be delivered exactly once when the
  hub appears — but `reconnect()` purges payloads older than `max_heartbeat_seconds` first, so an
  outage longer than that window intentionally drops the stale fix.
- Benign noise: watchdog probes connect and immediately reset, so
  `WARNING asyncio socket.send() raised exception.` appears alongside `TAK client ... dropped`.
  Tracebacks or `Task exception was never retrieved` are NOT benign — those are regressions.

## Devin Secrets Needed

None — everything runs locally on loopback.
