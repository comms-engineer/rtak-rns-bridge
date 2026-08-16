# rtak-rns-bridge

Bridges Cursor on Target (CoT) tactical data from ATAK / iTAK / WinTAK across the
Reticulum Network Stack (RNS) using LXMF messaging.

A CoT event is 400-800 bytes of XML, which no LoRa or HF link can carry at the rate a TAK
end user device emits it. This bridge compacts every event into a MessagePack dictionary
with integer field keys, throttles position reports by spatial delta, keeps only the newest
state per track while the radio is down, and routes GeoChat into federated LXMF group hubs.
Peers are authenticated by RNS identity hash rather than TLS client certificates.

## Architecture

```
ATAK / iTAK  --TCP 8087-->  edge_bridge  --LXMF/RNS-->  server_hub  <--RNS Link-->  sibling hub
                                |                            |
                          queue + dedup                   state.db
                                |                       (tracks, chat)
                          cot_converter
```

| Module | Responsibility |
| --- | --- |
| `src/cot_converter.py` | CoT XML <-> compact LXMF MessagePack fields |
| `src/queue_manager.py` | Haversine spatial delta throttling, latest-only outbound queue |
| `src/dedup.py` | Rolling 60 s SHA-256 window, `GeoChat.<UID>.<HASH>` tagging |
| `src/group_resolver.py` | GeoChat room name -> 16-byte group hub destination hash |
| `src/state_db.py` | SQLite tracks and chat history, late-joiner state dump, S2S sync |
| `src/access_control.py` | RNS identity allow lists for siblings and edge nodes |
| `src/edge_bridge.py` | Async CoT TCP listener and LXMF egress |
| `src/server_hub.py` | RNS server daemon and federation sync handler |
| `src/watchdog.py` | Socket bind and `RNS.Transport` health monitor with auto-recovery |

### LXMF field keys

| Key | Type | Field |
| --- | --- | --- |
| `0x01` | string | `uid` |
| `0x02` | string | `type` (e.g. `a-f-G-U-C`) |
| `0x03` | float | `lat` |
| `0x04` | float | `lon` |
| `0x05` | float | `hae` |
| `0x06` | string | `callsign` |
| `0x07` | string | `group` / team role |
| `0x08` | int | `timestamp` (epoch seconds) |
| `0x09` | string | `text` (GeoChat content) |

Only populated keys are packed, so a bare position report costs well under a third of the
equivalent XML.

## Configuration

- `config/config.json` — node type, callsign, TCP listener, Reticulum paths, telemetry
  filter thresholds, SQLite path. `default_group` names the group hub that positions are
  published to.
- `config/groups.json` — GeoChat room name to 16-byte hex destination hash.
- `config/access_control.json` — `authorized_siblings` and `authorized_edge_nodes` identity
  hashes; set `require_signatures` to `false` only on a closed bench network.

Telemetry defaults: a position is transmitted when the track has moved more than
`min_distance_meters` (30 m) or `max_heartbeat_seconds` (300 s) have elapsed. With
`purge_stale_queue_on_reconnect` enabled, queued state older than the heartbeat window is
discarded on reconnect instead of being transmitted as a stale breadcrumb.

## Running

```bash
pip install -r requirements.txt

# Edge node: ATAK on the same handset uses 127.0.0.1, iTAK over Wi-Fi uses 0.0.0.0.
python -m src.edge_bridge --config config/config.json

# Server hub
python -m src.server_hub --config config/config.json
```

Point ATAK at the listener with a TCP streaming connection to `<host>:8087`. On connect the
bridge immediately replays every track updated in the last 24 hours as CoT XML, so a late
joiner sees the current picture without waiting for the next heartbeat.

### Docker

```bash
docker compose up --build
```

`rnsd` owns the Reticulum interfaces on the host network; `rtak-bridge` runs the translation
stack against it, with `./config` and `./data` mounted in.

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest -q
```

Reticulum and LXMF are imported lazily, so the translation pipeline and the whole test
suite run on a host with no RNS installation and no radio attached.
