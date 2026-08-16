"""Edge bridge, state dump, group routing and access control tests."""

from __future__ import annotations

import json
import socket
import threading
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pytest

from src.access_control import AccessController, PeerRole
from src.cot_converter import F_CALLSIGN, F_LAT, F_TEXT, F_UID, unpack_fields
from src.edge_bridge import EdgeBridge, split_events
from src.group_resolver import GroupResolutionError, GroupResolver, parse_group_target
from src.state_db import StateDB

GROUPS = {
    "Team Cyan": "7e2a91b40c98f12110aef53123b0981d",
    "All Staff": "f8910b2a9c0d12e4f51198e7d23a4b0e",
}
EDGE_HASH = "7e2a91b40c98f12110aef53123b0981d"
SIBLING_HASH = "4a2c91b40c98f12110aef53123b0981a"


class FakeTransport:
    """Captures outbound payloads instead of putting them on the air."""

    def __init__(self, online: bool = True) -> None:
        self.online = online
        self.sent: list[tuple[bytes, dict[int, Any]]] = []

    def send(self, destination_hash: bytes, fields: dict[int, Any]) -> bool:
        if not self.online:
            return False
        self.sent.append((destination_hash, fields))
        return True

    def start(self) -> None:
        self.online = True

    def stop(self) -> None:
        self.online = False


class FakeIdentity:
    def __init__(self, identity_hash: str) -> None:
        self.hash = bytes.fromhex(identity_hash)


class FakeLink:
    """Minimal stand-in for RNS.Link exposing only what access control touches."""

    def __init__(self, identity: FakeIdentity | None) -> None:
        self._identity = identity
        self.torn_down = False

    def get_remote_identity(self) -> FakeIdentity | None:
        return self._identity

    def teardown(self) -> None:
        self.torn_down = True


@pytest.fixture
def config(tmp_path: Path) -> dict[str, Any]:
    return {
        "node_type": "edge",
        "callsign": "PHANTOM-GATEWAY-01",
        "default_group": "Team Cyan",
        "tcp_listener": {"host": "127.0.0.1", "port": 0},
        "telemetry_filter": {"min_distance_meters": 30.0, "max_heartbeat_seconds": 300},
        "database": {"sqlite_path": str(tmp_path / "state.db")},
    }


@pytest.fixture
def bridge(config: dict[str, Any], tmp_path: Path) -> EdgeBridge:
    groups_path = tmp_path / "groups.json"
    groups_path.write_text(json.dumps(GROUPS), encoding="utf-8")
    instance = EdgeBridge(
        config,
        transport=FakeTransport(),
        db=StateDB(config["database"]["sqlite_path"]),
        resolver=GroupResolver.from_file(groups_path),
        access=AccessController([SIBLING_HASH], [EDGE_HASH]),
    )
    yield instance
    instance.close()


def test_position_is_translated_persisted_and_transmitted(
    bridge: EdgeBridge, position_cot: str
) -> None:
    fields = bridge.ingest(position_cot, now=1000.0)

    assert fields is not None
    destination, sent = bridge.transport.sent[0]
    assert destination == bytes.fromhex(GROUPS["Team Cyan"])
    assert sent[F_CALLSIGN] == "PHANTOM-01"
    assert [track.callsign for track in bridge.db.active_tracks(now=fields[8])] == ["PHANTOM-01"]


def test_duplicate_event_is_dropped(bridge: EdgeBridge, position_cot: str) -> None:
    assert bridge.ingest(position_cot, now=1000.0) is not None
    assert bridge.ingest(position_cot, now=1005.0) is None
    assert len(bridge.transport.sent) == 1


def test_throttled_position_is_not_transmitted(bridge: EdgeBridge, make_position: Any) -> None:
    first = make_position("UID-01", 51.478, -0.0014, "2026-08-10T12:00:00.000Z")
    nudged = make_position("UID-01", 51.4780449, -0.0014, "2026-08-10T12:00:10.000Z")

    assert bridge.ingest(first, now=1000.0) is not None
    assert bridge.ingest(nudged, now=1010.0) is None
    assert len(bridge.transport.sent) == 1


def test_geochat_is_routed_to_the_resolved_group_hub(
    bridge: EdgeBridge, geochat_cot: str
) -> None:
    fields = bridge.ingest(geochat_cot, now=1000.0)

    assert fields is not None
    destination, sent = bridge.transport.sent[0]
    assert destination == bytes.fromhex(GROUPS["Team Cyan"])
    assert sent[F_TEXT] == "Moving to checkpoint bravo"
    assert [message.text for message in bridge.db.recent_chat(now=fields[8])] == [
        "Moving to checkpoint bravo"
    ]


def test_offline_transport_marks_the_queue_down(
    bridge: EdgeBridge, position_cot: str, event_epoch: int
) -> None:
    bridge.transport.online = False

    assert bridge.ingest(position_cot, now=1000.0) is None
    assert bridge.queue.online is False

    bridge.transport.online = True
    assert bridge.queue.pending == 1
    assert bridge.flush_queue(now=event_epoch + 10) == 1


def test_state_dump_writes_cot_xml_to_a_connected_client(
    bridge: EdgeBridge, position_cot: str
) -> None:
    """A late-joining TAK client is handed the current picture the moment it connects."""
    bridge.ingest(position_cot, now=1000.0)

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    client.settimeout(5)
    server_side, _ = listener.accept()

    written: list[int] = []
    thread = threading.Thread(
        target=lambda: written.append(bridge.db.dump_current_state_to_socket(server_side))
    )
    thread.start()
    thread.join(timeout=5)

    payload = client.recv(65536).decode("utf-8")
    for handle in (client, server_side, listener):
        handle.close()

    assert written == [1]
    event = ET.fromstring(payload.strip().splitlines()[0])
    assert event.tag == "event"
    assert event.get("uid") == "ANDROID-352cba1f1234"
    point = event.find("point")
    assert point is not None
    assert float(point.get("lat")) == pytest.approx(51.478)


def test_state_dump_skips_tracks_outside_the_catch_up_window(
    bridge: EdgeBridge, position_cot: str, event_epoch: int
) -> None:
    bridge.ingest(position_cot, now=1000.0)

    assert bridge.db.active_tracks(now=event_epoch + 60) != []
    assert bridge.db.active_tracks(now=event_epoch + 24 * 3600 + 60) == []


def test_split_events_handles_partial_and_batched_reads(make_position: Any) -> None:
    first = make_position("UID-01", 51.478, -0.0014, "2026-08-10T12:00:00.000Z").encode()
    second = make_position("UID-02", 51.479, -0.0014, "2026-08-10T12:00:01.000Z").encode()

    events, remainder = split_events(first + second[:40])
    assert len(events) == 1
    events, remainder = split_events(remainder + second[40:])
    assert len(events) == 1
    assert remainder == b""


def test_server_to_server_state_sync_merges_snapshots(
    bridge: EdgeBridge, position_cot: str, tmp_path: Path
) -> None:
    bridge.ingest(position_cot, now=1000.0)
    sibling = StateDB(tmp_path / "sibling.db")

    assert sibling.import_state(bridge.db.export_state()) == 1
    assert [track.uid for track in sibling.active_tracks()] == ["ANDROID-352cba1f1234"]

    # A replayed snapshot carries no newer timestamps, so nothing is applied twice.
    assert sibling.import_state(bridge.db.export_state()) == 0
    sibling.close()


def test_group_target_parsing_and_resolution(geochat_cot: str) -> None:
    resolver = GroupResolver(GROUPS)

    assert parse_group_target(geochat_cot) == "Team Cyan"
    assert resolver.resolve("team cyan") == bytes.fromhex(GROUPS["Team Cyan"])
    assert len(resolver.resolve("All Staff")) == 16

    with pytest.raises(GroupResolutionError, match="no group hub configured"):
        resolver.resolve("Team Magenta")


def test_group_hashes_must_be_sixteen_bytes() -> None:
    with pytest.raises(GroupResolutionError, match="16 bytes"):
        GroupResolver({"Team Short": "7e2a91b4"})


def test_unauthorized_identities_are_rejected_and_links_torn_down() -> None:
    access = AccessController([SIBLING_HASH], [EDGE_HASH])

    assert access.is_authorized(EDGE_HASH, PeerRole.EDGE_NODE) is True
    assert access.is_authorized(EDGE_HASH, PeerRole.SIBLING) is False
    assert access.role_of(SIBLING_HASH) is PeerRole.SIBLING
    assert access.role_of("00" * 16) is None

    rogue = FakeLink(FakeIdentity("11" * 16))
    assert access.authorize_link(rogue, PeerRole.SIBLING) is False
    assert rogue.torn_down is True

    sibling = FakeLink(FakeIdentity(SIBLING_HASH))
    assert access.authorize_link(sibling, PeerRole.SIBLING) is True
    assert sibling.torn_down is False


def test_unidentified_links_are_torn_down_when_signatures_are_required() -> None:
    strict = AccessController([SIBLING_HASH], [EDGE_HASH], require_signatures=True)
    anonymous = FakeLink(None)

    assert strict.authorize_link(anonymous, PeerRole.SIBLING) is False
    assert anonymous.torn_down is True

    permissive = AccessController(require_signatures=False)
    assert permissive.authorize_link(FakeLink(None), PeerRole.SIBLING) is True


def test_inbound_lxmf_payload_is_persisted(bridge: EdgeBridge, position_cot: str) -> None:
    from src.cot_converter import pack_fields, xml_to_lxmf_fields

    fields = xml_to_lxmf_fields(position_cot)
    decoded = unpack_fields(pack_fields(fields))
    bridge.db.apply_fields("GeoChat.test.hash", decoded)

    stored = bridge.db.active_tracks(now=float(fields[8]))
    assert len(stored) == 1
    assert stored[0].lat == pytest.approx(fields[F_LAT], abs=1e-6)
    assert str(decoded[F_UID]) == "ANDROID-352cba1f1234"
