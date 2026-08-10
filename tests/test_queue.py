"""Spatial delta throttling and latest-only queue tests."""

from __future__ import annotations

import pytest

from src.cot_converter import F_LAT, F_LON, F_TIMESTAMP, F_UID
from src.queue_manager import QueueManager, haversine_meters

BASE_LAT = 51.478
BASE_LON = -0.0014
METERS_PER_DEGREE_LAT = 111_320.0


def position(uid: str, lat: float, lon: float, timestamp: int) -> dict[int, object]:
    return {F_UID: uid, F_LAT: lat, F_LON: lon, F_TIMESTAMP: timestamp}


def offset_north(meters: float) -> float:
    return BASE_LAT + meters / METERS_PER_DEGREE_LAT


def test_haversine_matches_a_known_short_baseline() -> None:
    assert haversine_meters(BASE_LAT, BASE_LON, offset_north(100.0), BASE_LON) == pytest.approx(
        100.0, abs=0.5
    )


def test_second_update_within_five_meters_is_not_emitted() -> None:
    queue = QueueManager()

    assert queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0) is True
    assert queue.submit(position("UID-01", offset_north(5.0), BASE_LON, 1001), now=1001.0) is False
    assert queue.pending == 1
    assert queue.flush()[0][F_LAT] == pytest.approx(BASE_LAT)


def test_movement_beyond_the_threshold_is_emitted() -> None:
    queue = QueueManager(min_distance_meters=30.0)
    queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0)
    queue.flush()

    assert queue.submit(position("UID-01", offset_north(45.0), BASE_LON, 1010), now=1010.0) is True


def test_stationary_track_still_emits_a_heartbeat() -> None:
    queue = QueueManager(max_heartbeat_seconds=300.0)
    queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0)
    queue.flush()

    assert queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1200), now=1200.0) is False
    assert queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1300), now=1300.0) is True


def test_offline_breadcrumbs_collapse_to_the_latest_state() -> None:
    queue = QueueManager()
    queue.set_online(False)

    for step in range(1, 11):
        accepted = queue.submit(
            position("UID-01", offset_north(40.0 * step), BASE_LON, 1000 + step),
            now=1000.0 + step,
        )
        assert accepted is True

    assert queue.pending == 1
    assert queue.flush() == []

    flushed = queue.reconnect(now=1011.0)
    assert len(flushed) == 1
    assert flushed[0][F_LAT] == pytest.approx(offset_north(400.0))
    assert flushed[0][F_TIMESTAMP] == 1010
    assert queue.pending == 0


def test_reconnect_purges_state_older_than_the_heartbeat_window() -> None:
    queue = QueueManager(max_heartbeat_seconds=300.0, purge_stale_queue_on_reconnect=True)
    queue.set_online(False)
    queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0)

    assert queue.reconnect(now=1000.0 + 3600) == []


def test_reconnect_keeps_stale_state_when_purging_is_disabled() -> None:
    queue = QueueManager(max_heartbeat_seconds=300.0, purge_stale_queue_on_reconnect=False)
    queue.set_online(False)
    queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0)

    assert len(queue.reconnect(now=1000.0 + 3600)) == 1


def test_queue_tracks_each_uid_independently() -> None:
    queue = QueueManager()
    queue.set_online(False)
    queue.submit(position("UID-01", BASE_LAT, BASE_LON, 1000), now=1000.0)
    queue.submit(position("UID-02", BASE_LAT, BASE_LON, 1001), now=1001.0)

    assert queue.pending == 2
    assert {str(payload[F_UID]) for payload in queue.reconnect(now=1002.0)} == {"UID-01", "UID-02"}


def test_events_without_a_position_are_never_throttled() -> None:
    queue = QueueManager()
    chat = {F_UID: "UID-01", F_TIMESTAMP: 1000}

    assert queue.submit(chat, now=1000.0) is True
    assert queue.submit(chat, now=1000.5) is True
