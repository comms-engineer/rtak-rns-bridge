"""Spatial delta throttling and latest-only outbound queueing.

Radio links (LoRa, HF, packet) have a duty cycle budget far below the CoT emission
rate of a TAK end user device. Positions are therefore only released when the track
has actually moved, and while the link is down the queue keeps a single, newest
payload per UID instead of a breadcrumb history.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

from src.cot_converter import F_LAT, F_LON, F_TIMESTAMP, F_UID

EARTH_RADIUS_METERS = 6371008.8


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS84 coordinates, in metres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * EARTH_RADIUS_METERS * math.asin(math.sqrt(a))


@dataclass
class _TrackState:
    lat: float
    lon: float
    sent_at: float


@dataclass
class QueueManager:
    """Decides which position reports earn airtime, and buffers them while offline."""

    min_distance_meters: float = 30.0
    max_heartbeat_seconds: float = 300.0
    purge_stale_queue_on_reconnect: bool = True
    online: bool = True
    _last_sent: dict[str, _TrackState] = field(default_factory=dict)
    _outbound: dict[str, dict[int, Any]] = field(default_factory=dict)

    def should_emit(self, fields: dict[int, Any], now: float | None = None) -> bool:
        """Whether the event has moved far enough, or waited long enough, to transmit."""
        now = time.time() if now is None else now
        uid = str(fields.get(F_UID, ""))
        lat, lon = fields.get(F_LAT), fields.get(F_LON)
        if lat is None or lon is None:
            # Non-positional events (chat, alerts) are never throttled.
            return True

        previous = self._last_sent.get(uid)
        if previous is None:
            return True
        if now - previous.sent_at >= self.max_heartbeat_seconds:
            return True
        return haversine_meters(previous.lat, previous.lon, float(lat), float(lon)) > (
            self.min_distance_meters
        )

    def submit(self, fields: dict[int, Any], now: float | None = None) -> bool:
        """Offer an event to the queue.

        Returns True when the event was accepted (queued or ready to send) and False
        when spatial delta throttling discarded it.
        """
        now = time.time() if now is None else now
        if not self.should_emit(fields, now=now):
            return False

        uid = str(fields.get(F_UID, ""))
        # Latest-only semantics: an older pending payload for this UID is worthless.
        self._outbound[uid] = fields
        lat, lon = fields.get(F_LAT), fields.get(F_LON)
        if lat is not None and lon is not None:
            self._last_sent[uid] = _TrackState(float(lat), float(lon), now)
        return True

    def set_online(self, online: bool) -> None:
        """Mark the RF transport up or down."""
        self.online = online

    def flush(self) -> list[dict[int, Any]]:
        """Drain the queue, newest state per UID, and hand it to the caller."""
        if not self.online:
            return []
        pending = sorted(
            self._outbound.values(), key=lambda payload: int(payload.get(F_TIMESTAMP, 0))
        )
        self._outbound.clear()
        return pending

    def purge(self) -> None:
        """Drop every pending payload without transmitting it."""
        self._outbound.clear()

    def reconnect(self, now: float | None = None) -> list[dict[int, Any]]:
        """Bring the transport back up and release the surviving state.

        With purging enabled, payloads whose state is older than the heartbeat window are
        dropped rather than transmitted: they no longer describe where the track is.
        """
        now = time.time() if now is None else now
        self.set_online(True)
        if self.purge_stale_queue_on_reconnect:
            for uid, payload in list(self._outbound.items()):
                age = now - int(payload.get(F_TIMESTAMP, now))
                if age > self.max_heartbeat_seconds:
                    del self._outbound[uid]
        return self.flush()

    @property
    def pending(self) -> int:
        """Number of UIDs with a payload awaiting transmission."""
        return len(self._outbound)
