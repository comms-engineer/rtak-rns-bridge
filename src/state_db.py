"""SQLite persistence for the tactical picture and chat history.

Late-joining end user devices get the current picture replayed at them the moment they
open a TCP socket, and sibling servers reconcile the same tables over an RNS Link.
"""

from __future__ import annotations

import socket
import sqlite3
import time
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.cot_converter import (
    F_CALLSIGN,
    F_GROUP,
    F_HAE,
    F_LAT,
    F_LON,
    F_TEXT,
    F_TIMESTAMP,
    F_TYPE,
    F_UID,
    lxmf_fields_to_xml,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS active_tracks (
    uid          TEXT PRIMARY KEY,
    callsign     TEXT,
    type         TEXT,
    lat          REAL,
    lon          REAL,
    hae          REAL,
    last_updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tracks_updated ON active_tracks (last_updated);

CREATE TABLE IF NOT EXISTS chat_history (
    msg_id       TEXT PRIMARY KEY,
    sender_uid   TEXT NOT NULL,
    target_group TEXT,
    text         TEXT NOT NULL,
    timestamp    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chat_timestamp ON chat_history (timestamp);
"""

CATCH_UP_WINDOW_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class Track:
    """A row of active_tracks."""

    uid: str
    callsign: str | None
    type: str
    lat: float
    lon: float
    hae: float
    last_updated: float

    def to_fields(self) -> dict[int, Any]:
        """Render the track as an LXMF field dictionary."""
        fields: dict[int, Any] = {
            F_UID: self.uid,
            F_TYPE: self.type,
            F_LAT: self.lat,
            F_LON: self.lon,
            F_HAE: self.hae,
            F_TIMESTAMP: int(self.last_updated),
        }
        if self.callsign:
            fields[F_CALLSIGN] = self.callsign
        return fields

    def to_xml(self) -> str:
        """Render the track as a CoT XML event."""
        return lxmf_fields_to_xml(self.to_fields())


@dataclass(frozen=True)
class ChatMessage:
    """A row of chat_history."""

    msg_id: str
    sender_uid: str
    target_group: str | None
    text: str
    timestamp: float


class StateDB:
    """Thread-safe SQLite store of tracks and chat history."""

    def __init__(self, path: str | Path = "state.db") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()

    def __enter__(self) -> StateDB:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def upsert_track(self, fields: dict[int, Any], now: float | None = None) -> None:
        """Insert or refresh the track described by an LXMF field dictionary."""
        uid = str(fields.get(F_UID, ""))
        if not uid:
            raise ValueError("cannot persist a track without a UID")
        last_updated = float(fields.get(F_TIMESTAMP, now if now is not None else time.time()))
        self._conn.execute(
            """
            INSERT INTO active_tracks (uid, callsign, type, lat, lon, hae, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                callsign = excluded.callsign,
                type = excluded.type,
                lat = excluded.lat,
                lon = excluded.lon,
                hae = excluded.hae,
                last_updated = excluded.last_updated
            """,
            (
                uid,
                fields.get(F_CALLSIGN),
                str(fields.get(F_TYPE, "")),
                float(fields.get(F_LAT, 0.0)),
                float(fields.get(F_LON, 0.0)),
                float(fields.get(F_HAE, 0.0)),
                last_updated,
            ),
        )
        self._conn.commit()

    def record_chat(self, msg_id: str, fields: dict[int, Any]) -> None:
        """Persist a GeoChat message, ignoring replays of a known message id."""
        self._conn.execute(
            """
            INSERT INTO chat_history (msg_id, sender_uid, target_group, text, timestamp)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(msg_id) DO NOTHING
            """,
            (
                msg_id,
                str(fields.get(F_UID, "")),
                fields.get(F_GROUP),
                str(fields.get(F_TEXT, "")),
                float(fields.get(F_TIMESTAMP, time.time())),
            ),
        )
        self._conn.commit()

    def active_tracks(
        self, window_seconds: float = CATCH_UP_WINDOW_SECONDS, now: float | None = None
    ) -> list[Track]:
        """Tracks updated inside the catch-up window, newest first."""
        now = time.time() if now is None else now
        rows = self._conn.execute(
            "SELECT * FROM active_tracks WHERE last_updated >= ? ORDER BY last_updated DESC",
            (now - window_seconds,),
        ).fetchall()
        return [
            Track(
                uid=row["uid"],
                callsign=row["callsign"],
                type=row["type"],
                lat=row["lat"],
                lon=row["lon"],
                hae=row["hae"],
                last_updated=row["last_updated"],
            )
            for row in rows
        ]

    def recent_chat(
        self, window_seconds: float = CATCH_UP_WINDOW_SECONDS, now: float | None = None
    ) -> list[ChatMessage]:
        """Chat messages inside the catch-up window, oldest first."""
        now = time.time() if now is None else now
        rows = self._conn.execute(
            "SELECT * FROM chat_history WHERE timestamp >= ? ORDER BY timestamp ASC",
            (now - window_seconds,),
        ).fetchall()
        return [
            ChatMessage(
                msg_id=row["msg_id"],
                sender_uid=row["sender_uid"],
                target_group=row["target_group"],
                text=row["text"],
                timestamp=row["timestamp"],
            )
            for row in rows
        ]

    def prune(self, window_seconds: float = CATCH_UP_WINDOW_SECONDS) -> int:
        """Delete state older than the window. Returns the number of rows removed."""
        cutoff = time.time() - window_seconds
        with closing(self._conn.cursor()) as cursor:
            cursor.execute("DELETE FROM active_tracks WHERE last_updated < ?", (cutoff,))
            removed = cursor.rowcount
            cursor.execute("DELETE FROM chat_history WHERE timestamp < ?", (cutoff,))
            removed += cursor.rowcount
        self._conn.commit()
        return removed

    def dump_current_state_to_socket(
        self,
        socket_conn: socket.socket,
        window_seconds: float = CATCH_UP_WINDOW_SECONDS,
        now: float | None = None,
    ) -> int:
        """Write the current picture to a freshly connected TAK client as CoT XML.

        Returns the number of events written.
        """
        events = [track.to_xml() for track in self.active_tracks(window_seconds, now=now)]
        for message in self.recent_chat(window_seconds, now=now):
            events.append(
                lxmf_fields_to_xml(
                    {
                        F_UID: message.msg_id,
                        F_TYPE: "b-t-f",
                        F_TIMESTAMP: int(message.timestamp),
                        F_TEXT: message.text,
                        F_GROUP: message.target_group or "",
                        F_CALLSIGN: message.sender_uid,
                    }
                )
            )
        for event in events:
            socket_conn.sendall(event.encode("utf-8") + b"\n")
        return len(events)

    def export_state(self, window_seconds: float = CATCH_UP_WINDOW_SECONDS) -> dict[str, Any]:
        """Snapshot for server-to-server synchronisation over an RNS Link."""
        return {
            "tracks": [track.to_fields() for track in self.active_tracks(window_seconds)],
            "chat": [
                {
                    "msg_id": message.msg_id,
                    "sender_uid": message.sender_uid,
                    "target_group": message.target_group,
                    "text": message.text,
                    "timestamp": message.timestamp,
                }
                for message in self.recent_chat(window_seconds)
            ],
        }

    def import_state(self, snapshot: dict[str, Any]) -> int:
        """Merge a sibling's snapshot. Newer timestamps win. Returns rows applied."""
        applied = 0
        for fields in snapshot.get("tracks", []):
            incoming = {int(key): value for key, value in fields.items()}
            uid = str(incoming.get(F_UID, ""))
            row = self._conn.execute(
                "SELECT last_updated FROM active_tracks WHERE uid = ?", (uid,)
            ).fetchone()
            if row is None or float(incoming.get(F_TIMESTAMP, 0)) > row["last_updated"]:
                self.upsert_track(incoming)
                applied += 1
        for message in snapshot.get("chat", []):
            self.record_chat(
                message["msg_id"],
                {
                    F_UID: message["sender_uid"],
                    F_GROUP: message.get("target_group"),
                    F_TEXT: message["text"],
                    F_TIMESTAMP: message["timestamp"],
                },
            )
            applied += 1
        return applied

    def apply_fields(self, msg_id: str, fields: dict[int, Any]) -> None:
        """Route an inbound event into the right table."""
        if fields.get(F_TEXT):
            self.record_chat(msg_id, fields)
        else:
            self.upsert_track(fields)


def iter_state_xml(tracks: Iterable[Track]) -> Iterable[str]:
    """CoT XML for each track, for callers that stream rather than buffer."""
    for track in tracks:
        yield track.to_xml()
