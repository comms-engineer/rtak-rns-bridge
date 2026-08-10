"""Rolling deduplication window for federated re-broadcast loop mitigation.

Group hubs forward what they receive, so a message that traverses two hubs can arrive
back at its origin. A short sliding window of content hashes breaks those loops.
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from src.cot_converter import F_TEXT, F_TIMESTAMP, F_UID


def message_hash(sender_uid: str, timestamp: int | float, text: str) -> str:
    """SHA-256 over sender UID, timestamp and payload text."""
    digest = hashlib.sha256()
    digest.update(sender_uid.encode("utf-8"))
    digest.update(str(int(timestamp)).encode("utf-8"))
    digest.update(text.encode("utf-8"))
    return digest.hexdigest()


def fields_hash(fields: dict[int, Any]) -> str:
    """Content hash of an LXMF field dictionary."""
    return message_hash(
        str(fields.get(F_UID, "")),
        fields.get(F_TIMESTAMP, 0),
        str(fields.get(F_TEXT, "")),
    )


def geochat_uid(sender_uid: str, digest: str) -> str:
    """Build the ATAK-visible GeoChat event UID: GeoChat.<SENDER_UID>.<HASH>."""
    return f"GeoChat.{sender_uid}.{digest}"


@dataclass
class DeduplicationEngine:
    """Sliding-window cache of recently seen message hashes."""

    ttl_seconds: float = 60.0
    _seen: OrderedDict[str, float] = field(default_factory=OrderedDict)

    def _expire(self, now: float) -> None:
        for digest, seen_at in list(self._seen.items()):
            if now - seen_at >= self.ttl_seconds:
                del self._seen[digest]
            else:
                # OrderedDict is insertion ordered, so the rest are younger still.
                break

    def is_duplicate(self, fields: dict[int, Any], now: float | None = None) -> bool:
        """Whether this event was already seen inside the TTL window.

        Unseen events are recorded, so calling this twice with the same content returns
        False and then True.
        """
        now = time.time() if now is None else now
        self._expire(now)
        digest = fields_hash(fields)
        if digest in self._seen:
            return True
        self._seen[digest] = now
        return False

    def tag(self, fields: dict[int, Any]) -> str:
        """Return the GeoChat-formatted UID for an event."""
        return geochat_uid(str(fields.get(F_UID, "")), fields_hash(fields))

    @property
    def size(self) -> int:
        """Number of hashes currently held in the window."""
        return len(self._seen)
