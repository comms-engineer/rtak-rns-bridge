"""ATAK GeoChat target to LXMF Group Hub destination hash resolution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from src.cot_converter import F_GROUP, F_TEXT, xml_to_lxmf_fields

DESTINATION_HASH_BYTES = 16


class GroupResolutionError(ValueError):
    """Raised when a chat target cannot be mapped onto a group hub."""


def parse_group_target(xml_str: str) -> str | None:
    """Extract the human-readable GeoChat target name from a CoT event."""
    event = ET.fromstring(xml_str)
    detail = event.find("detail")
    if detail is None:
        return None
    chat = detail.find("__chat")
    if chat is None:
        return None
    return chat.get("chatroom") or chat.get("groupOwner") or chat.get("id")


class GroupResolver:
    """Maps chat room names onto 16-byte Reticulum destination hashes."""

    def __init__(self, groups: dict[str, str]) -> None:
        self._groups: dict[str, bytes] = {}
        for name, hex_hash in groups.items():
            self._groups[name.casefold()] = self._decode_hash(name, hex_hash)

    @classmethod
    def from_file(cls, path: str | Path) -> GroupResolver:
        """Load the name-to-hash table from config/groups.json."""
        with open(path, encoding="utf-8") as handle:
            return cls(json.load(handle))

    @staticmethod
    def _decode_hash(name: str, hex_hash: str) -> bytes:
        try:
            raw = bytes.fromhex(hex_hash)
        except ValueError as exc:
            raise GroupResolutionError(f"group '{name}' has a non-hex hash") from exc
        if len(raw) != DESTINATION_HASH_BYTES:
            raise GroupResolutionError(
                f"group '{name}' hash must be {DESTINATION_HASH_BYTES} bytes, got {len(raw)}"
            )
        return raw

    def resolve(self, target: str) -> bytes:
        """Return the destination hash for a chat room name."""
        try:
            return self._groups[target.casefold()]
        except KeyError as exc:
            raise GroupResolutionError(f"no group hub configured for '{target}'") from exc

    def resolve_optional(self, target: str) -> bytes | None:
        """Return the destination hash, or None when the group is unknown."""
        return self._groups.get(target.casefold())

    def known_groups(self) -> list[str]:
        """Configured group names, as supplied in the config file."""
        return sorted(self._groups)

    def wrap_geochat(self, xml_str: str) -> tuple[bytes, dict[int, Any]]:
        """Translate a GeoChat CoT event into a destination hash and LXMF fields."""
        target = parse_group_target(xml_str)
        if not target:
            raise GroupResolutionError("event carries no GeoChat target")
        destination = self.resolve(target)
        fields = xml_to_lxmf_fields(xml_str)
        if F_TEXT not in fields:
            raise GroupResolutionError("GeoChat event carries no remarks text")
        fields[F_GROUP] = target
        return destination, fields
