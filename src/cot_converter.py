"""Bidirectional translation between Cursor on Target XML and compact LXMF fields.

A CoT event is 400-800 bytes of XML; the same information packs into 60-120 bytes
of MessagePack once redundant markup and default attributes are dropped.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree as ET

import msgpack

# Compact integer field keys for the LXMF payload dictionary.
F_UID = 0x01
F_TYPE = 0x02
F_LAT = 0x03
F_LON = 0x04
F_HAE = 0x05
F_CALLSIGN = 0x06
F_GROUP = 0x07
F_TIMESTAMP = 0x08
F_TEXT = 0x09

DEFAULT_TYPE = "a-f-G-U-C"
DEFAULT_HOW = "m-g"
GEOCHAT_TYPE = "b-t-f"

# Detail children that carry no tactical information over a constrained link.
STRIPPED_TAGS = ("_flow-tags_", "flow-tags_", "archive", "status", "takv", "precisionlocation")

_COT_TIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"


def _parse_cot_time(value: str | None) -> int:
    """Return the epoch seconds encoded in a CoT timestamp attribute."""
    if not value:
        return int(time.time())
    text = value.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except ValueError:
        return int(time.time())


def _format_cot_time(epoch: int) -> str:
    """Render epoch seconds as a millisecond-precision CoT timestamp."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(_COT_TIME_FMT)[:-3] + "Z"


def _float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def strip_redundant(xml_str: str) -> str:
    """Remove flow tags and other non-tactical detail children from a CoT event."""
    event = ET.fromstring(xml_str)
    detail = event.find("detail")
    if detail is not None:
        for child in list(detail):
            if child.tag in STRIPPED_TAGS:
                detail.remove(child)
    return ET.tostring(event, encoding="unicode")


def xml_to_lxmf_fields(xml_str: str) -> dict[int, Any]:
    """Convert a CoT XML event into a compact LXMF field dictionary.

    Only populated fields are emitted so that stationary or chat-only events stay small.
    """
    event = ET.fromstring(xml_str)
    if event.tag != "event":
        raise ValueError(f"expected a CoT <event> root, got <{event.tag}>")

    point = event.find("point")
    detail = event.find("detail")

    fields: dict[int, Any] = {
        F_UID: event.get("uid", ""),
        F_TYPE: event.get("type", DEFAULT_TYPE),
        F_TIMESTAMP: _parse_cot_time(event.get("time") or event.get("start")),
    }

    if point is not None:
        fields[F_LAT] = _float(point.get("lat"))
        fields[F_LON] = _float(point.get("lon"))
        fields[F_HAE] = _float(point.get("hae"))

    if detail is not None:
        contact = detail.find("contact")
        if contact is not None:
            callsign = contact.get("callsign")
            if callsign:
                fields[F_CALLSIGN] = callsign

        group = detail.find("__group")
        if group is not None:
            name = group.get("name")
            role = group.get("role")
            if name:
                fields[F_GROUP] = f"{name}/{role}" if role else name

        chat = detail.find("__chat")
        if chat is not None:
            room = chat.get("chatroom") or chat.get("groupOwner")
            sender = chat.get("senderCallsign")
            if room:
                fields[F_GROUP] = room
            if sender and F_CALLSIGN not in fields:
                fields[F_CALLSIGN] = sender

        remarks = detail.find("remarks")
        if remarks is not None and remarks.text:
            fields[F_TEXT] = remarks.text

    return fields


def lxmf_fields_to_xml(fields: dict[int, Any], stale_seconds: int = 300) -> str:
    """Rebuild a minimal, ATAK-ingestible CoT XML event from LXMF fields."""
    timestamp = int(fields.get(F_TIMESTAMP, time.time()))
    event_time = _format_cot_time(timestamp)
    stale = _format_cot_time(timestamp + stale_seconds)

    event = ET.Element(
        "event",
        {
            "version": "2.0",
            "uid": str(fields.get(F_UID, "")),
            "type": str(fields.get(F_TYPE, DEFAULT_TYPE)),
            "how": DEFAULT_HOW,
            "time": event_time,
            "start": event_time,
            "stale": stale,
        },
    )
    ET.SubElement(
        event,
        "point",
        {
            "lat": f"{float(fields.get(F_LAT, 0.0)):.7f}",
            "lon": f"{float(fields.get(F_LON, 0.0)):.7f}",
            "hae": f"{float(fields.get(F_HAE, 0.0)):.1f}",
            "ce": "9999999.0",
            "le": "9999999.0",
        },
    )

    detail = ET.SubElement(event, "detail")
    callsign = fields.get(F_CALLSIGN)
    if callsign:
        ET.SubElement(detail, "contact", {"callsign": str(callsign)})

    group = fields.get(F_GROUP)
    text = fields.get(F_TEXT)
    if group and text is None:
        name, _, role = str(group).partition("/")
        attrs = {"name": name}
        if role:
            attrs["role"] = role
        ET.SubElement(detail, "__group", attrs)

    if text is not None:
        chat_attrs = {"id": str(fields.get(F_UID, "")), "senderCallsign": str(callsign or "")}
        if group:
            chat_attrs["chatroom"] = str(group)
        ET.SubElement(detail, "__chat", chat_attrs)
        remarks = ET.SubElement(detail, "remarks")
        remarks.text = str(text)

    return ET.tostring(event, encoding="unicode")


def pack_fields(fields: dict[int, Any]) -> bytes:
    """Serialize an LXMF field dictionary to MessagePack bytes."""
    return msgpack.packb(fields, use_bin_type=True)


def unpack_fields(payload: bytes) -> dict[int, Any]:
    """Deserialize MessagePack bytes back into an LXMF field dictionary."""
    return msgpack.unpackb(payload, raw=False, strict_map_key=False)


def is_geochat(fields: dict[int, Any]) -> bool:
    """Whether the event carries chat text rather than a bare position report."""
    return bool(fields.get(F_TEXT))


def stale_cutoff(hours: int = 24) -> float:
    """Epoch seconds before which tracks are considered stale."""
    return (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).timestamp()
