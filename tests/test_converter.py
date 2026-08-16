"""CoT XML <-> LXMF field dictionary translation tests."""

from __future__ import annotations

from xml.etree import ElementTree as ET

import pytest

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
    pack_fields,
    strip_redundant,
    unpack_fields,
    xml_to_lxmf_fields,
)


def test_position_round_trip_preserves_tactical_fields(position_cot: str) -> None:
    fields = xml_to_lxmf_fields(position_cot)
    round_tripped = xml_to_lxmf_fields(lxmf_fields_to_xml(fields))

    for key in (F_UID, F_TYPE, F_CALLSIGN):
        assert round_tripped[key] == fields[key]
    for key in (F_LAT, F_LON, F_HAE):
        assert round_tripped[key] == pytest.approx(fields[key], abs=1e-6)
    assert round_tripped[F_TIMESTAMP] == fields[F_TIMESTAMP]


def test_round_trip_through_messagepack(position_cot: str) -> None:
    fields = xml_to_lxmf_fields(position_cot)
    decoded = unpack_fields(pack_fields(fields))

    assert decoded == fields
    xml = lxmf_fields_to_xml(decoded)
    point = ET.fromstring(xml).find("point")
    assert point is not None
    assert float(point.get("lat")) == pytest.approx(51.478)
    assert float(point.get("lon")) == pytest.approx(-0.0014)


def test_compacted_payload_is_far_smaller_than_the_xml(position_cot: str) -> None:
    packed = pack_fields(xml_to_lxmf_fields(position_cot))

    assert len(packed) < len(position_cot.encode("utf-8")) / 3


def test_group_and_role_are_preserved(position_cot: str) -> None:
    fields = xml_to_lxmf_fields(position_cot)

    assert fields[F_GROUP] == "Cyan/Team Member"

    group = ET.fromstring(lxmf_fields_to_xml(fields)).find("detail/__group")
    assert group is not None
    assert group.get("name") == "Cyan"
    assert group.get("role") == "Team Member"


def test_geochat_text_and_room_survive_translation(geochat_cot: str) -> None:
    fields = xml_to_lxmf_fields(geochat_cot)

    assert fields[F_TEXT] == "Moving to checkpoint bravo"
    assert fields[F_GROUP] == "Team Cyan"

    detail = ET.fromstring(lxmf_fields_to_xml(fields)).find("detail")
    assert detail is not None
    assert detail.findtext("remarks") == "Moving to checkpoint bravo"
    chat = detail.find("__chat")
    assert chat is not None
    assert chat.get("chatroom") == "Team Cyan"


def test_strip_redundant_removes_flow_tags_and_telemetry(position_cot: str) -> None:
    detail = ET.fromstring(strip_redundant(position_cot)).find("detail")
    assert detail is not None

    tags = {child.tag for child in detail}
    assert tags == {"contact", "__group"}


def test_rebuilt_event_is_valid_cot_xml(position_cot: str) -> None:
    event = ET.fromstring(lxmf_fields_to_xml(xml_to_lxmf_fields(position_cot)))

    assert event.tag == "event"
    assert event.get("version") == "2.0"
    source = ET.fromstring(position_cot)
    assert event.get("time") == source.get("time")
    assert event.get("stale") == source.get("stale")


def test_non_event_root_is_rejected() -> None:
    with pytest.raises(ValueError, match="expected a CoT <event> root"):
        xml_to_lxmf_fields("<detail/>")
