"""Rolling deduplication window tests."""

from __future__ import annotations

from src.cot_converter import F_TEXT, F_TIMESTAMP, F_UID, xml_to_lxmf_fields
from src.dedup import DeduplicationEngine, geochat_uid, message_hash


def test_identical_geochat_within_the_window_is_a_duplicate(geochat_cot: str) -> None:
    engine = DeduplicationEngine(ttl_seconds=60)
    fields = xml_to_lxmf_fields(geochat_cot)

    assert engine.is_duplicate(fields, now=1000.0) is False
    assert engine.is_duplicate(xml_to_lxmf_fields(geochat_cot), now=1030.0) is True


def test_hash_expires_once_the_window_closes(geochat_cot: str) -> None:
    engine = DeduplicationEngine(ttl_seconds=60)
    fields = xml_to_lxmf_fields(geochat_cot)
    engine.is_duplicate(fields, now=1000.0)

    assert engine.is_duplicate(fields, now=1061.0) is False
    assert engine.size == 1


def test_differing_text_is_not_a_duplicate() -> None:
    engine = DeduplicationEngine()
    first = {F_UID: "UID-01", F_TIMESTAMP: 1000, F_TEXT: "contact front"}
    second = {F_UID: "UID-01", F_TIMESTAMP: 1000, F_TEXT: "contact rear"}

    assert engine.is_duplicate(first, now=1000.0) is False
    assert engine.is_duplicate(second, now=1000.0) is False


def test_same_text_from_a_different_sender_is_not_a_duplicate() -> None:
    engine = DeduplicationEngine()
    fields = {F_UID: "UID-01", F_TIMESTAMP: 1000, F_TEXT: "rally point alpha"}

    assert engine.is_duplicate(fields, now=1000.0) is False
    assert engine.is_duplicate({**fields, F_UID: "UID-02"}, now=1000.0) is False


def test_message_hash_is_stable_and_sha256_shaped() -> None:
    digest = message_hash("UID-01", 1000, "rally point alpha")

    assert digest == message_hash("UID-01", 1000.9, "rally point alpha")
    assert len(digest) == 64


def test_tag_formats_the_geochat_uid(geochat_cot: str) -> None:
    engine = DeduplicationEngine()
    fields = xml_to_lxmf_fields(geochat_cot)
    tag = engine.tag(fields)

    head, digest = tag.rsplit(".", 1)
    assert head == f"GeoChat.{fields[F_UID]}"
    assert len(digest) == 64
    assert tag == geochat_uid(str(fields[F_UID]), digest)


def test_expired_entries_are_evicted_from_the_window() -> None:
    engine = DeduplicationEngine(ttl_seconds=60)
    for index in range(5):
        engine.is_duplicate({F_UID: f"UID-{index}", F_TIMESTAMP: 1000 + index}, now=1000.0 + index)

    assert engine.size == 5
    engine.is_duplicate({F_UID: "UID-late", F_TIMESTAMP: 2000}, now=1065.0)
    assert engine.size == 1
