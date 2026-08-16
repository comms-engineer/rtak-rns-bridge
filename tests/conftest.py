"""Shared CoT fixtures for the bridge test suite."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Protocol

import pytest

# Fixture events are stamped relative to the current clock: the bridge filters tracks
# against a 24 hour catch-up window, so a hard-coded date would silently age out.
EVENT_EPOCH = int(time.time())


def cot_time(epoch: int) -> str:
    """Render epoch seconds as a CoT timestamp attribute."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ] + "Z"


POSITION_XML = (
    '<event version="2.0" uid="ANDROID-352cba1f1234" type="a-f-G-U-C" how="m-g" '
    f'time="{cot_time(EVENT_EPOCH)}" start="{cot_time(EVENT_EPOCH)}" '
    f'stale="{cot_time(EVENT_EPOCH + 300)}">'
    '<point lat="51.4780000" lon="-0.0014000" hae="42.5" ce="9.5" le="9.5"/>'
    "<detail>"
    '<contact callsign="PHANTOM-01" endpoint="*:-1:stcp"/>'
    '<__group name="Cyan" role="Team Member"/>'
    '<takv device="PIXEL 7" platform="ATAK-CIV" os="34" version="5.1.0"/>'
    '<status battery="88"/>'
    f'<_flow-tags_ TAK-Server="{cot_time(EVENT_EPOCH)}"/>'
    "</detail>"
    "</event>"
)

GEOCHAT_XML = (
    '<event version="2.0" uid="GeoChat.ANDROID-352cba1f1234.Team Cyan.abc123" type="b-t-f" '
    f'how="h-g-i-g-o" time="{cot_time(EVENT_EPOCH + 60)}" start="{cot_time(EVENT_EPOCH + 60)}" '
    f'stale="{cot_time(EVENT_EPOCH + 360)}">'
    '<point lat="51.4780000" lon="-0.0014000" hae="42.5" ce="9.5" le="9.5"/>'
    "<detail>"
    '<__chat parent="RootContactGroup" groupOwner="false" chatroom="Team Cyan" '
    'id="Team Cyan" senderCallsign="PHANTOM-01"/>'
    '<link uid="ANDROID-352cba1f1234" type="a-f-G-U-C" relation="p-p"/>'
    "<remarks source=\"BAO.F.ATAK.ANDROID-352cba1f1234\">Moving to checkpoint bravo</remarks>"
    "</detail>"
    "</event>"
)


def position_xml(uid: str, lat: float, lon: float, timestamp: str) -> str:
    """A minimal position report for a given UID, coordinate and CoT timestamp."""
    return (
        f'<event version="2.0" uid="{uid}" type="a-f-G-U-C" how="m-g" '
        f'time="{timestamp}" start="{timestamp}" stale="{timestamp}">'
        f'<point lat="{lat:.7f}" lon="{lon:.7f}" hae="10.0" ce="9.5" le="9.5"/>'
        '<detail><contact callsign="PHANTOM-01"/></detail>'
        "</event>"
    )


class PositionFactory(Protocol):
    """Builds a position report for a given UID, coordinate and CoT timestamp."""

    def __call__(self, uid: str, lat: float, lon: float, timestamp: str) -> str: ...


@pytest.fixture
def event_epoch() -> int:
    """Epoch seconds carried by the position fixture."""
    return EVENT_EPOCH


@pytest.fixture
def make_position() -> PositionFactory:
    """Factory for minimal position reports."""
    return position_xml


@pytest.fixture
def position_cot() -> str:
    """A full ATAK self-position event including tags the bridge must strip."""
    return POSITION_XML


@pytest.fixture
def geochat_cot() -> str:
    """A GeoChat event addressed to the 'Team Cyan' chat room."""
    return GEOCHAT_XML
