"""Shared CoT fixtures for the bridge test suite."""

from __future__ import annotations

import pytest

POSITION_XML = (
    '<event version="2.0" uid="ANDROID-352cba1f1234" type="a-f-G-U-C" how="m-g" '
    'time="2026-08-10T12:00:00.000Z" start="2026-08-10T12:00:00.000Z" '
    'stale="2026-08-10T12:05:00.000Z">'
    '<point lat="51.4780000" lon="-0.0014000" hae="42.5" ce="9.5" le="9.5"/>'
    "<detail>"
    '<contact callsign="PHANTOM-01" endpoint="*:-1:stcp"/>'
    '<__group name="Cyan" role="Team Member"/>'
    '<takv device="PIXEL 7" platform="ATAK-CIV" os="34" version="5.1.0"/>'
    '<status battery="88"/>'
    "<_flow-tags_ TAK-Server=\"2026-08-10T12:00:00Z\"/>"
    "</detail>"
    "</event>"
)

GEOCHAT_XML = (
    '<event version="2.0" uid="GeoChat.ANDROID-352cba1f1234.Team Cyan.abc123" type="b-t-f" '
    'how="h-g-i-g-o" time="2026-08-10T12:01:00.000Z" start="2026-08-10T12:01:00.000Z" '
    'stale="2026-08-10T12:06:00.000Z">'
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


@pytest.fixture
def position_cot() -> str:
    """A full ATAK self-position event including tags the bridge must strip."""
    return POSITION_XML


@pytest.fixture
def geochat_cot() -> str:
    """A GeoChat event addressed to the 'Team Cyan' chat room."""
    return GEOCHAT_XML
