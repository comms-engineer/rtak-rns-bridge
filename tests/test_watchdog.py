"""Watchdog health probe and recovery tests."""

from __future__ import annotations

import socket
from typing import Any

import pytest

from src.watchdog import Watchdog, probe_listener


class StubTransport:
    def __init__(self) -> None:
        self.starts = 0
        self.stops = 0
        self.online = False
        self.sent: list[Any] = []

    def start(self) -> None:
        self.starts += 1
        self.online = True

    def stop(self) -> None:
        self.stops += 1
        self.online = False

    def send(self, destination_hash: bytes, fields: dict[int, Any]) -> bool:
        self.sent.append(fields)
        return True


class StubBridge:
    """Only the surface the watchdog touches."""

    def __init__(self, address: tuple[str, int]) -> None:
        self.listen_address = address
        self.transport = StubTransport()
        self.loop = None
        self.server = None
        self.flushed = 0

        class _Queue:
            online = True

            def set_online(self, online: bool) -> None:
                self.online = online

        self.queue = _Queue()

    def flush_queue(self) -> int:
        self.flushed += 1
        return 0


def test_probe_listener_detects_a_bound_socket() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()

    assert probe_listener(host, port) is True
    listener.close()
    assert probe_listener(host, port) is False


def test_probe_listener_maps_wildcard_binds_to_loopback() -> None:
    listener = socket.socket()
    listener.bind(("0.0.0.0", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    assert probe_listener("0.0.0.0", port) is True
    listener.close()


def test_check_once_reinitialises_a_dead_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    bridge = StubBridge(listener.getsockname())
    watchdog = Watchdog(bridge)  # type: ignore[arg-type]
    monkeypatch.setattr("src.watchdog.probe_transport", lambda: False)

    assert watchdog.check_once() is False
    assert watchdog.transport_restarts == 1
    assert bridge.transport.starts == 1
    assert bridge.flushed == 1
    listener.close()


def test_check_once_is_quiet_when_everything_is_healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    bridge = StubBridge(listener.getsockname())
    watchdog = Watchdog(bridge)  # type: ignore[arg-type]
    monkeypatch.setattr("src.watchdog.probe_transport", lambda: True)

    assert watchdog.check_once() is True
    assert watchdog.transport_restarts == 0
    assert watchdog.listener_restarts == 0
    listener.close()


def test_listener_restart_is_skipped_without_a_running_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = StubBridge(("127.0.0.1", 1))
    watchdog = Watchdog(bridge)  # type: ignore[arg-type]
    monkeypatch.setattr("src.watchdog.probe_transport", lambda: True)

    assert watchdog.check_once() is False
    assert watchdog.listener_restarts == 0
