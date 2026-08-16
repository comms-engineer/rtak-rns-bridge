"""Process watchdog for the socket listener and the Reticulum transport.

Android background memory pressure and radio interface stalls both surface as a silently
dead listener or a starved RNS thread, which a TAK client cannot distinguish from an
empty picture. A daemon thread probes both every few seconds and re-initialises them.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.edge_bridge import EdgeBridge

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 10.0
PROBE_TIMEOUT_SECONDS = 2.0


def probe_listener(host: str, port: int, timeout: float = PROBE_TIMEOUT_SECONDS) -> bool:
    """Whether a TCP connection to the listener can be established."""
    probe_host = "127.0.0.1" if host in ("0.0.0.0", "::", "") else host
    try:
        with socket.create_connection((probe_host, port), timeout=timeout):
            return True
    except OSError:
        return False


def probe_transport() -> bool:
    """Whether the Reticulum transport is instantiated and running."""
    try:
        import RNS  # noqa: PLC0415 -- optional radio dependency
    except ImportError:
        return False
    reticulum: Any = getattr(RNS.Reticulum, "get_instance", lambda: None)()
    if reticulum is None:
        return False
    return bool(getattr(RNS.Transport, "identity", None) is not None)


class Watchdog:
    """Monitors bridge health from a daemon thread and repairs what it can."""

    def __init__(
        self, bridge: EdgeBridge, check_interval: float = CHECK_INTERVAL_SECONDS
    ) -> None:
        self.bridge = bridge
        self.check_interval = check_interval
        self.listener_restarts = 0
        self.transport_restarts = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Spawn the monitoring thread."""
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="rtak-watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Ask the monitoring thread to exit."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.check_interval)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.wait(self.check_interval):
            try:
                self.check_once()
            except Exception:  # noqa: BLE001 -- a watchdog must never die
                logger.exception("watchdog iteration failed")

    def check_once(self) -> bool:
        """Run one health check pass. Returns True when everything looked healthy."""
        healthy = True
        host, port = self.bridge.listen_address
        if not probe_listener(host, port):
            logger.warning("CoT listener on %s:%s is not accepting connections", host, port)
            self.restart_listener()
            healthy = False
        if not probe_transport():
            logger.warning("Reticulum transport is down")
            self.restart_transport()
            healthy = False
        return healthy

    def restart_listener(self) -> None:
        """Rebind the asyncio TCP listener on the bridge's own event loop."""
        loop = self.bridge.loop
        if loop is None or loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(self._rebind(), loop)
        self.listener_restarts += 1

    async def _rebind(self) -> None:
        server = self.bridge.server
        if server is not None:
            server.close()
            await server.wait_closed()
        self.bridge.server = None
        asyncio.create_task(self.bridge.serve())

    def restart_transport(self) -> None:
        """Re-initialise Reticulum and flush whatever the queue held while it was down."""
        self.bridge.queue.set_online(False)
        try:
            self.bridge.transport.stop()
            self.bridge.transport.start()
        except Exception:  # noqa: BLE001 -- retried on the next pass
            logger.exception("failed to re-initialise the LXMF transport")
            return
        self.transport_restarts += 1
        self.bridge.flush_queue()
