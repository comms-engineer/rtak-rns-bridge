"""Edge bridge: local CoT TCP listener that emits LXMF over Reticulum.

ATAK on the same handset connects to 127.0.0.1:8087; iTAK over edge Wi-Fi connects to
0.0.0.0:8087. Ingested XML is throttled, deduplicated, compacted and pushed out as LXMF.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import ParseError

from src.access_control import AccessController
from src.cot_converter import (
    F_TEXT,
    F_UID,
    lxmf_fields_to_xml,
    pack_fields,
    strip_redundant,
    unpack_fields,
    xml_to_lxmf_fields,
)
from src.dedup import DeduplicationEngine
from src.group_resolver import GroupResolutionError, GroupResolver
from src.queue_manager import QueueManager
from src.state_db import StateDB

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
DEFAULT_CONFIG = CONFIG_DIR / "config.json"
FLUSH_INTERVAL_SECONDS = 2.0
EVENT_TERMINATOR = b"</event>"


def load_config(path: str | Path = DEFAULT_CONFIG) -> dict[str, Any]:
    """Read the bridge configuration document."""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def split_events(buffer: bytes) -> tuple[list[str], bytes]:
    """Split a TCP read buffer into complete CoT events plus the trailing remainder."""
    events: list[str] = []
    while True:
        end = buffer.find(EVENT_TERMINATOR)
        if end == -1:
            return events, buffer
        cutoff = end + len(EVENT_TERMINATOR)
        chunk = buffer[:cutoff]
        buffer = buffer[cutoff:]
        start = chunk.find(b"<event")
        if start != -1:
            events.append(chunk[start:cutoff].decode("utf-8", errors="replace"))


class LXMFTransport:
    """Thin wrapper over LXMF.LXMRouter so the bridge stays testable without radios.

    Reticulum is imported lazily: the translation pipeline and its tests must run on
    hosts with no RNS installation or interfaces.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.router: Any = None
        self.identity: Any = None
        self.source: Any = None
        self.online = False

    def start(self) -> None:
        """Bring up Reticulum, load the node identity and register the LXMF router."""
        import LXMF  # noqa: PLC0415 -- optional radio dependency
        import RNS  # noqa: PLC0415

        reticulum = self.config.get("reticulum", {})
        RNS.Reticulum(configdir=reticulum.get("config_dir"))
        keyfile = reticulum.get("identity_keyfile")
        if keyfile and Path(keyfile).exists():
            self.identity = RNS.Identity.from_file(keyfile)
        else:
            self.identity = RNS.Identity()
            if keyfile:
                Path(keyfile).parent.mkdir(parents=True, exist_ok=True)
                self.identity.to_file(keyfile)
        self.router = LXMF.LXMRouter(identity=self.identity, storagepath=".lxmf")
        self.source = self.router.register_delivery_identity(
            self.identity, display_name=self.config.get("callsign", "rtak-bridge")
        )
        self.online = True
        logger.info("LXMF transport online as %s", RNS.prettyhexrep(self.identity.hash))

    def send(self, destination_hash: bytes, fields: dict[int, Any]) -> bool:
        """Hand a compact payload to LXMF for delivery. False when the link is down."""
        if not self.online or self.router is None:
            return False
        import LXMF  # noqa: PLC0415
        import RNS  # noqa: PLC0415

        identity = RNS.Identity.recall(destination_hash)
        if identity is None:
            RNS.Transport.request_path(destination_hash)
            logger.info("no path yet for %s, requested", destination_hash.hex())
            return False
        destination = RNS.Destination(
            identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "lxmf",
            "delivery",
        )
        message = LXMF.LXMessage(
            destination,
            self.source,
            content=pack_fields(fields),
            desired_method=LXMF.LXMessage.OPPORTUNISTIC,
        )
        message.fields = fields
        self.router.handle_outbound(message)
        return True

    def stop(self) -> None:
        """Release the LXMF router."""
        self.online = False
        if self.router is not None:
            self.router.exit_handler()
            self.router = None


class EdgeBridge:
    """Wires the TCP listener, translation pipeline and LXMF transport together."""

    def __init__(
        self,
        config: dict[str, Any],
        transport: LXMFTransport | None = None,
        db: StateDB | None = None,
        resolver: GroupResolver | None = None,
        access: AccessController | None = None,
    ) -> None:
        self.config = config
        telemetry = config.get("telemetry_filter", {})
        self.queue = QueueManager(
            min_distance_meters=float(telemetry.get("min_distance_meters", 30.0)),
            max_heartbeat_seconds=float(telemetry.get("max_heartbeat_seconds", 300)),
            purge_stale_queue_on_reconnect=bool(
                telemetry.get("purge_stale_queue_on_reconnect", True)
            ),
        )
        self.dedup = DeduplicationEngine()
        self.db = db or StateDB(config.get("database", {}).get("sqlite_path", "state.db"))
        self.resolver = resolver or GroupResolver.from_file(CONFIG_DIR / "groups.json")
        self.access = access or AccessController.from_file(CONFIG_DIR / "access_control.json")
        self.transport = transport if transport is not None else LXMFTransport(config)
        self.default_destination: bytes | None = self._default_destination()
        self.server: asyncio.AbstractServer | None = None
        self.clients: set[asyncio.StreamWriter] = set()
        self.loop: asyncio.AbstractEventLoop | None = None

    def _default_destination(self) -> bytes | None:
        target = self.config.get("default_group")
        if not target:
            return None
        return self.resolver.resolve_optional(str(target))

    @property
    def listen_address(self) -> tuple[str, int]:
        """Configured TCP listener host and port."""
        listener = self.config.get("tcp_listener", {})
        return str(listener.get("host", "0.0.0.0")), int(listener.get("port", 8087))

    def ingest(self, xml_str: str, now: float | None = None) -> dict[int, Any] | None:
        """Run one CoT event through the pipeline, returning the payload that was sent."""
        now = time.time() if now is None else now
        try:
            fields = xml_to_lxmf_fields(strip_redundant(xml_str))
        except (ParseError, ValueError):
            logger.debug("discarding malformed CoT event")
            return None

        if self.dedup.is_duplicate(fields, now=now):
            logger.debug("dropping duplicate event %s", fields.get(F_UID))
            return None

        destination = self.default_destination
        if fields.get(F_TEXT):
            try:
                destination, fields = self.resolver.wrap_geochat(xml_str)
            except GroupResolutionError as exc:
                logger.warning("unroutable GeoChat: %s", exc)
                return None
            self.db.record_chat(self.dedup.tag(fields), fields)
        else:
            if not self.queue.submit(fields, now=now):
                logger.debug("throttled position for %s", fields.get(F_UID))
                return None
            self.db.upsert_track(fields, now=now)

        if destination is None:
            logger.debug("no destination for %s, holding in queue", fields.get(F_UID))
            return None
        if not self.transport.send(destination, fields):
            self.queue.set_online(False)
            return None
        return fields

    def flush_queue(self, now: float | None = None) -> int:
        """Transmit everything the queue has been holding. Returns the count sent."""
        if self.default_destination is None:
            return 0
        sent = 0
        for fields in self.queue.reconnect(now=now):
            if self.transport.send(self.default_destination, fields):
                sent += 1
        return sent

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Serve one TAK client: replay current state, then ingest its event stream."""
        peer = writer.get_extra_info("peername")
        logger.info("TAK client connected: %s", peer)
        self.clients.add(writer)
        try:
            sock = writer.get_extra_info("socket")
            if sock is not None:
                await asyncio.get_running_loop().run_in_executor(
                    None, self.db.dump_current_state_to_socket, sock
                )
            buffer = b""
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buffer += chunk
                events, buffer = split_events(buffer)
                for event in events:
                    self.ingest(event)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            logger.info("TAK client %s dropped", peer)
        finally:
            self.clients.discard(writer)
            writer.close()

    async def broadcast_xml(self, fields: dict[int, Any]) -> None:
        """Push an inbound LXMF payload back down to every connected TAK client."""
        payload = lxmf_fields_to_xml(fields).encode("utf-8") + b"\n"
        for writer in list(self.clients):
            try:
                writer.write(payload)
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                self.clients.discard(writer)

    def handle_inbound_lxmf(self, message: Any) -> None:
        """LXMF delivery callback: persist and fan the event out to local clients."""
        fields = message.fields or unpack_fields(message.content)
        fields = {int(key): value for key, value in fields.items()}
        if self.dedup.is_duplicate(fields):
            return
        self.db.apply_fields(self.dedup.tag(fields), fields)
        if self.loop is not None:
            asyncio.run_coroutine_threadsafe(self.broadcast_xml(fields), self.loop)

    async def serve(self) -> None:
        """Run the TCP listener until cancelled."""
        self.loop = asyncio.get_running_loop()
        host, port = self.listen_address
        self.server = await asyncio.start_server(self.handle_client, host, port)
        logger.info("CoT listener bound to %s:%s", host, port)
        async with self.server:
            await self.server.serve_forever()

    async def periodic_flush(self) -> None:
        """Drain the outbound queue on a fixed cadence."""
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            self.flush_queue()

    def close(self) -> None:
        """Release the database and transport."""
        self.transport.stop()
        self.db.close()


async def _run(config: dict[str, Any]) -> None:
    from src.watchdog import Watchdog  # noqa: PLC0415 -- avoids an import cycle

    bridge = EdgeBridge(config)
    bridge.transport.start()
    watchdog = Watchdog(bridge)
    watchdog.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    serve_task = asyncio.create_task(bridge.serve())
    flush_task = asyncio.create_task(bridge.periodic_flush())
    await stop.wait()
    for task in (serve_task, flush_task):
        task.cancel()
    watchdog.stop()
    bridge.close()


def main(argv: list[str] | None = None) -> int:
    """Entry point for `python -m src.edge_bridge`."""
    parser = argparse.ArgumentParser(description="RNS <-> TAK edge bridge")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="path to config.json")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run(load_config(args.config)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
