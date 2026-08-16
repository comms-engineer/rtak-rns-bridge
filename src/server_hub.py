"""Server hub daemon: RNS destination for edge nodes plus sibling state federation.

The hub owns the authoritative picture for its domain. Edge nodes deliver LXMF
envelopes to it, sibling hubs reconcile their tables with it over an RNS Link, and
every peer is checked against the identity allow lists before anything is applied.
"""

from __future__ import annotations

import argparse
import logging
import threading
from pathlib import Path
from typing import Any

import msgpack

from src.access_control import AccessController, PeerRole
from src.cot_converter import pack_fields, unpack_fields
from src.dedup import DeduplicationEngine
from src.edge_bridge import CONFIG_DIR, DEFAULT_CONFIG, load_config
from src.group_resolver import GroupResolver
from src.state_db import StateDB

logger = logging.getLogger(__name__)

APP_NAME = "rtak"
SYNC_ASPECT = "sync"
SYNC_INTERVAL_SECONDS = 300.0

OP_STATE_REQUEST = 0x10
OP_STATE_SNAPSHOT = 0x11
OP_EVENT = 0x12


class ServerHub:
    """RNS server daemon holding the shared tactical picture."""

    def __init__(
        self,
        config: dict[str, Any],
        db: StateDB | None = None,
        access: AccessController | None = None,
        resolver: GroupResolver | None = None,
    ) -> None:
        self.config = config
        self.db = db or StateDB(config.get("database", {}).get("sqlite_path", "state.db"))
        self.access = access or AccessController.from_file(CONFIG_DIR / "access_control.json")
        self.resolver = resolver or GroupResolver.from_file(CONFIG_DIR / "groups.json")
        self.dedup = DeduplicationEngine()
        self.identity: Any = None
        self.destination: Any = None
        self.links: dict[bytes, Any] = {}
        self._stop = threading.Event()

    def start(self) -> None:
        """Bring up Reticulum and announce the sync destination."""
        import RNS  # noqa: PLC0415 -- optional radio dependency

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

        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            SYNC_ASPECT,
        )
        self.destination.set_link_established_callback(self.on_link_established)
        self.destination.announce()
        logger.info("hub announced as %s", RNS.prettyhexrep(self.destination.hash))

    def on_link_established(self, link: Any) -> None:
        """Gate an inbound RNS Link on the sibling allow list."""
        link.set_link_closed_callback(self.on_link_closed)
        link.set_packet_callback(self.on_packet)
        link.set_remote_identified_callback(self.on_remote_identified)
        logger.info("link established, awaiting identification")

    def on_remote_identified(self, link: Any, identity: Any) -> None:
        """Authorise the peer once it proves its identity, then exchange state."""
        if not self.access.authorize_link(link, PeerRole.SIBLING):
            return
        self.links[identity.hash] = link
        self.send_snapshot(link)

    def on_link_closed(self, link: Any) -> None:
        """Forget a peer whose link went away."""
        for identity_hash, known in list(self.links.items()):
            if known is link:
                del self.links[identity_hash]

    def on_packet(self, message: bytes, packet: Any) -> None:
        """Handle a sync frame from an authorised sibling."""
        link = getattr(packet, "link", None)
        identity = getattr(link, "get_remote_identity", lambda: None)() if link else None
        if identity is None or not self.access.is_authorized(identity.hash, PeerRole.SIBLING):
            logger.warning("dropping sync frame from unauthenticated peer")
            return
        if not message:
            return
        opcode, body = message[0], message[1:]
        if opcode == OP_STATE_REQUEST:
            self.send_snapshot(link)
        elif opcode == OP_STATE_SNAPSHOT:
            snapshot = msgpack.unpackb(body, raw=False, strict_map_key=False)
            applied = self.db.import_state(snapshot)
            logger.info("merged %d rows from sibling", applied)
        elif opcode == OP_EVENT:
            fields = {int(key): value for key, value in unpack_fields(body).items()}
            if not self.dedup.is_duplicate(fields):
                self.db.apply_fields(self.dedup.tag(fields), fields)
        else:
            logger.debug("ignoring unknown sync opcode 0x%02x", opcode)

    def send_snapshot(self, link: Any) -> None:
        """Send the current picture to a sibling over an established link."""
        import RNS  # noqa: PLC0415

        payload = bytes([OP_STATE_SNAPSHOT]) + msgpack.packb(
            self.db.export_state(), use_bin_type=True
        )
        RNS.Packet(link, payload).send()

    def handle_edge_envelope(self, source_hash: bytes, payload: bytes) -> bool:
        """Apply an LXMF envelope from an edge node. False when it was rejected."""
        if not self.access.authorize_envelope(source_hash, PeerRole.EDGE_NODE):
            return False
        fields = {int(key): value for key, value in unpack_fields(payload).items()}
        if self.dedup.is_duplicate(fields):
            return False
        self.db.apply_fields(self.dedup.tag(fields), fields)
        return True

    def broadcast_to_siblings(self, fields: dict[int, Any]) -> int:
        """Relay one event to every connected sibling. Returns the number reached."""
        import RNS  # noqa: PLC0415

        payload = bytes([OP_EVENT]) + pack_fields(fields)
        sent = 0
        for link in list(self.links.values()):
            RNS.Packet(link, payload).send()
            sent += 1
        return sent

    def run(self) -> None:
        """Announce periodically and prune stale state until stopped."""
        while not self._stop.is_set():
            self._stop.wait(SYNC_INTERVAL_SECONDS)
            if self._stop.is_set():
                break
            if self.destination is not None:
                self.destination.announce()
            removed = self.db.prune()
            if removed:
                logger.info("pruned %d stale rows", removed)

    def stop(self) -> None:
        """Signal the daemon loop to exit and release the database."""
        self._stop.set()
        self.db.close()


def main(argv: list[str] | None = None) -> int:
    """Entry point for `python -m src.server_hub`."""
    parser = argparse.ArgumentParser(description="RNS <-> TAK server hub")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="path to config.json")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    hub = ServerHub(load_config(args.config))
    hub.start()
    try:
        hub.run()
    except KeyboardInterrupt:
        pass
    finally:
        hub.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
