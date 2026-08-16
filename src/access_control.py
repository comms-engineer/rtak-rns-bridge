"""RNS identity based access control.

Reticulum identities already provide the authentication primitive, so peers are
authorised by public identity hash rather than by TLS client certificates.
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class PeerRole(str, Enum):
    """The privilege class an identity hash is authorised for."""

    SIBLING = "sibling"
    EDGE_NODE = "edge_node"


def normalise_hash(identity_hash: str | bytes) -> str:
    """Return the lowercase hex form of an identity hash."""
    if isinstance(identity_hash, bytes):
        return identity_hash.hex()
    return identity_hash.strip().lower().removeprefix("0x")


class AccessController:
    """Checks identities against the configured allow lists."""

    def __init__(
        self,
        authorized_siblings: list[str] | None = None,
        authorized_edge_nodes: list[str] | None = None,
        require_signatures: bool = True,
    ) -> None:
        self.require_signatures = require_signatures
        self._siblings = {normalise_hash(item) for item in authorized_siblings or []}
        self._edge_nodes = {normalise_hash(item) for item in authorized_edge_nodes or []}

    @classmethod
    def from_file(cls, path: str | Path) -> AccessController:
        """Load allow lists from config/access_control.json."""
        with open(path, encoding="utf-8") as handle:
            config: dict[str, Any] = json.load(handle)
        return cls(
            authorized_siblings=config.get("authorized_siblings", []),
            authorized_edge_nodes=config.get("authorized_edge_nodes", []),
            require_signatures=bool(config.get("require_signatures", True)),
        )

    def is_authorized(self, identity_hash: str | bytes, role: PeerRole) -> bool:
        """Whether an identity may act in the given role."""
        if not self.require_signatures:
            return True
        candidate = normalise_hash(identity_hash)
        allowed = self._siblings if role is PeerRole.SIBLING else self._edge_nodes
        return candidate in allowed

    def role_of(self, identity_hash: str | bytes) -> PeerRole | None:
        """The role an identity is authorised for, or None when unknown."""
        candidate = normalise_hash(identity_hash)
        if candidate in self._siblings:
            return PeerRole.SIBLING
        if candidate in self._edge_nodes:
            return PeerRole.EDGE_NODE
        return None

    def authorize_link(self, link: Any, role: PeerRole) -> bool:
        """Validate an RNS Link's remote identity, tearing it down when unauthorised.

        Links without a proven remote identity are rejected whenever signatures are
        required, since an unidentified peer cannot be matched against an allow list.
        """
        identity = getattr(link, "get_remote_identity", lambda: None)()
        if identity is None:
            if self.require_signatures:
                logger.warning("tearing down link with no remote identity")
                self._teardown(link)
                return False
            return True

        identity_hash = getattr(identity, "hash", b"")
        if not self.is_authorized(identity_hash, role):
            logger.warning(
                "tearing down link from unauthorised %s %s",
                role.value,
                normalise_hash(identity_hash),
            )
            self._teardown(link)
            return False
        return True

    def authorize_envelope(self, source_hash: str | bytes, role: PeerRole) -> bool:
        """Validate the source identity hash of an inbound LXMF envelope."""
        if self.is_authorized(source_hash, role):
            return True
        logger.warning(
            "discarding LXMF envelope from unauthorised %s %s",
            role.value,
            normalise_hash(source_hash),
        )
        return False

    @staticmethod
    def _teardown(link: Any) -> None:
        teardown = getattr(link, "teardown", None)
        if callable(teardown):
            teardown()
