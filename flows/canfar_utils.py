"""
canfar_utils.py
---------------
Shared helpers for CANFAR session creation in the ALMA-SAILS Prefect flows.

The make_session() helper is the single place that controls which CANFAR
cluster a session is submitted to. Pass use_staging=True to target the
staging cluster; the Python Configuration API is used to override the
server name and URL in-memory — no CLI calls, no on-disk config changes.
"""

from __future__ import annotations

from canfar.models.config import Configuration
from canfar.sessions import Session

_STAGING_SERVER_NAME = "CADC-CANFAR-STAGING"
_STAGING_SERVER_URL  = "https://staging.canfar.net/skaha"


def make_session(use_staging: bool = False) -> Session:
    """Return a configured CANFAR Session pointing to production or staging.

    Parameters
    ----------
    use_staging:
        If True, targets the CANFAR staging cluster. If False (default),
        uses whatever server is configured in the active canfar context.
    """
    if not use_staging:
        return Session()

    cfg = Configuration()
    active = cfg.active  # e.g. "CADC-CANFAR" — read from the user's config file
    cfg = cfg.set_value(f"contexts.{active}.server.name", _STAGING_SERVER_NAME)
    cfg = cfg.set_value(f"contexts.{active}.server.url",  _STAGING_SERVER_URL)
    return Session(config=cfg)
