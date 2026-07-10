"""
canfar_utils.py
---------------
Shared helpers for CANFAR session creation in the ALMA-SAILS Prefect flows.

The make_session() helper is the single place that controls which CANFAR
cluster a session is submitted to. Pass use_staging=True to target the
staging cluster, False for production; the Python Configuration API is used
to override the active context's server name and URL in-memory — no CLI
calls, no on-disk config changes. Both branches set the server explicitly:
use_staging=False must not fall back to whatever the ambient active context
happens to be, since that can be staging (e.g. a context literally named
"default" configured with the staging URL) -- see state_transitions/run notes
around 2026-06-26 for the incident where this silently sent a use_staging=False
run to staging.
"""

from __future__ import annotations

from canfar.models.config import Configuration
from canfar.sessions import Session

_STAGING_CONTEXT_NAME    = "CADC-CANFAR-STAGING"
_STAGING_SERVER_URL      = "https://staging.canfar.net/skaha"
_PRODUCTION_CONTEXT_NAME = "CADC-CANFAR"


def make_session(use_staging: bool = False) -> Session:
    """Return a configured CANFAR Session pointing to production or staging.

    Parameters
    ----------
    use_staging:
        If True, targets the CANFAR staging cluster. If False (default),
        explicitly targets production -- never inferred from whichever
        context happens to be active in the user's canfar config.
    """
    cfg = Configuration()
    active = cfg.active  # e.g. "CADC-CANFAR" or "default" — read from the user's config file

    if use_staging:
        server_name, server_url = _STAGING_CONTEXT_NAME, _STAGING_SERVER_URL
    else:
        production = cfg.contexts[_PRODUCTION_CONTEXT_NAME]
        server_name, server_url = production.server.name, production.server.url

    cfg = cfg.set_value(f"contexts.{active}.server.name", server_name)
    cfg = cfg.set_value(f"contexts.{active}.server.url",  server_url)
    return Session(config=cfg)
