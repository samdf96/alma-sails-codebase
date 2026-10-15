"""
canfar_utils.py
---------------
Shared helpers for CANFAR session creation, used by every Prefect flow and by
the project-root batch_*.py launchers.

Three concerns live here, all of which must agree across every launcher:
  - which cluster a session goes to      (make_session)
  - what a session is named              (key_* builders, job_name)
  - whether a session actually launched  (create_checked)

The naming and launch guards are only effective if all callers share them --
a flow that builds its own name cannot see a batch script's running session,
which is the concurrency hole they exist to close.

The make_session() helper is the single place that decides which CANFAR
server a session is submitted to. It uses the active selection of the user's
canfar configuration -- or, where no ~/.canfar/config.yaml exists (e.g. inside
a CANFAR interactive session), the client's built-in X.509 default -- and
refuses to proceed unless that resolves to the production server URL. It never
looks anything up by name: context and server names are not stable. A config
written by `canfar auth login` on one machine has them where a machine with no
config file does not, and the client itself renamed its default server from
"CADC-CANFAR" (<= 1.3) to "canfar" (1.4). The URL is what requests go to.
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import NamedTuple, Optional

from canfar.models.config import Configuration
from canfar.sessions import Session

# The client's default is 30s, and it is a read timeout on a request the server
# has already accepted -- nothing about it reaches CANFAR or cancels the work.
# Sessions routinely land anyway and the caller is left reconciling something
# that already happened. 300 (the client's own cap) buys enough patience to
# actually hear the answer; it does not make the server faster.
HTTP_TIMEOUT_S = 300

_PRODUCTION_SERVER_URL = "https://ws-uv.canfar.net/skaha"


def make_session(timeout: int = HTTP_TIMEOUT_S) -> Session:
    """Return a CANFAR Session on the production server.

    Parameters
    ----------
    timeout:
        HTTP timeout in seconds. Must be set here, at construction: the httpx
        client is built lazily and then cached, so assigning session.timeout
        after the first request reads back as the new value while the live
        client silently keeps the old one.

    Raises
    ------
    RuntimeError
        If the active selection resolves anywhere other than production.
        Checked rather than assumed: on 2026-06-26 an ambient context silently
        sent a run to a different cluster.
    """
    cfg = Configuration()
    # .context is the one accessor common to the 1.1-1.3 and 1.4 config models
    # (1.4 keeps it as a legacy view over its separate authentication/servers).
    server = cfg.context.server
    url = str(getattr(server, "url", None) or "").rstrip("/")
    if url != _PRODUCTION_SERVER_URL:
        raise RuntimeError(
            f"The active canfar configuration points at {url or 'no server'} "
            f"(server name {getattr(server, 'name', None)!r}), not production "
            f"({_PRODUCTION_SERVER_URL}). Select the production server in the "
            f"canfar client before launching."
        )
    return Session(config=cfg, timeout=timeout)


# ---------------------------------------------------------------------------
# Job naming
# ---------------------------------------------------------------------------
#
# A session name is  "<unit_key>-<timestamp>", where <unit_key> identifies the
# work unit the job operates on. The timestamp is NOT the uniqueness mechanism
# -- it only distinguishes sequential runs of the same unit for a human reading
# `canfar ps`. Uniqueness comes from unit_key carrying every parameter that
# changes the output product (robust value included). Minute granularity is
# therefore sufficient: a collision needs the same unit launched twice inside
# 60 seconds, which is exactly what preflight_check() prevents.
#
# unit_key doubles as the preflight/reconcile lookup key, so it must be a
# stable prefix of the name. Anything appended for readability alone would
# break prefix matching -- don't.
#
# Flows and batch scripts MUST build identical unit_keys for the same work
# unit, or a Prefect launch will not see a batch-launched session (and vice
# versa). That is why the key builders live here rather than at each call site.

JOB_TS_FMT = "%Y%m%d-%H%M"

# Session states that mean "this job still owns its work unit". CANFAR keeps
# terminal sessions listed for roughly an hour (variable) before clearing them;
# treating those as occupying would make a finished unit un-relaunchable for
# that hour, which would break every --force reprocessing run.
OCCUPYING_STATUSES = ("Pending", "Running", "Terminating")


def safe(value) -> str:
    """Reduce a value to the [a-z0-9-] alphabet used in session names.

    Lowercased because CANFAR lowercases session names server-side: a job
    submitted as 'imaging-S68Nc-b3-12m-r0-5' comes back from fetch() as
    'imaging-s68nc-...'. Building mixed-case keys made the preflight prefix
    match fail for every target with a capital letter in its name -- which is
    nearly all of them -- silently disabling the duplicate guard.
    """
    return re.sub(r"[^a-zA-Z0-9-]", "-", str(value)).lower()


def mous_tag(mous_id: str) -> str:
    """Shorten a MOUS uid to its distinguishing tail.

    'uid://A001/X2d6/X175' -> 'x2d6-x175'

    The 'uid://A001' prefix is constant across this survey, so it costs name
    budget without adding information. Long unit_keys are a practical problem:
    a full mous_id plus a source like 'Serpens_Main_and_Serpens_South' pushes
    the name past 75 characters, and the longest name known to work on this
    cluster is 50.
    """
    parts = [p for p in str(mous_id).replace("uid://", "").split("/") if p]
    tail = parts[-2:] if len(parts) >= 2 else parts
    return safe("-".join(tail)).lower()


def robust_tag(robust) -> str:
    """'r0-5' / 'r2-0'. Mirrors the rob0.5/rob2.0 product filenames."""
    return safe(f"r{float(robust):.1f}")


def job_name(unit_key: str, now=None) -> str:
    """Full session name for a work unit: '<unit_key>-<YYYYmmdd-HHMM>'."""
    stamp = (now or datetime.now()).strftime(JOB_TS_FMT)
    return f"{unit_key}-{stamp}"


def key_download(mous_id) -> str:
    return f"download-{mous_tag(mous_id)}"


def key_split(mous_id) -> str:
    return f"split-{mous_tag(mous_id)}"


def key_selfcal(mous_id) -> str:
    return f"selfcal-{mous_tag(mous_id)}"


def key_imaging(source, band, config, robust) -> str:
    return (f"imaging-{safe(source)}-b{safe(band)}-{safe(config)}-"
            f"{robust_tag(robust)}")


def key_line_imaging(detection_id, robust) -> str:
    return f"line-imaging-det{int(detection_id)}-{robust_tag(robust)}"


def key_line_triage(mous_id, source) -> str:
    """Deliberately coarser than line_triage_unit's (mous, source, spw) key.

    A spw=None run covers every spw of the (mous, source) pair, so a concurrent
    single-spw run on the same pair would collide on the output directory and
    the DB rows. Keying on the pair blocks that; keying on spw would not.
    """
    return f"line-triage-{mous_tag(mous_id)}-{safe(source)}"


def key_admit(mous_id, source, spw) -> str:
    return f"admit-{mous_tag(mous_id)}-{safe(source)}-spw{int(spw)}"


# ---------------------------------------------------------------------------
# Guarded session creation
# ---------------------------------------------------------------------------


class LaunchFailed(RuntimeError):
    """Raised only when we have positively established that nothing launched.

    Distinct from an ambiguous failure, which returns outcome='unknown' instead:
    a caller may safely retry a LaunchFailed, and must not retry an 'unknown'.

    Reaching this requires BOTH that the lookups ran and found nothing AND that
    the original failure was not transport-ambiguous -- see
    _is_ambiguous_failure. An exhausted lookup alone is not enough: on
    2026-08-30 a create failed, four lookups over 110s found nothing, and the
    session started running 49s later.
    """


# Failure modes that say nothing about whether the server acted. Matched by
# type name rather than imported: the CANFAR client may wrap or replace httpx's
# exceptions, and a hard import would couple this module to its dependencies.
_AMBIGUOUS_EXC_NAMES = frozenset({
    "ReadTimeout", "WriteTimeout", "ConnectTimeout", "PoolTimeout",
    "TimeoutException", "TimeoutError", "ConnectError", "ReadError",
    "WriteError", "RemoteProtocolError", "NetworkError",
})


def _is_ambiguous_failure(exc) -> bool:
    """Whether a create failure leaves the outcome genuinely unknown."""
    if exc is None:
        # session.create() returned no ids without raising. The client logs its
        # own "The read operation timed out" and returns empty in exactly this
        # case, so the timeout information is gone before we see it and this
        # cannot be told apart from a real rejection. Assume the worse case.
        return True
    if type(exc).__name__ in _AMBIGUOUS_EXC_NAMES:
        return True
    return "timed out" in str(exc).lower() or "timeout" in type(exc).__name__.lower()


class LaunchResult(NamedTuple):
    ids: list
    outcome: str          # "created" | "adopted" | "unknown"
    name: str
    detail: str = ""


def live_sessions(session, unit_key: str) -> list:
    """Sessions for `unit_key` that still occupy it (Pending/Running/Terminating).

    Matching is on the "<unit_key>-" prefix, so the trailing separator matters:
    without it 'EC-53' would also match a session for 'EC-53b'. Comparison is
    case-insensitive -- CANFAR lowercases names server-side.
    """
    # Case-insensitive: safe() already lowercases what we build, but names
    # created before that change, or by anything else, must still match.
    prefix = f"{unit_key}-".lower()
    found = []
    for s in session.fetch(kind="headless"):
        if not str(s.get("name", "")).lower().startswith(prefix):
            continue
        if s.get("status") in OCCUPYING_STATUSES:
            found.append(s)
    return found


def create_checked(session, unit_key: str, name: Optional[str] = None,
                   preflight: bool = True, poll_delays=(5, 15, 30, 60),
                   **create_kwargs) -> LaunchResult:
    """session.create() that survives an ambiguous response.

    The CANFAR client raises ReadTimeout with some regularity, and the session
    is frequently created anyway -- the HTTP response was lost, not the work.
    Treating that as a failure is what allowed concurrent sessions onto the same
    work unit: the caller reports failure, the status report still lists the
    unit as outstanding, and the next launch (or Prefect's own retry) starts a
    second job writing the same image base.

    Parameters
    ----------
    session:
        A Session from make_session().
    unit_key:
        Work-unit key from one of the key_* builders. Used both as the lookup
        prefix and, unless `name` is given, as the stem of the session name.
    name:
        Session name. Defaults to job_name(unit_key).
    preflight:
        Check for an already-running session on this unit before creating.
        Leave enabled even for forced reruns -- `force` governs overwriting
        products, which is a different question from running two sessions at
        once, and wiring the two together would drop the guard exactly where
        reprocessing needs it most.
    poll_delays:
        Seconds to wait before each reconcile lookup, cumulative ~110s. The
        session list is normally populated within a second or two, but on
        2026-08-29 a create timed out, the session did land, and it was still
        absent from the list 20s later -- the cluster was holding two
        long-running jobs at the time. Registration lag scales with load, and
        load is what causes the timeouts in the first place, so the window is
        sized for the bad case rather than the normal one.

    Returns
    -------
    LaunchResult
        outcome 'created' for a normal launch, 'adopted' when a live session for
        this unit already existed (or was found after an ambiguous failure), and
        'unknown' when we could not establish either way. 'unknown' must be
        treated as launched: a false "it launched" costs one skipped unit that
        the next status report catches, while a false "it didn't" costs a
        corrupting concurrent run.

    Raises
    ------
    LaunchFailed
        Only when the reconcile lookups themselves succeeded and positively
        showed no session -- i.e. it is safe to try again.
    """
    name = name or job_name(unit_key)

    if preflight:
        try:
            existing = live_sessions(session, unit_key)
        except Exception as exc:
            existing = []
            print(f"  [WARNING] preflight lookup failed for {unit_key} "
                  f"({type(exc).__name__}: {exc}); creating anyway", flush=True)
        if existing:
            ids = [s.get("id") for s in existing]
            return LaunchResult(
                ids, "adopted", str(existing[0].get("name", name)),
                f"{len(existing)} session(s) already running on this unit")

    create_exc = None
    try:
        ids = session.create(name=name, **create_kwargs)
        if ids:
            return LaunchResult(list(ids), "created", name)
        create_error = "CANFAR returned no job ID"
    except Exception as exc:
        create_exc = exc
        create_error = f"{type(exc).__name__}: {exc}"

    lookup_failed = None
    waited = 0
    for delay in poll_delays:
        time.sleep(delay)
        waited += delay
        try:
            existing = live_sessions(session, unit_key)
        except Exception as exc:
            lookup_failed = f"{type(exc).__name__}: {exc}"
            continue
        lookup_failed = None
        if existing:
            ids = [s.get("id") for s in existing]
            return LaunchResult(
                ids, "adopted", str(existing[0].get("name", name)),
                f"create reported '{create_error}' but the session appeared "
                f"{waited}s later")

    if lookup_failed is not None:
        return LaunchResult(
            [], "unknown", name,
            f"create reported '{create_error}' and the reconcile lookup also "
            f"failed ({lookup_failed}); assuming it launched")

    if _is_ambiguous_failure(create_exc):
        return LaunchResult(
            [], "unknown", name,
            f"create reported '{create_error}' and no session for {unit_key} "
            f"appeared within {sum(poll_delays)}s -- but registration has been "
            f"seen to take longer than that, so it may still start. Check the "
            f"cluster before relaunching this unit. If it did launch, the "
            f"headless session records its own DB state on completion and the "
            f"unit drops off the outstanding list on its own")

    raise LaunchFailed(f"{create_error} (no session for {unit_key} after "
                       f"{sum(poll_delays)}s of lookups)")


def report_launch(logger, prefix: str, what: str, result: LaunchResult) -> str:
    """Log a LaunchResult uniformly and return the job id ('' when unknown).

    `logger` is passed in rather than created here so this module stays free of
    a Prefect dependency -- the batch_*.py launchers import it too.
    """
    job_id = result.ids[0] if result.ids else ""
    if result.outcome == "created":
        logger.info(f"{prefix} Headless {what} session launched: {job_id}")
    elif result.outcome == "adopted":
        logger.warning(f"{prefix} Reusing existing {what} session {job_id}: "
                       f"{result.detail}")
    else:
        logger.error(f"{prefix} {what} launch outcome UNKNOWN, treating as "
                     f"launched: {result.detail}. Verify on the cluster before "
                     f"relaunching this unit.")
    return job_id


def outcome_line(result: LaunchResult) -> str:
    """One-line launch summary for the print()-based batch launchers."""
    job_id = result.ids[0] if result.ids else "?"
    if result.outcome == "created":
        return f"submitted ({job_id})"
    if result.outcome == "adopted":
        return f"ALREADY RUNNING ({job_id}) -- {result.detail}"
    return f"UNKNOWN, may have launched -- {result.detail}"


def resource_desc(ncores: Optional[int], ram_gb: Optional[int]) -> str:
    """Human-readable description of what a launch pinned, for log lines.

    cores and RAM are pinned independently, so all four combinations are real.
    """
    parts = []
    if ncores is not None:
        parts.append(f"{ncores} cores")
    if ram_gb is not None:
        parts.append(f"{ram_gb} GB RAM")
    if not parts:
        return "flexible mode"
    return ", ".join(parts) + " pinned"


class UnitBusy(RuntimeError):
    """A live session already owns this work unit.

    Raised by ensure_unit_free() so callers can bail out *before* doing anything
    destructive. create_checked()'s own preflight is not sufficient for that:
    it runs at launch time, by which point a caller may already have deleted the
    products the running session was working on. That is not hypothetical --
    on 2026-08-30 a batch run cleared the live working set out from under two
    tclean processes six hours into cleaning, and only then discovered they were
    running and declined to launch a duplicate.
    """

    def __init__(self, unit_key: str, sessions: list):
        self.unit_key = unit_key
        self.sessions = sessions
        ids = ", ".join(str(s.get("id")) for s in sessions)
        super().__init__(f"{len(sessions)} session(s) already running on "
                         f"{unit_key}: {ids}")


def ensure_unit_free(session, unit_key: str) -> None:
    """Raise UnitBusy if a live session already owns this work unit.

    Call this before deleting products, writing payloads, or marking DB state --
    anything a concurrently running session would notice. A lookup failure is
    treated as busy: not knowing is not the same as knowing it is free, and the
    destructive path is the one that cannot be undone.
    """
    try:
        busy = live_sessions(session, unit_key)
    except Exception as exc:
        raise UnitBusy(unit_key, [{"id": f"lookup failed ({type(exc).__name__})"}])
    if busy:
        raise UnitBusy(unit_key, busy)
