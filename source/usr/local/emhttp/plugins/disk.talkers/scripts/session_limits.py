"""Size limits for the per-disk session state the collector keeps in memory.

A pool never spins down and an array disk can stay awake for weeks, so its
session lives as long as the collector. Every structure kept per session must
therefore be bounded by count, never by the session's age.
"""

from __future__ import annotations

from typing import Any

PID_TRACK_LIMIT = 32
SESSION_TALKER_LIMIT = 48
UNKNOWN_PID_PREFIX = "pid:"


def bounded_unique_prepend(items: list[Any], value: Any, limit: int) -> None:
    """Move value to the front of items and drop whatever falls past limit."""
    if value in items:
        items.remove(value)
    items.insert(0, value)
    del items[limit:]


def record_pid_event(state: dict[str, Any], pid: int, path: str, path_limit: int) -> None:
    """Count one event for pid on a talker, keeping only the most recent pids.

    pid_event_counts and pid_paths are kept in least-recent-first order so the
    oldest pid is the first key; short-lived workers (one pid per file) would
    otherwise add a key for every event.
    """
    counts = state["pid_event_counts"]
    counts[pid] = int(counts.pop(pid, 0)) + 1
    pid_paths = state["pid_paths"]
    paths = pid_paths.pop(pid, [])
    bounded_unique_prepend(paths, path, path_limit)
    pid_paths[pid] = paths
    state["event_pids"].add(pid)
    while len(counts) > PID_TRACK_LIMIT:
        oldest = next(iter(counts))
        counts.pop(oldest)
        pid_paths.pop(oldest, None)
        state["event_pids"].discard(oldest)


def prune_session_talkers(
    session: dict[str, Any],
    now: float,
    recent_window: float,
    keeps_whole_session: bool,
) -> None:
    """Drop talkers that can no longer be shown, then cap the rest by recency.

    keeps_whole_session is true for array disks, whose table lists every talker
    since the spin-up; pools and unassigned devices only show the recent window.
    A bare pid talker belongs to a process that is gone, so it expires either way.
    """
    talkers = session["talkers"]
    cutoff = now - recent_window
    for talker_id in list(talkers):
        stale = float(talkers[talker_id].get("last_seen") or 0.0) < cutoff
        if stale and (not keeps_whole_session or talker_id.startswith(UNKNOWN_PID_PREFIX)):
            del talkers[talker_id]
    if len(talkers) > SESSION_TALKER_LIMIT:
        newest = sorted(talkers, key=lambda tid: float(talkers[tid].get("last_seen") or 0.0), reverse=True)
        for talker_id in newest[SESSION_TALKER_LIMIT:]:
            del talkers[talker_id]
