"""Memory bounds of the collector's per-disk session state."""

from __future__ import annotations

import pathlib
import sys
import tracemalloc
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "source/usr/local/emhttp/plugins/disk.talkers/scripts"
sys.path.insert(0, str(SCRIPTS))

import collector  # noqa: E402
import session_limits  # noqa: E402


def talker_state(talker_id: str, last_seen: float) -> dict:
    return {"id": talker_id, "event_pids": set(), "pid_event_counts": {}, "pid_paths": {}, "last_seen": last_seen}


class RecordPidEventTest(unittest.TestCase):
    def test_keeps_only_most_recent_pids(self) -> None:
        state = talker_state("container:app", 0.0)
        for pid in range(1, 1001):
            session_limits.record_pid_event(state, pid, f"/mnt/disk1/f{pid}", 5)
        self.assertEqual(len(state["pid_event_counts"]), session_limits.PID_TRACK_LIMIT)
        self.assertEqual(set(state["pid_event_counts"]), state["event_pids"])
        self.assertEqual(set(state["pid_paths"]), state["event_pids"])
        self.assertIn(1000, state["event_pids"])
        self.assertNotIn(1, state["event_pids"])

    def test_repeated_pid_is_refreshed_not_evicted(self) -> None:
        state = talker_state("container:app", 0.0)
        session_limits.record_pid_event(state, 7, "/mnt/disk1/a", 5)
        for pid in range(100, 100 + session_limits.PID_TRACK_LIMIT * 3):
            session_limits.record_pid_event(state, 7, "/mnt/disk1/a", 5)
            session_limits.record_pid_event(state, pid, "/mnt/disk1/b", 5)
        self.assertIn(7, state["event_pids"])
        self.assertEqual(state["pid_event_counts"][7], 1 + session_limits.PID_TRACK_LIMIT * 3)


class PruneSessionTalkersTest(unittest.TestCase):
    def test_pool_drops_talkers_older_than_window(self) -> None:
        session = {"talkers": {"container:old": talker_state("container:old", 100.0), "container:new": talker_state("container:new", 990.0)}}
        session_limits.prune_session_talkers(session, 1000.0, 300.0, keeps_whole_session=False)
        self.assertEqual(list(session["talkers"]), ["container:new"])

    def test_array_disk_keeps_old_named_talkers_but_not_dead_pids(self) -> None:
        session = {"talkers": {"container:old": talker_state("container:old", 100.0), "pid:42": talker_state("pid:42", 100.0)}}
        session_limits.prune_session_talkers(session, 1000.0, 300.0, keeps_whole_session=True)
        self.assertEqual(list(session["talkers"]), ["container:old"])

    def test_talker_count_is_capped_by_recency(self) -> None:
        count = session_limits.SESSION_TALKER_LIMIT + 20
        session = {"talkers": {f"container:{i}": talker_state(f"container:{i}", 1000.0 + i) for i in range(count)}}
        session_limits.prune_session_talkers(session, 1000.0 + count, 300.0, keeps_whole_session=True)
        self.assertEqual(len(session["talkers"]), session_limits.SESSION_TALKER_LIMIT)
        self.assertIn(f"container:{count - 1}", session["talkers"])
        self.assertNotIn("container:0", session["talkers"])


class CollectorReplayTest(unittest.TestCase):
    def make_collector(self) -> collector.DiskTalkersCollector:
        with mock.patch.object(collector, "FanotifyMonitor", side_effect=OSError("no fanotify")):
            col = collector.DiskTalkersCollector(recent_window=300, max_talkers=5)
        self.disk = {"id": "cache", "name": "cache", "kind": "pool", "mount": "/mnt/cache"}
        col.disks = [self.disk]
        named = [{"id": f"container:app{i}", "name": f"app{i}", "kind": "container", "icon": {}} for i in range(8)]

        def identify(pid: int, path: str) -> dict:
            if pid % 3:
                return named[pid % 8]
            return {"id": f"pid:{pid}", "name": f"PID {pid}", "kind": "service", "icon": {}}

        col.identify_talker = identify
        return col

    def replay(self, col: collector.DiskTalkersCollector, start: int, count: int) -> None:
        for i in range(start, start + count):
            timestamp = i * 0.01
            event = {"pid": 1000 + i, "path": f"/mnt/cache/appdata/f{i % 5000}", "timestamp": timestamp, "mask": 0}
            col.record_event(self.disk, event)
            if i % 500 == 0:
                col.prune_sessions(timestamp)
        col.prune_sessions((start + count) * 0.01)

    def test_memory_stays_flat_over_a_long_run_of_new_pids(self) -> None:
        col = self.make_collector()
        tracemalloc.start()
        self.replay(col, 0, 100_000)
        after_warmup, _ = tracemalloc.get_traced_memory()
        self.replay(col, 100_000, 300_000)
        after_run, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertLessEqual(len(col.sessions["cache"]["talkers"]), session_limits.SESSION_TALKER_LIMIT)
        self.assertLess(after_run - after_warmup, 1_000_000)

    def test_mount_root_directory_open_is_not_a_talker(self) -> None:
        col = self.make_collector()
        col.monitor = mock.Mock(overflowed=False)
        col.monitor.drain.return_value = [
            {"pid": 5, "path": "/mnt/cache", "timestamp": 1.0, "mask": collector.FAN_OPEN},
            {"pid": 7, "path": "/mnt/cache/appdata", "timestamp": 1.0, "mask": collector.FAN_OPEN},
        ]
        col.drain_kernel_events()
        talkers = col.sessions["cache"]["talkers"]
        self.assertEqual(sum(t["event_count"] for t in talkers.values()), 1)


if __name__ == "__main__":
    unittest.main()
