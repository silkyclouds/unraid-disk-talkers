#!/usr/bin/env python3
"""Collect exact Unraid disk talkers from fanotify events and active holders."""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import datetime as dt
import fcntl
import json
import os
import pathlib
import re
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any

PLUGIN_NAME = "disk.talkers"
CONFIG_PATH = f"/boot/config/plugins/{PLUGIN_NAME}/{PLUGIN_NAME}.cfg"
DISKS_INI_PATH = "/var/local/emhttp/disks.ini"
SHARES_CFG_DIR = "/boot/config/shares"
LOCK_PATH = "/tmp/disk.talkers/collector.lock"
DEFAULT_STATE_PATH = "/tmp/disk.talkers/state.json"
DEFAULT_HISTORY_PATH = f"/boot/config/plugins/{PLUGIN_NAME}/history.sqlite3"
FRONTDOOR_MOUNTS = ["/mnt/user", "/mnt/user0"]
DEFAULT_HDD_POWER_MIN_W = 6.0
DEFAULT_HDD_POWER_MAX_W = 9.0
DEFAULT_CURRENCY_SYMBOL = "€"
DEFAULT_ELECTRICITY_TARIFF_MODE = "single"
DEFAULT_ELECTRICITY_SINGLE_RATE = 0.0
DEFAULT_ELECTRICITY_PEAK_RATE = 0.0
DEFAULT_ELECTRICITY_OFFPEAK_RATE = 0.0
DEFAULT_ELECTRICITY_OFFPEAK_START = "22:00"
DEFAULT_ELECTRICITY_OFFPEAK_END = "07:00"

RESERVED_MOUNTS = {
    "/mnt",
    "/mnt/addons",
    "/mnt/disks",
    "/mnt/remotes",
    "/mnt/rootshare",
    "/mnt/user",
    "/mnt/user0",
}

SERVICE_MAP = {
    "shfs": ("User Shares (shfs)", "service", "fa-folder-open"),
    "smbd": ("SMB", "service", "fa-share-alt"),
    "smbd-notifyd": ("SMB", "service", "fa-share-alt"),
    "smbd-cleanupd": ("SMB", "service", "fa-share-alt"),
    "emhttpd": ("Unraid WebUI", "service", "fa-globe"),
    "nginx": ("Unraid WebUI", "service", "fa-globe"),
    "php-fpm": ("Unraid WebUI", "service", "fa-globe"),
    "rclone": ("rclone", "service", "fa-cloud"),
    "rsync": ("rsync", "service", "fa-exchange"),
    "mover": ("Mover", "service", "fa-truck"),
}

PID_CACHE_TTL = 60.0
INVENTORY_REFRESH_SECONDS = 5.0
LOOP_SLEEP_SECONDS = 0.05
SPINUP_GRACE_SECONDS = 10.0
PROXY_RESOLVE_WINDOW = 15.0
PATH_LIMIT = 5
PID_LIMIT = 12
SECTOR_SIZE = 512
SUPPRESSED_TALKER_IDS = {"service:User Shares (shfs)"}
CONTAINER_IO_ACTIVE_BPS = 4096.0
OPEN_PATHS_CACHE_TTL = 5.0
DEFAULT_HISTORY_SAMPLE_INTERVAL = 300
HISTORY_BUCKET_SECONDS = 3600
HISTORY_PERIODS = {
    "daily": 86400,
    "weekly": 7 * 86400,
    "monthly": 30 * 86400,
    "yearly": 365 * 86400,
}

TIMELINE_GROUPS = {
    "daily": "hour",
    "weekly": "day",
    "monthly": "day",
    "yearly": "month",
}

UNATTRIBUTED_TALKER_ID = "service:Unattributed activity"
UNATTRIBUTED_TALKER_NAME = "Unattributed activity"
UNATTRIBUTED_TALKER_ICON = {"type": "fa", "value": "fa-history"}

FAN_CLOEXEC = 0x00000001
FAN_NONBLOCK = 0x00000002
FAN_CLASS_NOTIF = 0x00000000
FAN_UNLIMITED_QUEUE = 0x00000010
FAN_UNLIMITED_MARKS = 0x00000020

FAN_MARK_ADD = 0x00000001
FAN_MARK_MOUNT = 0x00000010

FAN_ACCESS = 0x00000001
FAN_MODIFY = 0x00000002
FAN_CLOSE_WRITE = 0x00000008
FAN_CLOSE_NOWRITE = 0x00000010
FAN_OPEN = 0x00000020
FAN_OPEN_EXEC = 0x00001000
FAN_Q_OVERFLOW = 0x00004000

FAN_EVENT_MASK = FAN_OPEN | FAN_OPEN_EXEC | FAN_MODIFY | FAN_CLOSE_WRITE | FAN_CLOSE_NOWRITE
AT_FDCWD = -100
METADATA_STRUCT = struct.Struct("IBBHQii")


def shell(command: list[str]) -> str:
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    return proc.stdout.strip()


def read_text(path: str) -> str:
    try:
        return pathlib.Path(path).read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return ""


def read_cfg(path: str) -> dict[str, str]:
    config: dict[str, str] = {}
    for line in read_text(path).splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        config[key.strip()] = value.strip().strip("\"'")
    return config


def parse_disks_ini(path: str) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None

    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            current = {}
            sections[line[1:-1].strip().strip('"')] = current
            continue
        if current is None or "=" not in line:
            continue
        key, value = line.split("=", 1)
        current[key.strip()] = value.strip().strip('"')
    return sections


def load_share_configs(directory: str = SHARES_CFG_DIR) -> dict[str, dict[str, str]]:
    shares: dict[str, dict[str, str]] = {}
    root = pathlib.Path(directory)
    if not root.exists():
        return shares
    for path in root.glob("*.cfg"):
        shares[path.stem] = read_cfg(str(path))
    return shares


def list_mounts() -> list[dict[str, str]]:
    mounts: list[dict[str, str]] = []
    output = shell(["findmnt", "-rn", "-o", "TARGET,SOURCE,FSTYPE"])
    for line in output.splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        target, source, fstype = parts
        if not target.startswith("/mnt/") or target in RESERVED_MOUNTS or target.startswith("/mnt/disks/"):
            continue
        mounts.append({"target": target.rstrip("/"), "source": source, "fstype": fstype})
    return mounts


def parse_mdcmd_status() -> dict[int, dict[str, str]]:
    rows: dict[int, dict[str, str]] = {}
    for line in shell(["mdcmd", "status"]).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        match = re.fullmatch(r"(diskName|rdevName)\.(\d+)", key)
        if not match:
            continue
        field = match.group(1)
        index = int(match.group(2))
        rows.setdefault(index, {})[field] = value.strip()
    return rows


def read_diskstats() -> dict[str, tuple[int, int]]:
    stats: dict[str, tuple[int, int]] = {}
    for line in read_text("/proc/diskstats").splitlines():
        parts = line.split()
        if len(parts) < 10:
            continue
        name = parts[2]
        try:
            sectors_read = int(parts[5])
            sectors_written = int(parts[9])
        except ValueError:
            continue
        stats[name] = (sectors_read, sectors_written)
    return stats


def human_rate(value: float) -> str:
    if value <= 0:
        return "-"
    units = ["B/s", "KB/s", "MB/s", "GB/s"]
    index = 0
    while value >= 1024.0 and index < len(units) - 1:
        value /= 1024.0
        index += 1
    if index == 0:
        return f"{int(value)} {units[index]}"
    return f"{value:.1f} {units[index]}"


def human_duration(value: float) -> str:
    if value <= 0:
        return "-"
    seconds = int(round(value))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, _ = divmod(seconds, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def human_percent(value: float) -> str:
    if value <= 0:
        return "0%"
    if value < 10:
        return f"{value:.1f}%"
    return f"{value:.0f}%"


def human_energy_kwh(value_kwh: float) -> str:
    if value_kwh <= 0:
        return "0 Wh"
    if value_kwh < 1:
        return f"{value_kwh * 1000:.0f} Wh"
    return f"{value_kwh:.2f} kWh"


def human_currency(value: float, symbol: str = DEFAULT_CURRENCY_SYMBOL) -> str:
    if value <= 0:
        return f"{symbol}0.00"
    if value < 1:
        return f"{symbol}{value:.2f}"
    return f"{symbol}{value:.2f}"


def parse_clock_minutes(value: str, fallback: str) -> int:
    text = (value or fallback or "").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not match:
        text = fallback
        match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not match:
        return 0
    return int(match.group(1)) * 60 + int(match.group(2))


def tariff_window_overlap_seconds(
    interval_start: dt.datetime,
    interval_end: dt.datetime,
    offpeak_start_minutes: int,
    offpeak_end_minutes: int,
) -> float:
    if interval_end <= interval_start or offpeak_start_minutes == offpeak_end_minutes:
        return 0.0

    overlap = 0.0
    start_day = (interval_start - dt.timedelta(days=1)).date()
    end_day = interval_end.date()
    total_days = (end_day - start_day).days + 1

    for day_offset in range(total_days):
        day = start_day + dt.timedelta(days=day_offset)
        base = dt.datetime.combine(day, dt.time.min, tzinfo=interval_start.tzinfo)
        if offpeak_start_minutes < offpeak_end_minutes:
            window_start = base + dt.timedelta(minutes=offpeak_start_minutes)
            window_end = base + dt.timedelta(minutes=offpeak_end_minutes)
        else:
            window_start = base + dt.timedelta(minutes=offpeak_start_minutes)
            window_end = base + dt.timedelta(days=1, minutes=offpeak_end_minutes)

        segment_start = max(interval_start, window_start)
        segment_end = min(interval_end, window_end)
        if segment_end > segment_start:
            overlap += (segment_end - segment_start).total_seconds()

    return overlap


def tariff_note(
    mode: str,
    currency_symbol: str,
    single_rate: float,
    peak_rate: float,
    offpeak_rate: float,
    offpeak_start: str,
    offpeak_end: str,
) -> str:
    if mode == "dual":
        if peak_rate <= 0 and offpeak_rate <= 0:
            return "Set peak and off-peak tariffs in Settings to estimate cost."
        return (
            f"Peak {currency_symbol}{peak_rate:.2f}/kWh · "
            f"Off-peak {currency_symbol}{offpeak_rate:.2f}/kWh "
            f"({offpeak_start}-{offpeak_end})"
        )
    if single_rate <= 0:
        return "Set your electricity tariff in Settings to estimate cost."
    return f"Single tariff {currency_symbol}{single_rate:.2f}/kWh"


def unattributed_talker(history_share: float = 1.0) -> dict[str, Any]:
    return {
        "id": UNATTRIBUTED_TALKER_ID,
        "name": UNATTRIBUTED_TALKER_NAME,
        "kind": "service",
        "icon": dict(UNATTRIBUTED_TALKER_ICON),
        "history_share": history_share,
    }


def normalize_history_talker_identity(
    talker_id: str,
    talker_name: str,
    kind: str,
    icon: dict[str, Any] | None,
) -> tuple[str, str, str, dict[str, Any]]:
    if talker_id == "service:Unknown" or talker_name == "Unknown":
        return (
            UNATTRIBUTED_TALKER_ID,
            UNATTRIBUTED_TALKER_NAME,
            "service",
            dict(UNATTRIBUTED_TALKER_ICON),
        )
    return talker_id, talker_name, kind, icon or {"type": "fa", "value": "fa-cog"}


def floor_period_start_local(moment: dt.datetime, group: str) -> dt.datetime:
    if group == "hour":
        return moment.replace(minute=0, second=0, microsecond=0)
    if group == "day":
        return moment.replace(hour=0, minute=0, second=0, microsecond=0)
    if group == "month":
        return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return moment


def add_period_step(moment: dt.datetime, group: str, step: int = 1) -> dt.datetime:
    if group == "hour":
        return moment + dt.timedelta(hours=step)
    if group == "day":
        return moment + dt.timedelta(days=step)
    if group == "month":
        month_index = (moment.month - 1) + step
        year = moment.year + (month_index // 12)
        month = (month_index % 12) + 1
        return moment.replace(year=year, month=month, day=1)
    return moment


def format_timeline_label(moment: dt.datetime, group: str) -> tuple[str, str]:
    if group == "hour":
        return moment.strftime("%H:%M"), moment.strftime("%H:%M")
    if group == "day":
        return moment.strftime("%a %d %b"), moment.strftime("%a")
    if group == "month":
        return moment.strftime("%b %Y"), moment.strftime("%b")
    return moment.isoformat(), moment.isoformat()


def normalize_talker_seconds(items: list[dict[str, Any]], ceiling_seconds: float) -> list[dict[str, Any]]:
    normalized = [dict(item) for item in items]
    total_seconds = sum(float(item.get("seconds", 0.0) or 0.0) for item in normalized)
    if ceiling_seconds > 0 and total_seconds > ceiling_seconds and total_seconds > 0:
        scale = ceiling_seconds / total_seconds
        for item in normalized:
            item["seconds"] = float(item.get("seconds", 0.0) or 0.0) * scale
    return normalized


def canonical_user_path(path: str) -> str:
    if path.startswith("/mnt/user0/"):
        return "/mnt/user/" + path.split("/", 3)[3]
    return path


def disk_path_to_user_path(path: str) -> str | None:
    if not path.startswith("/mnt/"):
        return None
    parts = path.split("/", 3)
    if len(parts) < 4 or parts[2] in {"user", "user0"}:
        return None
    return "/mnt/user/" + parts[3]


def read_pid_io(pid: int) -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in read_text(f"/proc/{pid}/io").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        try:
            values[key.strip()] = int(value.strip())
        except ValueError:
            continue
    return values.get("read_bytes", 0), values.get("write_bytes", 0)


def read_open_paths(pid: int, limit: int = PATH_LIMIT) -> list[str]:
    paths: list[str] = []
    fd_dir = pathlib.Path(f"/proc/{pid}/fd")
    try:
        entries = list(fd_dir.iterdir())
    except OSError:
        return paths

    for entry in entries:
        try:
            target = os.readlink(entry)
        except OSError:
            continue
        target = target.replace(" (deleted)", "")
        if not target.startswith("/mnt/"):
            continue
        bounded_unique_prepend(paths, target, limit)
    return paths


def natural_mount_key(mount: dict[str, Any]) -> tuple[int, Any]:
    name = mount["name"]
    match = re.fullmatch(r"disk(\d+)", name)
    if match:
        return (0, int(match.group(1)))
    if name == "cache":
        return (1, 0)
    return (2, name)


def build_disk_inventory() -> list[dict[str, Any]]:
    disks_ini = parse_disks_ini(DISKS_INI_PATH)
    md_status = parse_mdcmd_status()
    disks: list[dict[str, Any]] = []

    for mount in list_mounts():
        name = os.path.basename(mount["target"])
        meta = disks_ini.get(name, {})
        rotational = meta.get("rotational", "0")
        spundown = meta.get("spundown")
        match = re.fullmatch(r"disk(\d+)", name)
        stat_device = os.path.basename(mount["source"]).replace("/dev/", "")

        state = "active"
        label = "pool / ssd"

        if match:
            md_entry = md_status.get(int(match.group(1)), {})
            stat_device = md_entry.get("rdevName") or stat_device
            if spundown == "1":
                state = "spun_down"
                label = "spun down"
            else:
                state = "spun_up"
                label = "spun up"
        elif name == "cache":
            if rotational == "1" and spundown == "1":
                state = "spun_down"
                label = "spun down"
            else:
                state = "active"
                label = "pool / ssd"
        elif rotational == "1" and spundown == "1":
            state = "spun_down"
            label = "spun down"

        disks.append(
            {
                "id": name,
                "name": name,
                "mount": mount["target"],
                "device": meta.get("deviceSb") or mount["source"],
                "source": mount["source"],
                "fstype": mount["fstype"],
                "kind": "disk" if re.fullmatch(r"disk\d+", name) else "pool",
                "status": {"state": state, "label": label},
                "rotational": rotational == "1",
                "stat_device": stat_device,
            }
        )

    disks.sort(key=natural_mount_key)
    return disks


def bounded_unique_prepend(items: list[Any], value: Any, limit: int) -> None:
    if value in items:
        items.remove(value)
    items.insert(0, value)
    del items[limit:]


def parse_templates() -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    by_name: dict[str, str] = {}
    by_repo: dict[str, str] = {}
    path_by_name: dict[str, str] = {}
    path_by_repo: dict[str, str] = {}
    template_dir = pathlib.Path("/boot/config/plugins/dockerMan/templates-user")
    if not template_dir.exists():
        return by_name, by_repo, path_by_name, path_by_repo

    for template_path in template_dir.glob("*.xml"):
        try:
            root = ET.parse(template_path).getroot()
        except ET.ParseError:
            continue

        name = (root.findtext("Name") or "").strip()
        repo = (root.findtext("Repository") or "").strip()
        icon = (root.findtext("Icon") or "").strip()
        if icon:
            if name:
                by_name[name] = icon
            if repo:
                by_repo[repo] = icon
        if name:
            path_by_name[name] = str(template_path)
        if repo:
            path_by_repo[repo] = str(template_path)
    return by_name, by_repo, path_by_name, path_by_repo


class Resolver:
    def __init__(self) -> None:
        self._containers: dict[str, dict[str, Any]] = {}
        self._templates_by_name, self._templates_by_repo, self._template_paths_by_name, self._template_paths_by_repo = parse_templates()
        self._container_cache_until = 0.0

    def load_containers(self) -> dict[str, dict[str, Any]]:
        now = time.time()
        if now < self._container_cache_until and self._containers:
            return self._containers

        self._containers = {}
        output = shell(["docker", "ps", "-a", "--format", "{{.ID}} {{.Names}}"])
        ids = [line.split(None, 1)[0] for line in output.splitlines() if line.strip()]
        if not ids:
            self._container_cache_until = now + 30
            return self._containers

        inspect_proc = subprocess.run(
            ["docker", "inspect", *ids],
            capture_output=True,
            text=True,
            check=False,
        )
        if inspect_proc.returncode != 0:
            self._container_cache_until = now + 10
            return self._containers

        try:
            inspected = json.loads(inspect_proc.stdout)
        except json.JSONDecodeError:
            self._container_cache_until = now + 10
            return self._containers

        for info in inspected:
            cid = info.get("Id", "")
            if not cid:
                continue
            name = info.get("Name", "").lstrip("/")
            repository = info.get("Config", {}).get("Image", "")
            labels = info.get("Config", {}).get("Labels", {}) or {}
            template_path = (
                self._template_paths_by_name.get(name)
                or self._template_paths_by_repo.get(repository)
                or ""
            )
            icon_url = (
                labels.get("net.unraid.docker.icon")
                or self._templates_by_name.get(name)
                or self._templates_by_repo.get(repository)
                or ""
            )
            mounts: list[str] = []
            mount_details: list[dict[str, str]] = []
            for mount in info.get("Mounts", []) or []:
                source = (mount.get("Source") or "").rstrip("/")
                target = (mount.get("Destination") or "").rstrip("/")
                if source.startswith("/mnt/"):
                    mounts.append(source)
                    mount_details.append({"source": source, "target": target})
            self._containers[cid] = {
                "id": cid,
                "name": name or cid[:12],
                "repository": repository,
                "icon_url": icon_url,
                "template_path": template_path,
                "status": str(info.get("State", {}).get("Status", "")),
                "mounts": sorted(set(mounts)),
                "mount_details": mount_details,
            }

        self._container_cache_until = now + 30
        return self._containers

    def container_from_id(self, cid: str) -> dict[str, Any] | None:
        containers = self.load_containers()
        if cid in containers:
            return containers[cid]
        for full_id, info in containers.items():
            if full_id.startswith(cid):
                return info
        return None

    def container_from_path(self, path: str, disks: list[dict[str, Any]]) -> dict[str, Any] | None:
        candidates: list[tuple[int, dict[str, Any]]] = []
        for info in self.load_containers().values():
            for source in info["mounts"]:
                if path == source or path.startswith(source.rstrip("/") + "/"):
                    candidates.append((len(source), info))
                    continue

                if source.startswith("/mnt/user/") or source.startswith("/mnt/user0/"):
                    relative = source.split("/", 3)[3]
                    for disk in disks:
                        translated = f"{disk['mount'].rstrip('/')}/{relative}".rstrip("/")
                        if path == translated or path.startswith(translated + "/"):
                            candidates.append((len(translated), info))

        if not candidates:
            return None

        best_len = max(length for length, _ in candidates)
        best = {info["id"]: info for length, info in candidates if length == best_len}
        if len(best) == 1:
            return next(iter(best.values()))
        return None

    def containers_for_user_path(self, path: str) -> list[tuple[int, dict[str, Any]]]:
        path = canonical_user_path(path)
        matches: list[tuple[int, dict[str, Any]]] = []
        for info in self.load_containers().values():
            for source in info["mounts"]:
                candidate = canonical_user_path(source)
                if not candidate.startswith("/mnt/user/"):
                    continue
                if path == candidate or path.startswith(candidate.rstrip("/") + "/"):
                    matches.append((len(candidate), info))
        matches.sort(key=lambda item: (-item[0], item[1]["name"].lower()))
        return matches


def read_proc_context(pid: int) -> dict[str, str]:
    try:
        comm = read_text(f"/proc/{pid}/comm").strip()
        cmdline = read_text(f"/proc/{pid}/cmdline").replace("\x00", " ").strip()
        cgroup = read_text(f"/proc/{pid}/cgroup")
        return {"comm": comm, "cmdline": cmdline, "cgroup": cgroup}
    except OSError:
        return {"comm": "", "cmdline": "", "cgroup": ""}


def container_talker(container: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"container:{container['id']}",
        "name": container["name"],
        "kind": "container",
        "icon": {"type": "image", "value": container["icon_url"]} if container["icon_url"] else {"type": "fa", "value": "fa-cube"},
    }


def classify_process(pid: int, resolver: Resolver) -> dict[str, Any] | None:
    context = read_proc_context(pid)
    if not context["comm"] and not context["cmdline"] and not context["cgroup"]:
        return None

    cgroup_match = re.search(r"/docker/([0-9a-f]{12,64})", context["cgroup"])
    if cgroup_match:
        container = resolver.container_from_id(cgroup_match.group(1))
        if container:
            return container_talker(container)

    if context["comm"].startswith("qemu-system"):
        vm_name = "Virtual Machine"
        guest_match = re.search(r"guest=([^, ]+)", context["cmdline"])
        if guest_match:
            vm_name = guest_match.group(1)
        return {
            "id": f"vm:{vm_name}",
            "name": vm_name,
            "kind": "vm",
            "icon": {"type": "fa", "value": "fa-desktop"},
        }

    if context["comm"] in SERVICE_MAP:
        label, kind, icon = SERVICE_MAP[context["comm"]]
        return {"id": f"{kind}:{label}", "name": label, "kind": kind, "icon": {"type": "fa", "value": icon}}

    if "/usr/local/emhttp" in context["cmdline"]:
        return {
            "id": "service:Unraid WebUI",
            "name": "Unraid WebUI",
            "kind": "service",
            "icon": {"type": "fa", "value": "fa-globe"},
        }

    if context["comm"] in {"bash", "sh"}:
        script_match = re.search(r"(/[^ ]+\.(?:sh|py|pl|rb))", context["cmdline"])
        label = f"Script: {os.path.basename(script_match.group(1))}" if script_match else "Shell"
        return {
            "id": f"script:{label}",
            "name": label,
            "kind": "script",
            "icon": {"type": "fa", "value": "fa-terminal"},
        }

    command = context["comm"] or f"PID {pid}"
    return {
        "id": f"service:{command}",
        "name": command,
        "kind": "service",
        "icon": {"type": "fa", "value": "fa-cog"},
    }


def parse_fuser_output(mount: str, resolver: Resolver) -> list[dict[str, Any]]:
    output = shell(["fuser", "-m", mount])
    rows: list[dict[str, Any]] = []
    for pid_raw in output.split():
        if not pid_raw.isdigit():
            continue
        pid = int(pid_raw)
        talker = classify_process(pid, resolver)
        if talker is None:
            talker = {
                "id": f"pid:{pid}",
                "name": f"PID {pid}",
                "kind": "service",
                "icon": {"type": "fa", "value": "fa-cog"},
            }
        rows.append({"pid": pid, "talker": talker})
    return rows


def find_disk_for_path(path: str, disks: list[dict[str, Any]]) -> dict[str, Any] | None:
    mounts = sorted(disks, key=lambda item: len(item["mount"]), reverse=True)
    return next((disk for disk in mounts if path == disk["mount"] or path.startswith(disk["mount"] + "/")), None)


class FanotifyMonitor:
    def __init__(self) -> None:
        libc_name = ctypes.util.find_library("c") or "libc.so.6"
        self.libc = ctypes.CDLL(libc_name, use_errno=True)
        flags = FAN_CLOEXEC | FAN_NONBLOCK | FAN_CLASS_NOTIF | FAN_UNLIMITED_QUEUE | FAN_UNLIMITED_MARKS
        event_flags = os.O_RDONLY | getattr(os, "O_LARGEFILE", 0)
        fd = self.libc.fanotify_init(flags, event_flags)
        if fd < 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
        self.fd = fd
        self.marked_mounts: set[str] = set()
        self.overflowed = False

    def sync(self, mounts: list[str]) -> None:
        for mount in mounts:
            if mount in self.marked_mounts:
                continue
            rc = self.libc.fanotify_mark(
                self.fd,
                FAN_MARK_ADD | FAN_MARK_MOUNT,
                FAN_EVENT_MASK,
                AT_FDCWD,
                ctypes.c_char_p(mount.encode("utf-8")),
            )
            if rc != 0:
                err = ctypes.get_errno()
                raise OSError(err, f"fanotify_mark({mount}) failed: {os.strerror(err)}")
            self.marked_mounts.add(mount)

    def drain(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        while True:
            try:
                data = os.read(self.fd, 65536)
            except BlockingIOError:
                break
            except OSError as exc:
                if exc.errno in {11, 4}:
                    break
                raise

            if not data:
                break

            offset = 0
            while offset + METADATA_STRUCT.size <= len(data):
                event_len, vers, metadata_reserved, metadata_len, mask, objfd, pid = METADATA_STRUCT.unpack_from(data, offset)
                if event_len < METADATA_STRUCT.size:
                    break

                if mask & FAN_Q_OVERFLOW:
                    self.overflowed = True
                elif objfd >= 0 and pid > 0:
                    try:
                        path = os.readlink(f"/proc/self/fd/{objfd}")
                    except OSError:
                        path = ""
                    if path.startswith("/mnt/"):
                        events.append(
                            {
                                "timestamp": time.time(),
                                "pid": pid,
                                "path": path,
                                "mask": int(mask),
                            }
                        )

                if objfd >= 0:
                    try:
                        os.close(objfd)
                    except OSError:
                        pass

                offset += max(event_len, METADATA_STRUCT.size)

        return events


def new_session(now: float) -> dict[str, Any]:
    return {"started_at": now, "talkers": {}}


def new_talker_state(base: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": base["id"],
        "name": base["name"],
        "kind": base["kind"],
        "icon": base["icon"],
        "active_pids": set(),
        "event_pids": set(),
        "pid_event_counts": {},
        "pid_paths": {},
        "paths": [],
        "event_count": 0,
        "last_seen": 0.0,
    }


def talker_sort_key(item: dict[str, Any]) -> tuple[int, int, float, str]:
    return (
        0 if item["active"] else 1,
        -item["event_count"],
        -item["last_seen_ts"],
        item["name"].lower(),
    )


class HistoryStore:
    def __init__(
        self,
        path: str,
        power_min_w: float = DEFAULT_HDD_POWER_MIN_W,
        power_max_w: float = DEFAULT_HDD_POWER_MAX_W,
        tariff_mode: str = DEFAULT_ELECTRICITY_TARIFF_MODE,
        single_rate: float = DEFAULT_ELECTRICITY_SINGLE_RATE,
        peak_rate: float = DEFAULT_ELECTRICITY_PEAK_RATE,
        offpeak_rate: float = DEFAULT_ELECTRICITY_OFFPEAK_RATE,
        offpeak_start: str = DEFAULT_ELECTRICITY_OFFPEAK_START,
        offpeak_end: str = DEFAULT_ELECTRICITY_OFFPEAK_END,
        currency_symbol: str = DEFAULT_CURRENCY_SYMBOL,
    ):
        self.path = path
        self.power_min_w = max(0.0, float(power_min_w))
        self.power_max_w = max(self.power_min_w, float(power_max_w))
        self.tariff_mode = tariff_mode if tariff_mode in {"single", "dual"} else DEFAULT_ELECTRICITY_TARIFF_MODE
        self.single_rate = max(0.0, float(single_rate))
        self.peak_rate = max(0.0, float(peak_rate))
        self.offpeak_rate = max(0.0, float(offpeak_rate))
        self.offpeak_start = offpeak_start or DEFAULT_ELECTRICITY_OFFPEAK_START
        self.offpeak_end = offpeak_end or DEFAULT_ELECTRICITY_OFFPEAK_END
        self.offpeak_start_minutes = parse_clock_minutes(self.offpeak_start, DEFAULT_ELECTRICITY_OFFPEAK_START)
        self.offpeak_end_minutes = parse_clock_minutes(self.offpeak_end, DEFAULT_ELECTRICITY_OFFPEAK_END)
        self.currency_symbol = currency_symbol or DEFAULT_CURRENCY_SYMBOL
        self._schema_ready = False

    def connect(self) -> sqlite3.Connection:
        target = pathlib.Path(self.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(target))
        conn.row_factory = sqlite3.Row
        if not self._schema_ready:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS disk_state_history (
                    bucket_start INTEGER NOT NULL,
                    disk_id TEXT NOT NULL,
                    spun_up_seconds REAL NOT NULL DEFAULT 0,
                    PRIMARY KEY (bucket_start, disk_id)
                );

                CREATE TABLE IF NOT EXISTS disk_talker_history (
                    bucket_start INTEGER NOT NULL,
                    disk_id TEXT NOT NULL,
                    talker_id TEXT NOT NULL,
                    talker_name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    icon_json TEXT NOT NULL,
                    seconds REAL NOT NULL DEFAULT 0,
                    PRIMARY KEY (bucket_start, disk_id, talker_id)
                );
                """
            )
            self._schema_ready = True
        return conn

    def record_payload(self, payload: dict[str, Any], sample_seconds: float, timestamp: float | None = None) -> None:
        if sample_seconds <= 0:
            return
        ts = int(timestamp or time.time())
        bucket_start = ts - (ts % HISTORY_BUCKET_SECONDS)
        disks = payload.get("disks", [])
        with self.connect() as conn:
            for disk in disks:
                if disk.get("kind") != "disk" or disk.get("status", {}).get("state") != "spun_up":
                    continue

                disk_id = str(disk["id"])
                conn.execute(
                    """
                    INSERT INTO disk_state_history(bucket_start, disk_id, spun_up_seconds)
                    VALUES (?, ?, ?)
                    ON CONFLICT(bucket_start, disk_id)
                    DO UPDATE SET spun_up_seconds = spun_up_seconds + excluded.spun_up_seconds
                    """,
                    (bucket_start, disk_id, sample_seconds),
                )

                talkers = list(disk.get("history_talkers") or disk.get("talkers") or [])
                if not talkers:
                    talkers = [unattributed_talker()]

                aggregated_talkers: dict[str, dict[str, Any]] = {}
                remaining = sample_seconds
                for index, talker in enumerate(talkers):
                    share = float(talker.get("history_share", 0.0) or 0.0)
                    seconds = remaining if index == len(talkers) - 1 else max(0.0, sample_seconds * share)
                    seconds = min(seconds, remaining)
                    remaining = max(0.0, remaining - seconds)
                    if seconds <= 0:
                        continue
                    talker_id, talker_name, talker_kind, talker_icon = normalize_history_talker_identity(
                        str(talker["id"]),
                        str(talker["name"]),
                        str(talker["kind"]),
                        talker.get("icon"),
                    )
                    item = aggregated_talkers.setdefault(
                        talker_id,
                        {
                            "name": talker_name,
                            "kind": talker_kind,
                            "icon": talker_icon,
                            "seconds": 0.0,
                        },
                    )
                    item["seconds"] += seconds

                for talker_id, item in aggregated_talkers.items():
                    conn.execute(
                        """
                        INSERT INTO disk_talker_history(bucket_start, disk_id, talker_id, talker_name, kind, icon_json, seconds)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(bucket_start, disk_id, talker_id)
                        DO UPDATE SET
                            talker_name = excluded.talker_name,
                            kind = excluded.kind,
                            icon_json = excluded.icon_json,
                            seconds = seconds + excluded.seconds
                        """,
                        (
                            bucket_start,
                            disk_id,
                            talker_id,
                            str(item["name"]),
                            str(item["kind"]),
                            json.dumps(item["icon"]),
                            float(item["seconds"]),
                        ),
                    )

            cutoff = bucket_start - (370 * 86400)
            conn.execute("DELETE FROM disk_state_history WHERE bucket_start < ?", (cutoff,))
            conn.execute("DELETE FROM disk_talker_history WHERE bucket_start < ?", (cutoff,))
            conn.commit()

    def build_summary(self, disks: list[dict[str, Any]]) -> dict[str, Any]:
        disk_names = [disk["id"] for disk in disks if disk.get("kind") == "disk"]
        if not disk_names:
            return {"default_period": "daily", "periods": {}}

        now_local = dt.datetime.now().astimezone()
        tzinfo = now_local.tzinfo or dt.timezone.utc
        disk_count = float(len(disk_names))
        timeline_point_counts = {"daily": 24, "weekly": 7, "monthly": 30, "yearly": 12}

        with self.connect() as conn:
            periods: dict[str, Any] = {}
            for period, window_seconds in HISTORY_PERIODS.items():
                since = int((now_local - dt.timedelta(seconds=window_seconds)).timestamp())
                group = TIMELINE_GROUPS.get(period, "day")

                disk_state_rows = conn.execute(
                    """
                    SELECT bucket_start, disk_id, spun_up_seconds
                    FROM disk_state_history
                    WHERE bucket_start >= ?
                    """,
                    (since,),
                ).fetchall()

                talker_rows = conn.execute(
                    """
                    SELECT bucket_start, disk_id, talker_id, talker_name, kind, icon_json, seconds
                    FROM disk_talker_history
                    WHERE bucket_start >= ?
                    """,
                    (since,),
                ).fetchall()

                disk_totals: dict[str, float] = {}
                by_disk: dict[str, dict[str, dict[str, Any]]] = {}
                array_talkers: dict[str, dict[str, Any]] = {}
                timeline_state_totals: dict[int, float] = {}
                timeline_talkers: dict[int, dict[str, dict[str, Any]]] = {}
                total_energy_min_kwh = 0.0
                total_energy_max_kwh = 0.0
                total_cost_min = 0.0
                total_cost_max = 0.0

                for row in disk_state_rows:
                    disk_id = str(row["disk_id"])
                    seconds = float(row["spun_up_seconds"] or 0.0)
                    if seconds <= 0:
                        continue
                    disk_totals[disk_id] = disk_totals.get(disk_id, 0.0) + seconds

                    bucket_local = dt.datetime.fromtimestamp(int(row["bucket_start"]), tz=tzinfo)
                    point_start = floor_period_start_local(bucket_local, group)
                    point_key = int(point_start.timestamp())
                    timeline_state_totals[point_key] = timeline_state_totals.get(point_key, 0.0) + seconds

                    bucket_end = bucket_local + dt.timedelta(seconds=HISTORY_BUCKET_SECONDS)
                    bucket_span_seconds = max((bucket_end - bucket_local).total_seconds(), 1.0)
                    offpeak_overlap = tariff_window_overlap_seconds(
                        bucket_local,
                        bucket_end,
                        self.offpeak_start_minutes,
                        self.offpeak_end_minutes,
                    )
                    offpeak_fraction = max(0.0, min(1.0, offpeak_overlap / bucket_span_seconds))
                    peak_fraction = 1.0 - offpeak_fraction

                    energy_min_kwh = (seconds / 3600.0 * self.power_min_w) / 1000.0
                    energy_max_kwh = (seconds / 3600.0 * self.power_max_w) / 1000.0
                    total_energy_min_kwh += energy_min_kwh
                    total_energy_max_kwh += energy_max_kwh

                    if self.tariff_mode == "dual":
                        total_cost_min += energy_min_kwh * ((peak_fraction * self.peak_rate) + (offpeak_fraction * self.offpeak_rate))
                        total_cost_max += energy_max_kwh * ((peak_fraction * self.peak_rate) + (offpeak_fraction * self.offpeak_rate))
                    else:
                        total_cost_min += energy_min_kwh * self.single_rate
                        total_cost_max += energy_max_kwh * self.single_rate

                for row in talker_rows:
                    disk_id = str(row["disk_id"])
                    seconds = float(row["seconds"] or 0.0)
                    if seconds <= 0:
                        continue

                    icon = {"type": "fa", "value": "fa-cog"}
                    raw_icon = row["icon_json"]
                    if raw_icon:
                        try:
                            icon = json.loads(raw_icon)
                        except json.JSONDecodeError:
                            pass

                    talker_id, talker_name, talker_kind, talker_icon = normalize_history_talker_identity(
                        str(row["talker_id"]),
                        str(row["talker_name"]),
                        str(row["kind"]),
                        icon,
                    )

                    disk_item = by_disk.setdefault(disk_id, {}).setdefault(
                        talker_id,
                        {
                            "id": talker_id,
                            "name": talker_name,
                            "kind": talker_kind,
                            "icon": talker_icon,
                            "seconds": 0.0,
                        },
                    )
                    disk_item["seconds"] += seconds

                    agg = array_talkers.setdefault(
                        talker_id,
                        {
                            "id": talker_id,
                            "name": talker_name,
                            "kind": talker_kind,
                            "icon": talker_icon,
                            "seconds": 0.0,
                        },
                    )
                    agg["seconds"] += seconds

                    bucket_local = dt.datetime.fromtimestamp(int(row["bucket_start"]), tz=tzinfo)
                    point_start = floor_period_start_local(bucket_local, group)
                    point_key = int(point_start.timestamp())
                    point_item = timeline_talkers.setdefault(point_key, {}).setdefault(
                        talker_id,
                        {
                            "id": talker_id,
                            "name": talker_name,
                            "kind": talker_kind,
                            "icon": talker_icon,
                            "seconds": 0.0,
                        },
                    )
                    point_item["seconds"] += seconds

                period_disks: dict[str, Any] = {}
                total_spun_up_seconds = 0.0
                for disk_id in disk_names:
                    spun_up_seconds = min(float(disk_totals.get(disk_id, 0.0)), float(window_seconds))
                    total_spun_up_seconds += spun_up_seconds
                    talkers = normalize_talker_seconds(list(by_disk.get(disk_id, {}).values()), spun_up_seconds)
                    talkers.sort(key=lambda item: (-item["seconds"], item["name"].lower()))
                    top = []
                    for talker in talkers[:3]:
                        percent = (talker["seconds"] / spun_up_seconds * 100.0) if spun_up_seconds > 0 else 0.0
                        top.append(
                            {
                                "id": talker["id"],
                                "name": talker["name"],
                                "kind": talker["kind"],
                                "icon": talker["icon"],
                                "seconds": talker["seconds"],
                                "duration_human": human_duration(talker["seconds"]),
                                "percent": percent,
                                "percent_human": human_percent(percent),
                            }
                        )
                    period_disks[disk_id] = {
                        "spun_up_seconds": spun_up_seconds,
                        "spun_up_human": human_duration(spun_up_seconds),
                        "spun_up_percent": (spun_up_seconds / window_seconds * 100.0) if window_seconds > 0 else 0.0,
                        "spun_up_percent_human": human_percent((spun_up_seconds / window_seconds * 100.0) if window_seconds > 0 else 0.0),
                        "top_talkers": top,
                    }

                top_array = normalize_talker_seconds(list(array_talkers.values()), total_spun_up_seconds)
                top_array.sort(key=lambda item: (-item["seconds"], item["name"].lower()))

                point_count = timeline_point_counts.get(period, 24)
                current_point = floor_period_start_local(now_local, group)
                first_point = add_period_step(current_point, group, -(point_count - 1))
                timeline_points = []
                max_spun_up_disks = 0.0
                point_cursor = first_point
                while point_cursor <= current_point:
                    point_key = int(point_cursor.timestamp())
                    next_point = add_period_step(point_cursor, group, 1)
                    point_end = min(next_point, now_local)
                    point_window_seconds = max((point_end - point_cursor).total_seconds(), 1.0)
                    point_spun_up_seconds = min(float(timeline_state_totals.get(point_key, 0.0)), disk_count * point_window_seconds)
                    avg_spun_up_disks = min(point_spun_up_seconds / point_window_seconds, disk_count)
                    max_spun_up_disks = max(max_spun_up_disks, avg_spun_up_disks)
                    label, short_label = format_timeline_label(point_cursor, group)
                    point_talkers = normalize_talker_seconds(list(timeline_talkers.get(point_key, {}).values()), point_spun_up_seconds)
                    point_talkers.sort(key=lambda item: (-item["seconds"], item["name"].lower()))
                    top_point_talkers = []
                    for talker in point_talkers[:3]:
                        percent = (talker["seconds"] / point_spun_up_seconds * 100.0) if point_spun_up_seconds > 0 else 0.0
                        top_point_talkers.append(
                            {
                                "id": talker["id"],
                                "name": talker["name"],
                                "kind": talker["kind"],
                                "icon": talker["icon"],
                                "seconds": talker["seconds"],
                                "duration_human": human_duration(talker["seconds"]),
                                "percent": percent,
                                "percent_human": human_percent(percent),
                            }
                        )
                    timeline_points.append(
                        {
                            "timestamp": point_key,
                            "started_at": point_cursor.isoformat(),
                            "label": label,
                            "short_label": short_label,
                            "avg_spun_up_disks": avg_spun_up_disks,
                            "avg_spun_up_disks_human": f"{avg_spun_up_disks:.1f}",
                            "spun_up_seconds": point_spun_up_seconds,
                            "spun_up_human": human_duration(point_spun_up_seconds),
                            "top_talkers": top_point_talkers,
                        }
                    )
                    point_cursor = next_point

                periods[period] = {
                    "window_seconds": window_seconds,
                    "window_human": human_duration(window_seconds),
                    "total_spun_up_seconds": total_spun_up_seconds,
                    "total_spun_up_human": human_duration(total_spun_up_seconds),
                    "disk_hours": total_spun_up_seconds / 3600.0,
                    "disk_hours_human": f"{(total_spun_up_seconds / 3600.0):.1f} disk-hours",
                    "avg_spun_up_disks": min((total_spun_up_seconds / window_seconds), disk_count) if window_seconds > 0 else 0.0,
                    "avg_spun_up_disks_human": f"{min((total_spun_up_seconds / window_seconds), disk_count):.1f}" if window_seconds > 0 else "0.0",
                    "power": {
                        "min_w": self.power_min_w,
                        "max_w": self.power_max_w,
                        "energy_min_kwh": total_energy_min_kwh,
                        "energy_max_kwh": total_energy_max_kwh,
                        "energy_min_human": human_energy_kwh(total_energy_min_kwh),
                        "energy_max_human": human_energy_kwh(total_energy_max_kwh),
                        "assumption_human": f"{self.power_min_w:.0f}-{self.power_max_w:.0f} W per HDD",
                        "currency_symbol": self.currency_symbol,
                        "tariff_mode": self.tariff_mode,
                        "cost_min": total_cost_min,
                        "cost_max": total_cost_max,
                        "cost_min_human": human_currency(total_cost_min, self.currency_symbol),
                        "cost_max_human": human_currency(total_cost_max, self.currency_symbol),
                        "tariff_note_human": tariff_note(
                            self.tariff_mode,
                            self.currency_symbol,
                            self.single_rate,
                            self.peak_rate,
                            self.offpeak_rate,
                            self.offpeak_start,
                            self.offpeak_end,
                        ),
                    },
                    "top_array_talkers": [
                        {
                            "id": item["id"],
                            "name": item["name"],
                            "kind": item["kind"],
                            "icon": item["icon"],
                            "duration_human": human_duration(item["seconds"]),
                            "percent": (item["seconds"] / total_spun_up_seconds * 100.0) if total_spun_up_seconds > 0 else 0.0,
                            "percent_human": human_percent((item["seconds"] / total_spun_up_seconds * 100.0) if total_spun_up_seconds > 0 else 0.0),
                        }
                        for item in top_array[:5]
                    ],
                    "timeline": {
                        "group": group,
                        "max_spun_up_disks": max_spun_up_disks,
                        "max_spun_up_disks_human": f"{max_spun_up_disks:.1f}",
                        "points": timeline_points,
                    },
                    "disks": period_disks,
                }

        return {"default_period": "daily", "periods": periods}


class DiskTalkersCollector:
    def __init__(self, recent_window: int, max_talkers: int):
        self.recent_window = recent_window
        self.max_talkers = max_talkers
        self.resolver = Resolver()
        self.share_configs = load_share_configs()
        self.disks: list[dict[str, Any]] = []
        self.sessions: dict[str, dict[str, Any]] = {}
        self.history_talker_cache: dict[str, list[dict[str, Any]]] = {}
        self.frontdoor_activity: dict[str, dict[str, dict[str, Any]]] = {}
        self.disk_states: dict[str, str] = {}
        self.pid_cache: dict[int, tuple[float, dict[str, Any]]] = {}
        self.io_prev: dict[str, tuple[float, int, int]] = {}
        self.disk_rates: dict[str, dict[str, float | str]] = {}
        self.container_io_prev: dict[str, tuple[float, int, int]] = {}
        self.container_rates: dict[str, dict[str, float]] = {}
        self.pid_io_prev: dict[int, tuple[float, int, int]] = {}
        self.pid_rates: dict[int, dict[str, float | str]] = {}
        self.pid_open_paths_cache: dict[int, tuple[float, list[str]]] = {}
        self.last_inventory_refresh = 0.0
        self.monitor: FanotifyMonitor | None = None
        self.monitor_error = ""
        self.overflow_seen = False

        try:
            self.monitor = FanotifyMonitor()
        except OSError as exc:
            self.monitor_error = str(exc)

    def refresh_inventory_if_needed(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self.last_inventory_refresh) < INVENTORY_REFRESH_SECONDS and self.disks:
            return

        disks = build_disk_inventory()
        current_states = {disk["id"]: disk["status"]["state"] for disk in disks}
        known_ids = {disk["id"] for disk in disks}

        if self.monitor is not None:
            watch_mounts = [disk["mount"] for disk in disks]
            watch_mounts.extend(mount for mount in FRONTDOOR_MOUNTS if os.path.isdir(mount))
            self.monitor.sync(watch_mounts)

        for disk in disks:
            disk_id = disk["id"]
            state = disk["status"]["state"]
            previous = self.disk_states.get(disk_id)
            if disk["kind"] == "disk":
                if state == "spun_down":
                    self.sessions.pop(disk_id, None)
                    self.history_talker_cache.pop(disk_id, None)
                elif previous == "spun_down":
                    self.sessions[disk_id] = new_session(now - SPINUP_GRACE_SECONDS)
                else:
                    self.sessions.setdefault(disk_id, new_session(now))
            else:
                self.sessions.setdefault(disk_id, new_session(now))

        for disk_id in list(self.sessions):
            if disk_id not in known_ids:
                self.sessions.pop(disk_id, None)
                self.history_talker_cache.pop(disk_id, None)

        self.disks = disks
        self.disk_states = current_states
        self.last_inventory_refresh = now
        self.update_disk_rates(now)
        self.sample_container_rates(now)

    def prune_pid_cache(self) -> None:
        now = time.time()
        expired = [pid for pid, (expires_at, _) in self.pid_cache.items() if expires_at <= now]
        for pid in expired:
            self.pid_cache.pop(pid, None)

        stale_pids = [pid for pid, (timestamp, _, _) in self.pid_io_prev.items() if (now - timestamp) > 300]
        for pid in stale_pids:
            self.pid_io_prev.pop(pid, None)
            self.pid_rates.pop(pid, None)
            self.pid_open_paths_cache.pop(pid, None)

    def prune_frontdoor_activity(self) -> None:
        cutoff = time.time() - max(self.recent_window, PROXY_RESOLVE_WINDOW)
        for path in list(self.frontdoor_activity):
            by_talker = {
                talker_id: item
                for talker_id, item in self.frontdoor_activity[path].items()
                if item["timestamp"] >= cutoff
            }
            if by_talker:
                self.frontdoor_activity[path] = by_talker
            else:
                self.frontdoor_activity.pop(path, None)

    def update_disk_rates(self, now: float) -> None:
        stats = read_diskstats()
        rates: dict[str, dict[str, float | str]] = {}
        for disk in self.disks:
            stat_device = disk.get("stat_device") or ""
            current = stats.get(str(stat_device))
            if current is None:
                rates[disk["id"]] = {
                    "read_bps": 0.0,
                    "write_bps": 0.0,
                    "read_human": "-",
                    "write_human": "-",
                }
                continue

            current_read, current_write = current
            previous = self.io_prev.get(disk["id"])
            read_bps = 0.0
            write_bps = 0.0
            if previous is not None:
                previous_ts, previous_read, previous_write = previous
                elapsed = max(now - previous_ts, 0.001)
                read_bps = max(0.0, ((current_read - previous_read) * SECTOR_SIZE) / elapsed)
                write_bps = max(0.0, ((current_write - previous_write) * SECTOR_SIZE) / elapsed)

            self.io_prev[disk["id"]] = (now, current_read, current_write)
            rates[disk["id"]] = {
                "read_bps": read_bps,
                "write_bps": write_bps,
                "read_human": human_rate(read_bps),
                "write_human": human_rate(write_bps),
                }
        self.disk_rates = rates

    def sample_container_rates(self, now: float) -> None:
        rates: dict[str, dict[str, float]] = {}
        for container in self.resolver.load_containers().values():
            if not any(mount.startswith("/mnt/user/") or mount.startswith("/mnt/user0/") for mount in container["mounts"]):
                continue

            top_proc = subprocess.run(
                ["docker", "top", container["name"], "-eo", "pid"],
                capture_output=True,
                text=True,
                check=False,
            )
            if top_proc.returncode != 0:
                continue

            pids = [int(line.strip()) for line in top_proc.stdout.splitlines()[1:] if line.strip().isdigit()]
            total_read = 0
            total_write = 0
            for pid in pids:
                read_bytes, write_bytes = read_pid_io(pid)
                total_read += read_bytes
                total_write += write_bytes

            previous = self.container_io_prev.get(container["id"])
            read_bps = 0.0
            write_bps = 0.0
            if previous is not None:
                previous_ts, previous_read, previous_write = previous
                elapsed = max(now - previous_ts, 0.001)
                read_bps = max(0.0, (total_read - previous_read) / elapsed)
                write_bps = max(0.0, (total_write - previous_write) / elapsed)

            self.container_io_prev[container["id"]] = (now, total_read, total_write)
            rates[container["id"]] = {
                "read_bps": read_bps,
                "write_bps": write_bps,
                "total_bps": read_bps + write_bps,
                "pids": pids,
            }
        self.container_rates = rates

    def sample_pid_rate(self, pid: int, now: float) -> dict[str, float | str]:
        read_bytes, write_bytes = read_pid_io(pid)
        previous = self.pid_io_prev.get(pid)
        read_bps = 0.0
        write_bps = 0.0
        if previous is not None:
            previous_ts, previous_read, previous_write = previous
            elapsed = max(now - previous_ts, 0.001)
            read_bps = max(0.0, (read_bytes - previous_read) / elapsed)
            write_bps = max(0.0, (write_bytes - previous_write) / elapsed)

        self.pid_io_prev[pid] = (now, read_bytes, write_bytes)
        self.pid_rates[pid] = {
            "read_bps": read_bps,
            "write_bps": write_bps,
            "read_human": human_rate(read_bps),
            "write_human": human_rate(write_bps),
        }
        return self.pid_rates[pid]

    def read_open_paths_cached(self, pid: int) -> list[str]:
        now = time.time()
        cached = self.pid_open_paths_cache.get(pid)
        if cached is not None and cached[0] > now:
            return cached[1]

        paths = read_open_paths(pid)
        self.pid_open_paths_cache[pid] = (now + OPEN_PATHS_CACHE_TTL, paths)
        return paths

    def identify_talker(self, pid: int, path: str) -> dict[str, Any]:
        now = time.time()
        cached = self.pid_cache.get(pid)
        if cached and cached[0] > now:
            return cached[1]

        talker = classify_process(pid, self.resolver)
        if talker is None and self.disks:
            container = self.resolver.container_from_path(path, self.disks)
            if container is not None:
                talker = container_talker(container)

        if talker is None:
            talker = {
                "id": f"pid:{pid}",
                "name": f"PID {pid}",
                "kind": "service",
                "icon": {"type": "fa", "value": "fa-cog"},
            }

        self.pid_cache[pid] = (now + PID_CACHE_TTL, talker)
        return talker

    def apply_event_to_session(
        self,
        session: dict[str, Any],
        talker: dict[str, Any],
        pid: int,
        timestamp: float,
        path: str,
    ) -> None:
        state = session["talkers"].setdefault(talker["id"], new_talker_state(talker))
        state["name"] = talker["name"]
        state["kind"] = talker["kind"]
        state["icon"] = talker["icon"]
        state["event_count"] += 1
        state["last_seen"] = max(state["last_seen"], timestamp)
        state["event_pids"].add(pid)
        state["pid_event_counts"][pid] = int(state["pid_event_counts"].get(pid, 0)) + 1
        pid_paths = state["pid_paths"].setdefault(pid, [])
        bounded_unique_prepend(pid_paths, path, PATH_LIMIT)
        bounded_unique_prepend(state["paths"], path, PATH_LIMIT)

    def record_frontdoor_event(self, event: dict[str, Any]) -> None:
        path = canonical_user_path(event["path"])
        talker = self.identify_talker(event["pid"], path)
        if talker["id"] in SUPPRESSED_TALKER_IDS:
            return
        self.frontdoor_activity.setdefault(path, {})[talker["id"]] = {
            "timestamp": event["timestamp"],
            "pid": event["pid"],
            "path": path,
            "talker": talker,
        }

    def resolve_frontdoor_talkers(self, disk_path: str, timestamp: float) -> list[dict[str, Any]]:
        user_path = disk_path_to_user_path(disk_path)
        if not user_path:
            return []

        candidates = [
            item
            for item in self.frontdoor_activity.get(user_path, {}).values()
            if (timestamp - item["timestamp"]) <= PROXY_RESOLVE_WINDOW
        ]
        candidates.sort(key=lambda item: item["timestamp"], reverse=True)
        return candidates

    def resolve_hot_containers_for_paths(self, paths: list[str]) -> list[dict[str, Any]]:
        matches: dict[str, dict[str, Any]] = {}
        for path in paths:
            user_path = canonical_user_path(path) if path.startswith("/mnt/user") else disk_path_to_user_path(path)
            if not user_path:
                continue

            for prefix_len, container in self.resolver.containers_for_user_path(user_path):
                rate = self.container_rates.get(container["id"], {})
                total_bps = float(rate.get("total_bps", 0.0))
                if total_bps < CONTAINER_IO_ACTIVE_BPS:
                    continue
                item = matches.setdefault(
                    container["id"],
                    {
                        "talker": container_talker(container),
                        "pid": int((rate.get("pids") or [0])[0]),
                        "path": user_path,
                        "prefix_len": prefix_len,
                        "total_bps": total_bps,
                    },
                )
                if prefix_len > item["prefix_len"] or total_bps > item["total_bps"]:
                    item["path"] = user_path
                    item["prefix_len"] = prefix_len
                    item["total_bps"] = total_bps

        rows = list(matches.values())
        rows.sort(key=lambda item: (-item["total_bps"], -item["prefix_len"], item["talker"]["name"].lower()))
        return rows

    def record_event(self, disk: dict[str, Any], event: dict[str, Any]) -> None:
        session = self.sessions.setdefault(disk["id"], new_session(event["timestamp"]))
        talker = self.identify_talker(event["pid"], event["path"])
        if talker["id"] == "service:User Shares (shfs)":
            proxies = self.resolve_frontdoor_talkers(event["path"], event["timestamp"])
            if not proxies:
                proxies = self.resolve_hot_containers_for_paths([event["path"]])
            if proxies:
                for proxy in proxies:
                    proxy_pid = proxy.get("pid", 0)
                    self.apply_event_to_session(session, proxy["talker"], proxy_pid or event["pid"], event["timestamp"], proxy["path"])
                return
        self.apply_event_to_session(session, talker, event["pid"], event["timestamp"], event["path"])

    def drain_kernel_events(self) -> None:
        if self.monitor is None:
            return

        for event in self.monitor.drain():
            if event["path"].startswith("/mnt/user/") or event["path"].startswith("/mnt/user0/"):
                self.record_frontdoor_event(event)
                continue
            disk = find_disk_for_path(event["path"], self.disks)
            if disk is None:
                continue
            self.record_event(disk, event)

        if self.monitor.overflowed:
            self.overflow_seen = True

    def build_talkers_for_disk(self, disk: dict[str, Any]) -> list[dict[str, Any]]:
        now = time.time()
        talkers: dict[str, dict[str, Any]] = {}
        session = self.sessions.get(disk["id"])

        if session is not None:
            for session_talker in session["talkers"].values():
                if disk["kind"] != "disk" and (now - session_talker["last_seen"]) > self.recent_window:
                    continue

                if session_talker["id"] in SUPPRESSED_TALKER_IDS:
                    for proxy in self.resolve_hot_containers_for_paths(list(session_talker["paths"])):
                        talkers.setdefault(
                            proxy["talker"]["id"],
                            {
                                "id": proxy["talker"]["id"],
                                "name": proxy["talker"]["name"],
                                "kind": proxy["talker"]["kind"],
                                "icon": proxy["talker"]["icon"],
                                "active_pids": set(),
                                "event_pids": set(),
                                "pid_event_counts": {},
                                "pid_paths": {},
                                "paths": [],
                                "event_count": 0,
                                "last_seen": 0.0,
                            },
                        )
                        state = talkers[proxy["talker"]["id"]]
                        state["name"] = proxy["talker"]["name"]
                        state["kind"] = proxy["talker"]["kind"]
                        state["icon"] = proxy["talker"]["icon"]
                        state["event_count"] += session_talker["event_count"]
                        state["last_seen"] = max(state["last_seen"], session_talker["last_seen"])
                        proxy_pid = int(proxy.get("pid", 0))
                        if proxy_pid > 0:
                            state["event_pids"].add(proxy_pid)
                            state["pid_event_counts"][proxy_pid] = int(state["pid_event_counts"].get(proxy_pid, 0)) + session_talker["event_count"]
                            proxy_paths = state["pid_paths"].setdefault(proxy_pid, [])
                            bounded_unique_prepend(proxy_paths, proxy["path"], PATH_LIMIT)
                        for path in session_talker["paths"]:
                            bounded_unique_prepend(state["paths"], path, PATH_LIMIT)
                    continue

                talkers[session_talker["id"]] = {
                    "id": session_talker["id"],
                    "name": session_talker["name"],
                    "kind": session_talker["kind"],
                    "icon": session_talker["icon"],
                    "active_pids": set(session_talker["active_pids"]),
                    "event_pids": set(session_talker["event_pids"]),
                    "pid_event_counts": dict(session_talker.get("pid_event_counts", {})),
                    "pid_paths": {pid: list(paths) for pid, paths in session_talker.get("pid_paths", {}).items()},
                    "paths": list(session_talker["paths"]),
                    "event_count": session_talker["event_count"],
                    "last_seen": session_talker["last_seen"],
                }

        for row in parse_fuser_output(disk["mount"], self.resolver):
            if row["talker"]["id"] in SUPPRESSED_TALKER_IDS:
                continue
            talker_id = row["talker"]["id"]
            state = talkers.setdefault(
                talker_id,
                {
                    "id": row["talker"]["id"],
                    "name": row["talker"]["name"],
                    "kind": row["talker"]["kind"],
                    "icon": row["talker"]["icon"],
                    "active_pids": set(),
                    "event_pids": set(),
                    "pid_event_counts": {},
                    "pid_paths": {},
                    "paths": [],
                    "event_count": 0,
                    "last_seen": 0.0,
                },
            )
            state["name"] = row["talker"]["name"]
            state["kind"] = row["talker"]["kind"]
            state["icon"] = row["talker"]["icon"]
            state["active_pids"].add(row["pid"])
            state["pid_event_counts"].setdefault(row["pid"], 0)
            state["pid_paths"].setdefault(row["pid"], [])
            state["last_seen"] = max(state["last_seen"], now)

        disk_rate = self.disk_rates.get(disk["id"], {})
        disk_read_bps = float(disk_rate.get("read_bps", 0.0) or 0.0)
        disk_write_bps = float(disk_rate.get("write_bps", 0.0) or 0.0)
        talker_weights: dict[str, float] = {}
        for talker_id, state in talkers.items():
            weight = float(state["event_count"] + len(state["active_pids"]))
            if weight <= 0 and (state["paths"] or state["event_pids"]):
                weight = 1.0
            talker_weights[talker_id] = weight
        total_talker_weight = sum(weight for weight in talker_weights.values() if weight > 0)

        finalized: list[dict[str, Any]] = []
        for state in talkers.values():
            combined_pids = sorted(state["active_pids"] | state["event_pids"])[:PID_LIMIT]
            for pid in combined_pids:
                self.sample_pid_rate(pid, now)

            talker_weight = talker_weights.get(state["id"], 0.0)
            talker_history_share = talker_weight / total_talker_weight if total_talker_weight > 0 else 0.0
            talker_read_bps = disk_read_bps * talker_weight / total_talker_weight if total_talker_weight > 0 else 0.0
            talker_write_bps = disk_write_bps * talker_weight / total_talker_weight if total_talker_weight > 0 else 0.0

            pid_weights: dict[int, float] = {}
            for pid in combined_pids:
                weight = float(int(state["pid_event_counts"].get(pid, 0)) + (1 if pid in state["active_pids"] else 0))
                if weight <= 0:
                    weight = 1.0
                pid_weights[pid] = weight
            total_pid_weight = sum(pid_weights.values())

            pid_details: list[dict[str, Any]] = []
            for pid in combined_pids:
                pid_rate = self.pid_rates.get(pid) or self.sample_pid_rate(pid, now)
                pid_estimated_read_bps = talker_read_bps * pid_weights[pid] / total_pid_weight if total_pid_weight > 0 else 0.0
                pid_estimated_write_bps = talker_write_bps * pid_weights[pid] / total_pid_weight if total_pid_weight > 0 else 0.0
                pid_details.append(
                    {
                        "pid": pid,
                        "active": pid in state["active_pids"],
                        "event_count": int(state["pid_event_counts"].get(pid, 0)),
                        "estimated_read_bps": pid_estimated_read_bps,
                        "estimated_write_bps": pid_estimated_write_bps,
                        "estimated_read_human": human_rate(pid_estimated_read_bps),
                        "estimated_write_human": human_rate(pid_estimated_write_bps),
                        "process_read_bps": float(pid_rate.get("read_bps", 0.0) or 0.0),
                        "process_write_bps": float(pid_rate.get("write_bps", 0.0) or 0.0),
                        "process_read_human": str(pid_rate.get("read_human", "-")),
                        "process_write_human": str(pid_rate.get("write_human", "-")),
                        "recent_paths": list(state["pid_paths"].get(pid, []))[:PATH_LIMIT],
                        "open_paths": self.read_open_paths_cached(pid) if pid in state["active_pids"] else [],
                    }
                )
            pid_details.sort(key=lambda item: (0 if item["active"] else 1, -item["estimated_read_bps"], item["pid"]))

            finalized.append(
                {
                    "id": state["id"],
                    "name": state["name"],
                    "kind": state["kind"],
                    "icon": state["icon"],
                    "active": bool(state["active_pids"]),
                    "active_pids": sorted(state["active_pids"])[:PID_LIMIT],
                    "event_pids": sorted(state["event_pids"])[:PID_LIMIT],
                    "pids": combined_pids,
                    "paths": state["paths"][:PATH_LIMIT],
                    "event_count": state["event_count"],
                    "history_share": talker_history_share,
                    "estimated_read_bps": talker_read_bps,
                    "estimated_write_bps": talker_write_bps,
                    "estimated_read_human": human_rate(talker_read_bps),
                    "estimated_write_human": human_rate(talker_write_bps),
                    "pid_details": pid_details,
                    "last_seen": dt.datetime.fromtimestamp(state["last_seen"], tz=dt.timezone.utc).isoformat() if state["last_seen"] else None,
                    "last_seen_ts": state["last_seen"],
                }
            )

        finalized.sort(key=talker_sort_key)
        return finalized[: self.max_talkers]

    def build_array_summary(self, payload_disks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        summary: dict[str, dict[str, Any]] = {}
        for disk in payload_disks:
            if disk["kind"] != "disk" or disk["status"]["state"] != "spun_up":
                continue
            for talker in disk["talkers"]:
                if talker["id"] in SUPPRESSED_TALKER_IDS:
                    continue
                item = summary.setdefault(
                    talker["id"],
                    {
                        "id": talker["id"],
                        "name": talker["name"],
                        "kind": talker["kind"],
                        "icon": talker["icon"],
                        "disks": [],
                        "disk_count": 0,
                        "active": False,
                        "event_count": 0,
                    },
                )
                if disk["name"] not in item["disks"]:
                    item["disks"].append(disk["name"])
                item["disk_count"] = len(item["disks"])
                item["active"] = item["active"] or talker["active"]
                item["event_count"] += talker.get("event_count", 0)

        rows = list(summary.values())
        rows.sort(key=lambda item: (0 if item["active"] else 1, -item["disk_count"], -item["event_count"], item["name"].lower()))
        return rows

    def classify_mount_source(self, source: str) -> dict[str, str]:
        source = canonical_user_path(source.rstrip("/"))
        pool_mounts = {disk["mount"].rstrip("/"): disk for disk in self.disks if disk["kind"] != "disk"}

        if re.fullmatch(r"/mnt/disk\d+(?:/.*)?", source) or source.startswith("/mnt/user0/"):
            return {
                "category": "array_only",
                "severity": "high",
                "label": "array path",
                "reason": "Direct array path or user0 path; this bypasses pool mounts.",
                "suggestion": "Use a pool mount only if this data should stay on SSD.",
            }

        if source.startswith("/mnt/user/"):
            relative = source.split("/", 3)[3] if len(source.split("/", 3)) > 3 else ""
            share = relative.split("/", 1)[0] if relative else ""
            share_cfg = self.share_configs.get(share, {})
            use_cache = share_cfg.get("shareUseCache", "").lower()
            cache_pool = share_cfg.get("shareCachePool", "") or "cache"

            if use_cache == "only":
                return {
                    "category": "pool_only_user_share",
                    "severity": "low",
                    "label": "pool-only share",
                    "reason": f"Share `{share}` is pool-only on `{cache_pool}`, but the app still goes through `/mnt/user`.",
                    "suggestion": "A direct pool path would avoid shfs/user-share indirection.",
                }

            if use_cache == "no":
                return {
                    "category": "array_only",
                    "severity": "high",
                    "label": "array-only share",
                    "reason": f"Share `{share}` has cache disabled and always hits the array.",
                    "suggestion": "Move this workload to a pool path only if the data belongs on SSD.",
                }

            if use_cache in {"yes", "prefer"}:
                return {
                    "category": "mixed_share",
                    "severity": "medium",
                    "label": "mixed user share",
                    "reason": f"Share `{share}` uses cache mode `{use_cache}` on `{cache_pool}` and can still touch the array.",
                    "suggestion": "If this app should avoid HDD spin-ups, mount the pool path directly instead of `/mnt/user`.",
                }

            return {
                "category": "mixed_share",
                "severity": "medium",
                "label": "user share",
                "reason": f"Share `{share}` is mounted through `/mnt/user`; array access depends on current file placement.",
                "suggestion": "Use a direct pool path when you want predictable SSD-only access.",
            }

        for mount, disk in pool_mounts.items():
            if source == mount or source.startswith(mount + "/"):
                return {
                    "category": "pool_mount",
                    "severity": "safe",
                    "label": "pool mount",
                    "reason": f"Direct pool path on `{disk['name']}`.",
                    "suggestion": "",
                }

        return {
            "category": "other",
            "severity": "safe",
            "label": "other path",
            "reason": "Not an Unraid array or pool path.",
            "suggestion": "",
        }

    def build_mount_audit(self) -> list[dict[str, Any]]:
        severity_rank = {"high": 0, "medium": 1, "low": 2, "safe": 3}
        rows: list[dict[str, Any]] = []

        for container in self.resolver.load_containers().values():
            flagged_mounts: list[dict[str, str]] = []
            highest = "safe"
            for mount in container.get("mount_details", []):
                classification = self.classify_mount_source(mount["source"])
                if classification["severity"] == "safe":
                    continue
                flagged_mounts.append(
                    {
                        "source": mount["source"],
                        "target": mount["target"] or "-",
                        "label": classification["label"],
                        "severity": classification["severity"],
                        "reason": classification["reason"],
                        "suggestion": classification["suggestion"],
                    }
                )
                if severity_rank[classification["severity"]] < severity_rank[highest]:
                    highest = classification["severity"]

            if not flagged_mounts:
                continue

            flagged_mounts.sort(key=lambda item: (severity_rank[item["severity"]], item["source"]))
            rows.append(
                {
                    "id": f"container:{container['id']}",
                    "name": container["name"],
                    "kind": "container",
                    "icon": {"type": "image", "value": container["icon_url"]} if container["icon_url"] else {"type": "fa", "value": "fa-cube"},
                    "status": container.get("status", ""),
                    "config_url": f"/Docker/UpdateContainer?xmlTemplate={urllib.parse.quote('edit:' + container['template_path'])}" if container.get("template_path") else "",
                    "severity": highest,
                    "mounts": flagged_mounts[:4],
                }
            )

        rows.sort(key=lambda item: (severity_rank[item["severity"]], item["name"].lower()))
        return rows

    def history_talkers_for_disk(self, disk_id: str, talkers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        history_talkers = [
            {
                "id": talker["id"],
                "name": talker["name"],
                "kind": talker["kind"],
                "icon": talker.get("icon", {"type": "fa", "value": "fa-cog"}),
                "history_share": float(talker.get("history_share", 0.0) or 0.0),
            }
            for talker in talkers
        ]
        if history_talkers:
            self.history_talker_cache[disk_id] = history_talkers
            return history_talkers
        return list(self.history_talker_cache.get(disk_id, []))

    def build_payload(self) -> dict[str, Any]:
        self.prune_pid_cache()
        self.prune_frontdoor_activity()
        payload_disks: list[dict[str, Any]] = []

        for disk in self.disks:
            talkers = []
            history_talkers = []
            if disk["status"]["state"] != "spun_down" or disk["kind"] != "disk":
                talkers = self.build_talkers_for_disk(disk)
                if disk["kind"] == "disk":
                    history_talkers = self.history_talkers_for_disk(str(disk["id"]), talkers)

            payload_disks.append(
                {
                    "id": disk["id"],
                    "name": disk["name"],
                    "mount": disk["mount"],
                    "device": disk["device"],
                    "fstype": disk["fstype"],
                    "kind": disk["kind"],
                    "status": disk["status"],
                    "rates": self.disk_rates.get(
                        disk["id"],
                        {"read_bps": 0.0, "write_bps": 0.0, "read_human": "-", "write_human": "-"},
                    ),
                    "talkers": talkers,
                    "history_talkers": history_talkers,
                }
            )

        warnings: list[str] = []
        if self.monitor is None and self.monitor_error:
            warnings.append(f"fanotify unavailable: {self.monitor_error}")
        if self.overflow_seen:
            warnings.append("fanotify queue overflow detected")

        return {
            "ok": True,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "collector_mode": "fanotify+fuser",
            "mount_audit": self.build_mount_audit(),
            "array_talkers": self.build_array_summary(payload_disks),
            "warnings": warnings,
            "disks": payload_disks,
        }


def write_state(path: str, payload: dict[str, Any]) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=str(target.parent), delete=False, encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, target)


def attach_history(payload: dict[str, Any], history_store: HistoryStore | None, disks: list[dict[str, Any]]) -> dict[str, Any]:
    payload["history"] = history_store.build_summary(disks) if history_store is not None else {"default_period": "daily", "periods": {}}
    return payload


def collect_once(state_file: str, recent_window: int, max_talkers: int, history_store: HistoryStore | None = None) -> dict[str, Any]:
    pathlib.Path(os.path.dirname(LOCK_PATH)).mkdir(parents=True, exist_ok=True)
    with open(LOCK_PATH, "w", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if os.path.exists(state_file):
                with open(state_file, "r", encoding="utf-8") as existing:
                    return json.load(existing)
            raise

        collector = DiskTalkersCollector(recent_window=recent_window, max_talkers=max_talkers)
        collector.refresh_inventory_if_needed(force=True)
        collector.drain_kernel_events()
        payload = attach_history(collector.build_payload(), history_store, collector.disks)
        write_state(state_file, payload)
        return payload


def run_daemon(
    state_file: str,
    recent_window: int,
    max_talkers: int,
    publish_interval: int,
    history_store: HistoryStore | None = None,
    history_sample_interval: int = DEFAULT_HISTORY_SAMPLE_INTERVAL,
) -> None:
    pathlib.Path(os.path.dirname(LOCK_PATH)).mkdir(parents=True, exist_ok=True)
    with open(LOCK_PATH, "w", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX)

        collector = DiskTalkersCollector(recent_window=recent_window, max_talkers=max_talkers)
        next_publish = 0.0
        next_history_sample = 0.0
        last_history_sample = 0.0

        while True:
            try:
                collector.refresh_inventory_if_needed()
                collector.drain_kernel_events()

                now = time.time()
                if now >= next_publish:
                    payload = collector.build_payload()
                    if history_store is not None and now >= next_history_sample:
                        sample_seconds = history_sample_interval if last_history_sample <= 0 else max(1.0, min(now - last_history_sample, history_sample_interval * 2))
                        history_store.record_payload(payload, sample_seconds=sample_seconds, timestamp=now)
                        last_history_sample = now
                        next_history_sample = now + float(history_sample_interval)
                    payload = attach_history(payload, history_store, collector.disks)
                    write_state(state_file, payload)
                    next_publish = now + float(publish_interval)
            except Exception as exc:  # pragma: no cover - runtime guard on Unraid host
                error_payload = {
                    "ok": False,
                    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "error": str(exc),
                    "disks": [],
                }
                write_state(state_file, error_payload)
            time.sleep(LOOP_SLEEP_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect exact Unraid disk talkers.")
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--state-file", default=DEFAULT_STATE_PATH)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--recent-window", type=int, default=300)
    parser.add_argument("--max-talkers", type=int, default=5)
    parser.add_argument("--history-file", default=DEFAULT_HISTORY_PATH)
    parser.add_argument("--history-sample-interval", type=int, default=DEFAULT_HISTORY_SAMPLE_INTERVAL)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    cfg = read_cfg(args.config)
    publish_interval = max(1, int(cfg.get("POLL_INTERVAL", args.interval)))
    recent_window = max(30, int(cfg.get("RECENT_WINDOW", args.recent_window)))
    max_talkers = max(1, int(cfg.get("MAX_TALKERS", args.max_talkers)))
    state_file = cfg.get("STATE_FILE", args.state_file)
    history_file = cfg.get("HISTORY_FILE", args.history_file)
    history_sample_interval = max(60, int(cfg.get("HISTORY_SAMPLE_INTERVAL", args.history_sample_interval)))
    power_min_w = max(0.0, float(cfg.get("HDD_POWER_MIN_W", DEFAULT_HDD_POWER_MIN_W)))
    power_max_w = max(power_min_w, float(cfg.get("HDD_POWER_MAX_W", DEFAULT_HDD_POWER_MAX_W)))
    tariff_mode = cfg.get("ELECTRICITY_TARIFF_MODE", DEFAULT_ELECTRICITY_TARIFF_MODE).lower()
    currency_symbol = cfg.get("CURRENCY_SYMBOL", DEFAULT_CURRENCY_SYMBOL)
    single_rate = max(0.0, float(cfg.get("ELECTRICITY_SINGLE_RATE", DEFAULT_ELECTRICITY_SINGLE_RATE)))
    peak_rate = max(0.0, float(cfg.get("ELECTRICITY_PEAK_RATE", DEFAULT_ELECTRICITY_PEAK_RATE)))
    offpeak_rate = max(0.0, float(cfg.get("ELECTRICITY_OFFPEAK_RATE", DEFAULT_ELECTRICITY_OFFPEAK_RATE)))
    offpeak_start = cfg.get("ELECTRICITY_OFFPEAK_START", DEFAULT_ELECTRICITY_OFFPEAK_START)
    offpeak_end = cfg.get("ELECTRICITY_OFFPEAK_END", DEFAULT_ELECTRICITY_OFFPEAK_END)
    history_store = HistoryStore(
        history_file,
        power_min_w=power_min_w,
        power_max_w=power_max_w,
        tariff_mode=tariff_mode,
        single_rate=single_rate,
        peak_rate=peak_rate,
        offpeak_rate=offpeak_rate,
        offpeak_start=offpeak_start,
        offpeak_end=offpeak_end,
        currency_symbol=currency_symbol,
    )

    if args.once:
        payload = collect_once(state_file, recent_window, max_talkers, history_store=history_store)
        print(json.dumps(payload, indent=2))
        return 0

    run_daemon(
        state_file,
        recent_window,
        max_talkers,
        publish_interval,
        history_store=history_store,
        history_sample_interval=history_sample_interval,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
