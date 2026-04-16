#!/bin/bash
set -euo pipefail

REMOTE="${1:-root@192.168.3.2}"
ROOT="/boot/config/plugins/disk.talkers"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ssh "$REMOTE" "mkdir -p '${ROOT}/source'"

rsync -az --delete \
  --no-owner \
  --no-group \
  --omit-dir-times \
  --exclude '__pycache__' \
  "$SCRIPT_DIR/source/" \
  "${REMOTE}:${ROOT}/source/"

scp "$SCRIPT_DIR/disk.talkers.local.plg" "${REMOTE}:/boot/config/plugins/disk.talkers.plg"

ssh "$REMOTE" "
  mkdir -p '$ROOT' &&
  mkdir -p /usr/local/emhttp/plugins &&
  if [ ! -f '$ROOT/disk.talkers.cfg' ] && [ -f '$ROOT/source/boot/config/plugins/disk.talkers/disk.talkers.cfg' ]; then
    cp '$ROOT/source/boot/config/plugins/disk.talkers/disk.talkers.cfg' '$ROOT/disk.talkers.cfg';
  fi &&
  rm -rf /usr/local/emhttp/plugins/disk.talkers &&
  cp -R '$ROOT/source/usr/local/emhttp/plugins/disk.talkers' /usr/local/emhttp/plugins/ &&
  chmod +x /usr/local/emhttp/plugins/disk.talkers/event/started /usr/local/emhttp/plugins/disk.talkers/event/stopping_svcs /usr/local/emhttp/plugins/disk.talkers/scripts/collector.py /usr/local/emhttp/plugins/disk.talkers/scripts/rc.disk.talkers &&
  mkdir -p /tmp/disk.talkers &&
  /usr/local/emhttp/plugins/disk.talkers/scripts/rc.disk.talkers restart >/dev/null 2>&1 || true &&
  /usr/local/sbin/plugin install /boot/config/plugins/disk.talkers.plg >/dev/null 2>&1 || true
"

echo "Disk Talkers deployed to ${REMOTE}"
