#!/bin/sh
set +e

chmod +x /usr/local/emhttp/plugins/disk.talkers/scripts/collector.py 2>/dev/null || true
chmod +x /usr/local/emhttp/plugins/disk.talkers/scripts/rc.disk.talkers 2>/dev/null || true
chmod +x /usr/local/emhttp/plugins/disk.talkers/event/started 2>/dev/null || true
chmod +x /usr/local/emhttp/plugins/disk.talkers/event/stopping_svcs 2>/dev/null || true

mkdir -p /tmp/disk.talkers
