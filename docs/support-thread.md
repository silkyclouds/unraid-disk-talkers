# [Plugin] Disk Talkers

Disk Talkers adds a dedicated Unraid page to show what is actively keeping array
disks and pools spun up.

It correlates:

- live disk state and throughput,
- active processes and containers,
- user-share access through `shfs`,
- Docker mounts that point to array or mixed user-share paths,
- historical array usage across daily, weekly, monthly, and yearly windows.

## Highlights

- Per-disk view of current users, including Docker containers, VMs, and host
  services.
- Array-wide summary of the apps currently keeping HDDs spun up.
- Historical usage charts with attribution percentages and estimated spin-up
  time.
- Estimated power and electricity cost from configurable HDD watt ranges and
  tariffs.
- Mount-audit drawer to flag containers using `/mnt/user` or array-backed paths
  where a direct pool/cache path may avoid unnecessary spin-ups.

## Install

Until Community Applications listing is live, the plugin can already be tested
with the direct plugin URL.

### Prerequisite

Disk Talkers needs `Python 3 for UNRAID (6.11+)` on the Unraid host.

If Python is not installed yet, Disk Talkers will remain installed and show a
dependency-required state in the WebUI until the Python plugin is installed.

### WebUI method

1. Open your Unraid WebUI.
2. Go to `Plugins`.
3. Click `Install Plugin`.
4. Paste:

`https://raw.githubusercontent.com/silkyclouds/unraid-disk-talkers/main/disk.talkers.plg`

### CLI method

`installplg https://raw.githubusercontent.com/silkyclouds/unraid-disk-talkers/main/disk.talkers.plg`

## Notes

- Attribution is best-effort. Extremely short-lived activity can still appear as
  unattributed residual activity in history views.
- Energy and cost figures are estimates derived from historical HDD up-time and
  configured watt/tariff settings.

## Troubleshooting

If `/var/log/disk.talkers.log` contains:

`nohup: failed to run command 'python3': No such file or directory`

install `Python 3 for UNRAID (6.11+)` from Community Applications, then refresh
the Disk Talkers page.

## Support

Use this thread for bug reports, screenshots, attribution edge cases, and CA
listing feedback.
