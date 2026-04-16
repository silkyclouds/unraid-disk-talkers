# Disk Talkers

[![Install on Unraid](https://img.shields.io/badge/Unraid-Install%20Plugin-F15A24?style=for-the-badge)](#install-on-unraid)
[![GitHub release](https://img.shields.io/github/v/release/silkyclouds/unraid-disk-talkers?display_name=tag&style=for-the-badge)](https://github.com/silkyclouds/unraid-disk-talkers/releases)
[![Validate release packaging](https://img.shields.io/github/actions/workflow/status/silkyclouds/unraid-disk-talkers/validate-release.yml?branch=main&style=for-the-badge)](https://github.com/silkyclouds/unraid-disk-talkers/actions/workflows/validate-release.yml)

Unraid plugin that adds a dedicated WebUI page to show which containers, VMs,
or host services are probably keeping each array disk or pool busy.

Disk Talkers is already testable before Community Apps listing by installing the
plugin URL directly from Unraid.

## Install On Unraid

### WebUI method

1. Open your Unraid WebUI.
2. Go to `Plugins`.
3. Click `Install Plugin`.
4. Paste this URL:

```text
https://raw.githubusercontent.com/silkyclouds/unraid-disk-talkers/main/disk.talkers.plg
```

5. Confirm the installation.

### CLI method

```bash
installplg https://raw.githubusercontent.com/silkyclouds/unraid-disk-talkers/main/disk.talkers.plg
```

The plugin page then appears in Unraid under `System Information -> Disk
Talkers`.

## Layout

- `disk.talkers.local.plg`: local development manifest that installs directly
  from the synced `source/` tree on your Unraid host.
- `disk.talkers.plg.template`: public release manifest template used to produce
  the distributable `.plg`.
- `build-release.sh`: builds the public `.txz` package and generated `.plg`
  manifest for Community Apps publication.
- `release.env.example`: variables required for the GitHub/raw/support URLs used
  by the public release manifest.
- `packaging/`: Slackware package metadata used when generating the `.txz`.
- `source/`: files copied to `/boot/config/plugins/disk.talkers/source` on the
  Unraid host.
- `deploy-to-unraid.sh`: sync the local source tree to the server and install or
  refresh the plugin.

## Runtime model

- A lightweight Python collector runs on the Unraid host every 5 seconds.
- It combines:
  - `/var/log/file.activity.log` for recent host-side file paths.
  - `fuser -vm /mnt/<mount>` for processes that still hold a disk or pool open.
  - Docker inspect/template metadata to map PIDs to container names and icons.
- The collector writes `/tmp/disk.talkers/state.json`.
- The WebUI page fetches `/plugins/disk.talkers/api/state.php`.

## Deploy

```bash
./deploy-to-unraid.sh
```

Optional target:

```bash
./deploy-to-unraid.sh root@192.168.3.2
```

After deploy, the page is available in Unraid under `System Information ->
Disk Talkers`.

## Build A Community Apps Release

1. Copy `release.env.example` to `release.env`.
2. Fill in:
   - `GITHUB_REPO` with the public repo that will host the manifest and release,
   - `SUPPORT_URL` with the public Unraid forum support thread,
   - optionally `RELEASE_TAG`, `GITHUB_BRANCH`, and `WRITE_ROOT_MANIFEST`.
3. Run:

```bash
./build-release.sh
```

4. The build produces:
   - `dist/disk.talkers.plg`
   - `dist/disk.talkers-package-<version>-x86_64-1.txz`
5. Upload the `.txz` to the GitHub release whose tag matches `RELEASE_TAG`.
6. Publish the generated `.plg` at the repo path expected by `pluginURL`.
   - easiest path: set `WRITE_ROOT_MANIFEST="1"` in `release.env`, then commit
     the generated `disk.talkers.plg` at the repo root before tagging the
     release.
7. Create the support thread on the Unraid forums.
8. Submit the plugin to Community Apps with the public `.plg` URL.

## Notes

- The generated package is the CA-ready artifact. The local manifest remains
  separate so local development does not depend on GitHub Releases.
- Publishing cannot be fully automated from this repo alone because Community
  Apps still needs a public support thread and a public `.plg` URL.
- The current public install path is the direct plugin URL above. Community
  Apps indexing will be added after the plugin support thread is in place.
