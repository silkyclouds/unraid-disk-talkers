#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_NAME="disk.talkers"
VERSION="$(tr -d '\r\n' < "$SCRIPT_DIR/VERSION")"
ENV_FILE="${1:-$SCRIPT_DIR/release.env}"
TEMPLATE_FILE="$SCRIPT_DIR/disk.talkers.plg.template"
PLUGIN_SRC="$SCRIPT_DIR/source/usr/local/emhttp/plugins/$PLUGIN_NAME"
DEFAULT_CFG="$SCRIPT_DIR/source/boot/config/plugins/$PLUGIN_NAME/$PLUGIN_NAME.cfg"
DOINST_TEMPLATE="$SCRIPT_DIR/packaging/doinst.sh"
SLACK_DESC_TEMPLATE="$SCRIPT_DIR/packaging/slack-desc"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing release env file: $ENV_FILE"
  echo "Start from: $SCRIPT_DIR/release.env.example"
  exit 1
fi

# shellcheck source=/dev/null
source "$ENV_FILE"

: "${GITHUB_REPO:?GITHUB_REPO is required}"
: "${SUPPORT_URL:?SUPPORT_URL is required}"

GITHUB_BRANCH="${GITHUB_BRANCH:-main}"
RELEASE_TAG="${RELEASE_TAG:-$VERSION}"
PLUGIN_AUTHOR="${PLUGIN_AUTHOR:-meaning}"
WRITE_ROOT_MANIFEST="${WRITE_ROOT_MANIFEST:-0}"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/dist}"
PACKAGE_NAME="${PLUGIN_NAME}-package-${VERSION}-x86_64-1"
PACKAGE_FILE="${PACKAGE_NAME}.txz"
PACKAGE_URL="https://github.com/${GITHUB_REPO}/releases/download/${RELEASE_TAG}/${PACKAGE_FILE}"
PLUGIN_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${GITHUB_BRANCH}/${PLUGIN_NAME}.plg"
PROJECT_URL="https://github.com/${GITHUB_REPO}"
BUILD_ROOT="$SCRIPT_DIR/.build/${PACKAGE_NAME}"
PKG_ROOT="$BUILD_ROOT/pkgroot"
INSTALL_ROOT="$PKG_ROOT/install"
PACKAGE_OUTPUT="$OUTPUT_DIR/$PACKAGE_FILE"
MANIFEST_OUTPUT="$OUTPUT_DIR/${PLUGIN_NAME}.plg"
SLACK_DESC_OUTPUT="$INSTALL_ROOT/slack-desc"

if [[ ! -d "$PLUGIN_SRC" ]]; then
  echo "Missing plugin source tree: $PLUGIN_SRC"
  exit 1
fi

if [[ ! -f "$DEFAULT_CFG" ]]; then
  echo "Missing default config: $DEFAULT_CFG"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
rm -rf "$BUILD_ROOT"
mkdir -p "$PKG_ROOT/usr/local/emhttp/plugins/$PLUGIN_NAME"
mkdir -p "$INSTALL_ROOT"

rsync -a \
  --delete \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  "$PLUGIN_SRC/" \
  "$PKG_ROOT/usr/local/emhttp/plugins/$PLUGIN_NAME/"

mkdir -p "$PKG_ROOT/usr/local/emhttp/plugins/$PLUGIN_NAME/defaults"
cp "$DEFAULT_CFG" \
  "$PKG_ROOT/usr/local/emhttp/plugins/$PLUGIN_NAME/defaults/$PLUGIN_NAME.cfg.default"
cp "$DOINST_TEMPLATE" "$INSTALL_ROOT/doinst.sh"
chmod 755 "$INSTALL_ROOT/doinst.sh"

PROJECT_URL="$PROJECT_URL" python3 - "$SLACK_DESC_TEMPLATE" "$SLACK_DESC_OUTPUT" <<'PY'
import os
import sys
from pathlib import Path

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
content = template_path.read_text().replace("__PROJECT_URL__", os.environ["PROJECT_URL"])
output_path.write_text(content)
PY

python3 - "$PKG_ROOT" "$PACKAGE_OUTPUT" <<'PY'
import os
import stat
import sys
import tarfile
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])

with tarfile.open(dst, "w:xz") as archive:
    for path in sorted(src.rglob("*")):
        relative = path.relative_to(src)
        tarinfo = archive.gettarinfo(str(path), arcname=str(relative))
        tarinfo.uid = 0
        tarinfo.gid = 0
        tarinfo.uname = "root"
        tarinfo.gname = "root"
        if path.is_file():
            with path.open("rb") as handle:
                archive.addfile(tarinfo, handle)
        else:
            archive.addfile(tarinfo)
PY

PACKAGE_MD5="$(python3 - "$PACKAGE_OUTPUT" <<'PY'
import hashlib
import sys
path = sys.argv[1]
digest = hashlib.md5()
with open(path, "rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
)"

AUTHOR="$PLUGIN_AUTHOR" \
VERSION="$VERSION" \
PACKAGE_MD5="$PACKAGE_MD5" \
PLUGIN_URL="$PLUGIN_URL" \
PACKAGE_URL="$PACKAGE_URL" \
SUPPORT_URL="$SUPPORT_URL" \
python3 - "$TEMPLATE_FILE" "$MANIFEST_OUTPUT" <<'PY'
import os
import sys
from pathlib import Path

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
content = template_path.read_text()
for key in (
    "AUTHOR",
    "VERSION",
    "PACKAGE_MD5",
    "PLUGIN_URL",
    "PACKAGE_URL",
    "SUPPORT_URL",
):
    content = content.replace(f"__{key}__", os.environ[key])
output_path.write_text(content)
PY

if [[ "$WRITE_ROOT_MANIFEST" == "1" ]]; then
  cp "$MANIFEST_OUTPUT" "$SCRIPT_DIR/${PLUGIN_NAME}.plg"
fi

echo ""
echo "Build complete."
echo "Manifest: $MANIFEST_OUTPUT"
echo "Package:  $PACKAGE_OUTPUT"
echo "pluginURL: $PLUGIN_URL"
echo "packageURL: $PACKAGE_URL"
echo "md5: $PACKAGE_MD5"
