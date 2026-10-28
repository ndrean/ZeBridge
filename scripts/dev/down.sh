#!/usr/bin/env bash
# Stop the containerized scenario stack. The data stays in the Docker volumes, so the next
# up.sh resumes from where this left off (like `docker compose down`).
#
#   scripts/dev/down.sh            # stop and remove the containers, keep the volumes and the keys
#   scripts/dev/down.sh --clean    # also remove the volumes, the minted keys and the credentials
#
# --clean is destructive and deliberately not the default: it deletes the database and the
# JetStream data, and scripts/dev/state/ with the nsc keys that sign every credential (the
# .creds files in scripts/native/creds/ cannot be reproduced without them, so they go too).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ "${1:-}" = "--clean" ]; then
  docker compose -f "$HERE/compose.yml" --profile s3 down -v --remove-orphans
  echo "[dev] removing scripts/dev/state/ and scripts/native/creds/ — the next up.sh mints new ones"
  rm -rf "$HERE/state" "$ROOT/scripts/native/creds"
else
  docker compose -f "$HERE/compose.yml" --profile s3 down --remove-orphans
fi
