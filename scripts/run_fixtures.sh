#!/bin/sh
# Run the image on every fixture with scorer-like flags, then diff each against expected/.
# Usage: scripts/run_fixtures.sh FIXTURES_DIR [IMAGE]
set -eu
FIX=$(cd "$1" && pwd)
IMAGE="${2:-shopping-sessions:latest}"
OUT=$(mktemp -d)
chmod 777 "$OUT"
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges --user 1000:1000 \
  -v "$FIX":/fx:ro -v "$OUT":/out --entrypoint sh "$IMAGE" -c '
    for d in /fx/*/; do
      c=$(basename "$d"); [ -d "$d/input" ] || continue
      mkdir -p /out/$c
      DATA_DIR=$d/input OUTPUT_DIR=/out/$c SCRATCH_DIR=/tmp NCPU=8 /app/pipeline 2>/dev/null || echo "$c: exit $?"
    done'
uv run --quiet --with polars python "$(dirname "$0")/check.py" --fixtures "$OUT" "$FIX"
