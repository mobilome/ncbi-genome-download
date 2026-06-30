#!/usr/bin/env sh
set -eu

IMAGE_NAME="${IMAGE_NAME:-ngm:py3.12}"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
OUTPUT_TAR="${OUTPUT_TAR:-$SCRIPT_DIR/ngm-py3.12.img}"

echo "[1/2] Building image: $IMAGE_NAME"
echo "      Build context : $SCRIPT_DIR"
docker build -f "$SCRIPT_DIR/runtime.Dockerfile" -t "$IMAGE_NAME" "$SCRIPT_DIR"

echo "[2/2] Saving image to: $OUTPUT_TAR"
docker save -o "$OUTPUT_TAR" "$IMAGE_NAME"

echo "Done. Saved: $OUTPUT_TAR"
echo "Copy the tar to the target server, then run:"
echo "  docker load -i ngm-py3.12.img"
