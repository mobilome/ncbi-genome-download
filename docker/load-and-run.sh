#!/usr/bin/env sh
set -eu

IMAGE_TAR="${1:-ngm-py3.12.img}"
PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"

if [ ! -f "$IMAGE_TAR" ]; then
  echo "ERROR: image tar not found: $IMAGE_TAR" >&2
  exit 1
fi

cd "$PROJECT_ROOT"

echo "Loading image from: $IMAGE_TAR"
docker load -i "$IMAGE_TAR"

echo "Done"
docker images
