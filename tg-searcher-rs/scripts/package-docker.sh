#!/usr/bin/env bash
#
# Build the Docker image and export it as a compressed tar archive.
#
# Usage:
#   ./scripts/package-docker.sh
#   ./scripts/package-docker.sh --image tg-searcher:1.0 --output dist/tg-searcher-docker.tar.gz
#   PLATFORM=linux/arm64 ./scripts/package-docker.sh
#
# Requirements: Docker (with buildx for --platform), gzip, sha256sum.
# The archive can be loaded elsewhere with:
#   docker load -i tg-searcher-docker-image.tar.gz

set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
cd "$ROOT_DIR"

IMAGE="tg-searcher:latest"
OUTPUT="dist/tg-searcher-docker-image.tar.gz"
PLATFORM="${PLATFORM:-}"
VERIFY=1

usage() {
    cat >&2 <<'EOF'
Usage: ./scripts/package-docker.sh [options]

Options:
  --image TAG        Image tag (default: tg-searcher:latest)
  --output PATH      Archive path (default: dist/tg-searcher-docker-image.tar.gz)
  --no-verify        Skip the in-image --version smoke test
  -h, --help         Show this help

Environment:
  PLATFORM=linux/arm64   Cross-build with docker buildx (requires buildx)
EOF
    exit "${1:-1}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --image)
            [ $# -ge 2 ] || { echo "error: --image needs a tag" >&2; exit 1; }
            IMAGE=$2
            shift 2
            ;;
        --output)
            [ $# -ge 2 ] || { echo "error: --output needs a path" >&2; exit 1; }
            OUTPUT=$2
            shift 2
            ;;
        --no-verify)
            VERIFY=0
            shift
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage
            ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker is not installed or not in PATH" >&2
    exit 1
fi

echo "==> building Docker image $IMAGE"
if [ -n "$PLATFORM" ]; then
    if docker buildx version >/dev/null 2>&1; then
        docker buildx build --platform "$PLATFORM" --tag "$IMAGE" --load .
    else
        echo "error: PLATFORM was set but docker buildx is unavailable" >&2
        exit 1
    fi
else
    docker build --tag "$IMAGE" .
fi

if [ "$VERIFY" -eq 1 ]; then
    echo "==> smoke-testing image"
    docker run --rm "$IMAGE" --version
fi

mkdir -p "$(dirname -- "$OUTPUT")"
echo "==> docker save -> $OUTPUT"
docker save "$IMAGE" | gzip -9 > "$OUTPUT"

echo "==> checksum"
sha256sum "$OUTPUT"
echo
echo "Docker image archive ready:"
ls -lh "$OUTPUT"
echo
echo "Load on another host with:"
echo "  docker load -i $(basename -- "$OUTPUT")"
echo "Run with:"
echo "  docker run --rm -it -v \$PWD/config:/app/config -v \$PWD/data:/app/data --env-file .env $IMAGE"
