#!/usr/bin/env bash
#
# Build a portable Linux tarball for tg-searcher.
#
# Usage:
#   ./scripts/package-linux.sh
#   ./scripts/package-linux.sh --no-bundle-libs      # require system OpenCV
#   ./scripts/package-linux.sh --skip-build --output dist/linux
#
# Bundling copies the non-glibc shared libraries reported by ldd (OpenCV,
# libstdc++, libjpeg, ...) into lib/ and creates tg-searcher.sh, which sets
# LD_LIBRARY_PATH to that directory. glibc itself is never bundled.
#
# Target machine requirements (bundled mode):
#   * a glibc-based distribution not older than the build machine;
#   * x86_64 or aarch64 matching the build architecture.
#
# Prerequisites on the build machine:
#   * Rust, OpenCV development package, Clang/libclang (see docs/build.md);
#   * ldd (glibc), tar, gzip, sha256sum.

set -euo pipefail

SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
cd "$ROOT_DIR"

ARCH=$(uname -m)
OUTPUT_DIR="dist/linux-${ARCH}"
BUNDLE_LIBS=1
SKIP_BUILD=0
BINARY="target/release/tg-searcher"

usage() {
    cat >&2 <<'EOF'
Usage: ./scripts/package-linux.sh [options]

Options:
  --output DIR       Output directory (default: dist/linux-<arch>)
  --no-bundle-libs   Do not bundle OpenCV/libstdc++ dependencies
  --skip-build       Reuse an existing target/release/tg-searcher
  -h, --help         Show this help

Environment: standard Cargo/OpenCV/libclang variables (see docs/build.md).
EOF
    exit "${1:-1}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --output)
            [ $# -ge 2 ] || { echo "error: --output needs a directory" >&2; exit 1; }
            OUTPUT_DIR=$2
            shift 2
            ;;
        --no-bundle-libs)
            BUNDLE_LIBS=0
            shift
            ;;
        --skip-build)
            SKIP_BUILD=1
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

if [ "$SKIP_BUILD" -eq 0 ]; then
    echo "==> cargo build --release --locked"
    cargo build --release --locked
fi

if [ ! -x "$BINARY" ]; then
    echo "error: $BINARY not found; build it first or drop --skip-build" >&2
    exit 1
fi

STAGE_NAME="tg-searcher-linux-${ARCH}"
STAGE_DIR="$OUTPUT_DIR/$STAGE_NAME"
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR/lib"

echo "==> staging $STAGE_DIR"
cp "$BINARY" "$STAGE_DIR/tg-searcher"
cp searcher.sample.yaml "$STAGE_DIR/"
cp .env.example "$STAGE_DIR/"
cp README.md "$STAGE_DIR/README.md"
cp LICENSE "$STAGE_DIR/LICENSE"
cp -r docs "$STAGE_DIR/docs"

if [ "$BUNDLE_LIBS" -eq 1 ]; then
    echo "==> collecting shared libraries with ldd"
    if ! command -v ldd >/dev/null 2>&1; then
        echo "error: ldd is required for --bundle-libs" >&2
        exit 1
    fi
    missing=0
    while IFS= read -r line; do
        case "$line" in
            *"=> not found"*)
                echo "error: missing dependency: $line" >&2
                missing=1
                continue
                ;;
            *"=> /"*)
                lib=${line#*=> }
                lib=${lib%% (*}
                ;;
            /*)
                lib=${line%% (*}
                ;;
            *)
                continue
                ;;
        esac
        base=$(basename "$lib")
        case "$base" in
            libc.so*|libm.so*|libpthread.so*|libdl.so*|librt.so*|libresolv.so*|libutil.so*|ld-linux*|linux-vdso*|libanl.so*|libnsl.so*)
                continue
                ;;
        esac
        cp -L "$lib" "$STAGE_DIR/lib/$base"
    done < <(ldd "$STAGE_DIR/tg-searcher")
    if [ "$missing" -ne 0 ]; then
        echo "error: unresolved shared libraries; install OpenCV/Clang runtime packages" >&2
        exit 1
    fi

    # Set an rpath when patchelf is available. tg-searcher.sh works either way.
    if command -v patchelf >/dev/null 2>&1; then
        patchelf --set-rpath "\$ORIGIN/lib" "$STAGE_DIR/tg-searcher"
        echo "==> rpath set to \$ORIGIN/lib"
    else
        echo "warning: patchelf not found; use tg-searcher.sh to run the bundle" >&2
    fi
fi

cat > "$STAGE_DIR/tg-searcher.sh" <<'EOF'
#!/usr/bin/env sh
# Launcher for the bundled tg-searcher build.
set -eu
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -d "$DIR/lib" ]; then
    LD_LIBRARY_PATH="$DIR/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export LD_LIBRARY_PATH
fi
exec "$DIR/tg-searcher" "$@"
EOF
chmod +x "$STAGE_DIR/tg-searcher" "$STAGE_DIR/tg-searcher.sh"

cat > "$STAGE_DIR/README-portable.txt" <<'EOF'
TG Searcher portable build (Linux)

1. Copy searcher.sample.yaml to searcher.yaml and edit it.
2. Export the required environment variables (TG_API_ID, TG_API_HASH,
   TG_PHONE, TG_ADMIN_ID, TG_BOT_TOKEN), or put literal values in the YAML.
   ${VAR} placeholders are expanded at startup; missing variables are an error.
3. Run:
     ./tg-searcher.sh -f searcher.yaml

The first run is interactive: enter the Telegram login code (and 2FA password
if enabled). The session is stored under runtime_dir.

If this tarball was built with bundled libraries, lib/ contains the non-glibc
dependencies (OpenCV, libstdc++, image codecs). Use ./tg-searcher.sh so
LD_LIBRARY_PATH points at lib/. If no lib/ directory exists, install OpenCV on
the target system and run ./tg-searcher directly.
EOF

TARBALL="$OUTPUT_DIR/tg-searcher-linux-${ARCH}.tar.gz"
echo "==> creating $TARBALL"
rm -f "$TARBALL"
tar -C "$OUTPUT_DIR" -czf "$TARBALL" "$STAGE_NAME"

echo "==> checksums"
( cd "$OUTPUT_DIR" && sha256sum "tg-searcher-linux-${ARCH}.tar.gz" > "tg-searcher-linux-${ARCH}.tar.gz.sha256" )
cat "$OUTPUT_DIR/tg-searcher-linux-${ARCH}.tar.gz.sha256"

echo
echo "Portable Linux build ready:"
ls -lh "$TARBALL"
