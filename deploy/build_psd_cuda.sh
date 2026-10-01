#!/usr/bin/env bash
set -euo pipefail

# Build the GPU PSD library (libpsdcuda.so) for RFOBS_PSD_BACKEND=cuda.
#
# Compiles src/rfobserver/processing/cuda/psd_cuda.cu with nvcc into
# src/rfobserver/processing/cuda/libpsdcuda.so, next to the source, where
# rfobserver.processing.psd_cuda loads it. The file is gitignored but listed as
# a hatch build artifact, so building it before `pip3 install .` ships it in the
# installed package; an editable install picks it up in place.
#
# Without it (or without a CUDA device at runtime) the CPU backend is used.
#
# Needs the CUDA toolkit (nvcc + cuFFT). JetPack images do not always include
# it; on JetPack 6.x / CUDA 12.6 the minimal set is:
#   sudo apt-get install cuda-nvcc-12-6 cuda-cudart-dev-12-6 libcufft-dev-12-6
#
# Usage (run from the repo root; deploy/install.sh calls this):
#   ./deploy/build_psd_cuda.sh
# Environment:
#   NVCC        nvcc to use       (default: PATH, then /usr/local/cuda*/bin)
#   CUDA_ARCH   nvcc -arch value  (default: native, the GPU of this machine;
#               e.g. sm_87 for Orin when building without the GPU visible)

SRC_DIR="src/rfobserver/processing/cuda"
SRC="$SRC_DIR/psd_cuda.cu"
OUT="$SRC_DIR/libpsdcuda.so"
ARCH="${CUDA_ARCH:-native}"

if [ ! -f "$SRC" ]; then
    echo "Run from the repo root ($SRC not found)." >&2
    exit 1
fi

find_nvcc() {
    if [ -n "${NVCC:-}" ]; then
        echo "$NVCC"
        return
    fi
    if command -v nvcc >/dev/null 2>&1; then
        command -v nvcc
        return
    fi
    local cand
    for cand in /usr/local/cuda/bin/nvcc $(ls -d /usr/local/cuda-*/bin/nvcc 2>/dev/null | sort -V -r); do
        if [ -x "$cand" ]; then
            echo "$cand"
            return
        fi
    done
}

NVCC_BIN="$(find_nvcc)"
if [ -z "$NVCC_BIN" ]; then
    echo "nvcc not found; the GPU PSD backend will be unavailable (CPU is used)." >&2
    echo "Install the CUDA toolkit, e.g. on JetPack 6.x:" >&2
    echo "  sudo apt-get install cuda-nvcc-12-6 cuda-cudart-dev-12-6 libcufft-dev-12-6" >&2
    exit 2
fi

CUDA_HOME="$(cd "$(dirname "$NVCC_BIN")/.." && pwd)"
LIB_DIR=""
for cand in "$CUDA_HOME"/targets/*/lib "$CUDA_HOME/lib64"; do
    if [ -e "$cand/libcufft.so" ]; then
        LIB_DIR="$cand"
        break
    fi
done
if [ -z "$LIB_DIR" ]; then
    echo "libcufft.so not found under $CUDA_HOME (install libcufft-dev)." >&2
    exit 2
fi

echo "=== Building $OUT with $NVCC_BIN (arch $ARCH) ==="
# Build to a temp file and rename, so a failed build never leaves a broken .so
# where the loader would find it. The rpath makes the CUDA libraries resolve
# without LD_LIBRARY_PATH (the systemd service sets none).
TMP="$OUT.tmp.$$"
trap 'rm -f "$TMP"' EXIT
"$NVCC_BIN" -O3 -shared -Xcompiler -fPIC -arch="$ARCH" "$SRC" \
    -L"$LIB_DIR" -lcufft -Xlinker -rpath -Xlinker "$LIB_DIR" -o "$TMP"
chmod 644 "$TMP"
mv -f "$TMP" "$OUT"
trap - EXIT
echo "Built $OUT"
echo "Enable with RFOBS_PSD_BACKEND=cuda (or the Config page)."
