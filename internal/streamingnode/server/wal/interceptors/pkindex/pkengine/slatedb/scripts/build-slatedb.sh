#!/usr/bin/env bash
# Reproducible build of the SlateDB native artifacts required by the pkindex
# demo's SlateDB engine (design §13.2 / R-S1):
#   - libslatedb_uniffi.{so,dylib}  (cdylib the Go binding links via -lslatedb_uniffi)
#   - slatedb                       (slatedb-cli compactor binary, R-S2(a))
#
# The Go binding `slatedb.io/slatedb-go@v0.13.1` is a subdir module of the
# slatedb monorepo, tagged `bindings/go/v0.13.1`. UniFFI enforces a checksum
# contract, so the native lib MUST be built from the EXACT matching tag.
#
# Usage:
#   bash build-slatedb.sh                 # clone + build into the default prefix
#   PKINDEX_SLATEDB_PREFIX=/path bash build-slatedb.sh
#
# After a successful build it prints the env exports needed to build/run the
# SlateDB engine and its UTs.
set -euo pipefail

REPO="https://github.com/slatedb/slatedb"
TAG="bindings/go/v0.13.1"   # keep in lockstep with go.mod's slatedb.io/slatedb-go version
PREFIX="${PKINDEX_SLATEDB_PREFIX:-$HOME/.cache/milvus-pkindex/slatedb}"
SRC="$PREFIX/src"
REL="$SRC/target/release"

command -v cargo >/dev/null || { echo "ERROR: cargo (Rust toolchain) not found; see R-S1" >&2; exit 1; }
command -v cc    >/dev/null || { echo "ERROR: C toolchain (cc) not found; cgo needs it" >&2; exit 1; }

mkdir -p "$PREFIX"
if [ ! -d "$SRC/.git" ]; then
  echo ">> cloning $REPO @ $TAG"
  git clone --depth 1 --branch "$TAG" "$REPO" "$SRC"
else
  echo ">> reusing existing checkout at $SRC"
fi

echo ">> building slatedb-uniffi (cdylib) + slatedb-cli (release) ..."
( cd "$SRC" && cargo build --release -p slatedb-uniffi -p slatedb-cli )

# Resolve the produced shared library (.so on Linux, .dylib on macOS).
LIB=""
for cand in "$REL/libslatedb_uniffi.so" "$REL/libslatedb_uniffi.dylib"; do
  [ -f "$cand" ] && LIB="$cand"
done
[ -n "$LIB" ] || { echo "ERROR: libslatedb_uniffi not found under $REL" >&2; exit 1; }

echo
echo "OK: $LIB"
echo "OK: $REL/slatedb (compactor cli)"
echo
echo "# --- export these before building/running the SlateDB engine or UTs ---"
echo "export PKINDEX_SLATEDB_LIBDIR=\"$REL\""
echo "export CGO_LDFLAGS=\"-L$REL\""
case "$(uname -s)" in
  Darwin) echo "export DYLD_LIBRARY_PATH=\"$REL:\${DYLD_LIBRARY_PATH:-}\"" ;;
  *)      echo "export LD_LIBRARY_PATH=\"$REL:\${LD_LIBRARY_PATH:-}\"" ;;
esac
echo "# then:  go test -tags 'dynamic,test,slatedb' -gcflags=\"all=-N -l\" ./.../pkengine/harness"
