# Build wiring for the pkindex demo's SlateDB engine (design §13.2 / R-S1).
#
# These targets build the native SlateDB artifacts (libslatedb_uniffi cdylib +
# slatedb-cli compactor) and run the engine-parameterized UT suite with the
# `slatedb` build tag. They reuse a cached out-of-tree checkout + cargo target
# (no full Milvus rebuild, no `make clean`).
#
#   make -C <this dir> slatedb-build     # clone matching tag + cargo build (release)
#   make -C <this dir> slatedb-test      # build + run UT1-6 + UT-CKPT-1/2 on slatedb
#
# Override the artifact location with PKINDEX_SLATEDB_PREFIX.

PREFIX ?= $(HOME)/.cache/milvus-pkindex/slatedb
REL    := $(PREFIX)/src/target/release
HARNESS := github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex/pkengine/harness

.PHONY: slatedb-build slatedb-test slatedb-print-env

slatedb-build:
	PKINDEX_SLATEDB_PREFIX=$(PREFIX) bash $(CURDIR)/scripts/build-slatedb.sh

slatedb-print-env:
	@echo "CGO_LDFLAGS=-L$(REL)"
	@echo "LD_LIBRARY_PATH=$(REL)"

# GOWORK=off makes `go` resolve the worktree module from CURDIR (which is inside it).
slatedb-test: slatedb-build
	GOWORK=off CGO_ENABLED=1 CGO_LDFLAGS="-L$(REL)" LD_LIBRARY_PATH="$(REL):$$LD_LIBRARY_PATH" \
	go test -tags 'dynamic,test,slatedb' -gcflags="all=-N -l" -count=1 -v $(HARNESS)
