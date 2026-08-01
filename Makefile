# Developer entry points. Everything here is also runnable by hand, the
# targets just remember the flags.

PG_SRC := vendor/postgres
PG_BUILD := build/pg-build
PG_PREFIX := $(abspath build/pg)

ZOU_PG_LIB := $(abspath target/release)

.PHONY: demo test lint pg-init pg-patch pg-build pg-vector pg-clean zou-pg-lib

# The demo in two acts: the object layer on a local directory, then
# the real Postgres on a store when pg-build has run, see
# docs/quickstart.md.
demo:
	scripts/demo.sh

test:
	cargo test --workspace --all-features

lint:
	cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fetch the pinned Postgres and pgvector sources. Shallow, the full
# history is not needed.
pg-init:
	git submodule update --init --depth 1 $(PG_SRC)
	git submodule update --init --depth 1 vendor/pgvector

# Reset the vendored tree to the pinned commit and apply the patch series.
pg-patch: pg-init
	scripts/pg-apply-patches.sh

# The zou smgr patch calls into this static library.
zou-pg-lib:
	cargo build -p zou-pg --release

# Out of tree build so the submodule stays clean. LDFLAGS pulls in the
# zou-pg staticlib for the smgr patch, see docs/postgres.md.
pg-build: pg-patch zou-pg-lib
	meson setup $(PG_BUILD) $(PG_SRC) --prefix=$(PG_PREFIX) -Duuid=e2fs -Dc_link_args="-L$(ZOU_PG_LIB) -lzou_pg" || meson setup --reconfigure $(PG_BUILD) $(PG_SRC) --prefix=$(PG_PREFIX) -Duuid=e2fs -Dc_link_args="-L$(ZOU_PG_LIB) -lzou_pg"
	ninja -C $(PG_BUILD)
	ninja -C $(PG_BUILD) install
	$(MAKE) pg-vector

# pgvector builds out of tree against the installed pg_config via pgxs.
pg-vector:
	$(MAKE) -C vendor/pgvector clean PG_CONFIG=$(PG_PREFIX)/bin/pg_config
	$(MAKE) -C vendor/pgvector install PG_CONFIG=$(PG_PREFIX)/bin/pg_config

pg-clean:
	rm -rf $(PG_BUILD) $(PG_PREFIX)
