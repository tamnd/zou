# Developer entry points. Everything here is also runnable by hand, the
# targets just remember the flags.

PG_SRC := vendor/postgres
PG_BUILD := build/pg-build
PG_PREFIX := $(abspath build/pg)

ZOU_PG_LIB := $(abspath target/release)

.PHONY: demo test test-functions lint pg-init pg-patch pg-build pg-vector pg-clean zou-pg-lib

# The demo in two acts: the object layer on a local directory, then
# the real Postgres on a store when pg-build has run, see
# docs/quickstart.md.
demo:
	scripts/demo.sh

# zou-deno is left out because its feature is V8, which is a forty
# megabyte download and a minute of swc before a single test runs. It
# has its own target below and its own CI job.
test:
	cargo test --workspace --exclude zou-deno --all-features

# The javascript half of edge functions, engine and all.
test-functions:
	cargo test -p zou-deno --features isolate

lint:
	cargo fmt --check && cargo clippy --workspace --exclude zou-deno --all-targets --all-features -- -D warnings && cargo clippy -p zou-deno --features isolate --all-targets -- -D warnings

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

# Most of postgres' optional dependencies default to auto, which means
# the build takes whatever the machine happens to have installed: a
# builder with libkrb5-dev on it produces a postmaster that needs
# libgssapi_krb5 at runtime and one without produces a postmaster that
# does not, from the same commit. The ones below are authentication
# methods and integrations zou does not offer, so they are off by name
# rather than by accident, and the bundle script checks what is left.
PG_OFF := -Dgssapi=disabled -Dldap=disabled -Dpam=disabled -Dbsd_auth=disabled \
	-Dbonjour=disabled -Dselinux=disabled -Dsystemd=disabled -Dlibcurl=disabled \
	-Dlibnuma=disabled -Dliburing=disabled
PG_OPTS := --prefix=$(PG_PREFIX) -Duuid=e2fs $(PG_OFF) -Dc_link_args="-L$(ZOU_PG_LIB) -lzou_pg"

# Out of tree build so the submodule stays clean. LDFLAGS pulls in the
# zou-pg staticlib for the smgr patch, see docs/postgres.md.
pg-build: pg-patch zou-pg-lib
	meson setup $(PG_BUILD) $(PG_SRC) $(PG_OPTS) || meson setup --reconfigure $(PG_BUILD) $(PG_SRC) $(PG_OPTS)
	ninja -C $(PG_BUILD)
	ninja -C $(PG_BUILD) install
	$(MAKE) pg-vector

# pgvector builds out of tree against the installed pg_config via pgxs.
pg-vector:
	$(MAKE) -C vendor/pgvector clean PG_CONFIG=$(PG_PREFIX)/bin/pg_config
	$(MAKE) -C vendor/pgvector install PG_CONFIG=$(PG_PREFIX)/bin/pg_config

pg-clean:
	rm -rf $(PG_BUILD) $(PG_PREFIX)
