# Developer entry points. Everything here is also runnable by hand, the
# targets just remember the flags.

PG_SRC := vendor/postgres
PG_BUILD := build/pg-build
PG_PREFIX := $(abspath build/pg)

.PHONY: demo test lint pg-init pg-patch pg-build pg-clean

# End to end tour of the object layer on a local directory.
demo:
	cargo run -p zou-store --example demo

test:
	cargo test --workspace --all-features

lint:
	cargo fmt --check && cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fetch the pinned Postgres source. Shallow, the full history is not needed.
pg-init:
	git submodule update --init --depth 1 $(PG_SRC)

# Reset the vendored tree to the pinned commit and apply the patch series.
pg-patch: pg-init
	scripts/pg-apply-patches.sh

# Out of tree build so the submodule stays clean. Stock configuration,
# see docs/postgres.md before changing options.
pg-build: pg-patch
	meson setup $(PG_BUILD) $(PG_SRC) --prefix=$(PG_PREFIX) || meson setup --reconfigure $(PG_BUILD) $(PG_SRC) --prefix=$(PG_PREFIX)
	ninja -C $(PG_BUILD)
	ninja -C $(PG_BUILD) install

pg-clean:
	rm -rf $(PG_BUILD) $(PG_PREFIX)
