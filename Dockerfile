# A zou image: the binary and the patched Postgres it starts.
#
#   docker build -t zou .
#   docker run --rm -p 54321:54321 -p 5432:5432 -v zou-data:/data zou
#
# Two stages, because building Postgres takes meson and bison and a
# rust toolchain and running it takes none of them. What lands in the
# second stage is exactly what scripts/zou-bundle.sh produces for a
# release, so there is one definition of what ships.
FROM rust:1.98-bookworm AS builder

# The same list the release builds against, on purpose: a postgres
# built without liblz4-dev on the machine is a postgres without lz4,
# and an image whose features are a subset of the tarball's is a
# difference nobody wrote down.
RUN apt-get update && apt-get install -y --no-install-recommends \
      meson ninja-build flex bison libreadline-dev zlib1g-dev \
      libicu-dev liblz4-dev libzstd-dev libssl-dev libxml2-dev libxslt1-dev \
      pkg-config uuid-dev patchelf \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The patch series is applied against the pinned submodule commit, so
# the vendored tree comes with its git directory rather than as loose
# files.
RUN make pg-build && cargo build --release -p zou && scripts/zou-bundle.sh

FROM debian:bookworm-slim

# What the postmaster links against, and nothing that built it. The
# bundle script prints this list and fails on anything outside it, so
# the two stay together rather than drifting until a container cannot
# run initdb.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libicu72 libreadline8 zlib1g liblz4-1 libzstd1 libssl3 libuuid1 \
      libxml2 libxslt1.1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/dist/zou-linux-x64 /opt/zou
ENV PATH="/opt/zou/bin:$PATH"

# Postgres refuses to run as root and is right to, so the image has a
# user of its own and the volume belongs to them.
RUN useradd --system --create-home --home-dir /home/zou zou \
    && mkdir /data && chown zou:zou /data
USER zou
VOLUME /data

# The api, the postgres port, and the transaction pooler. zou serve
# binds all three on 0.0.0.0, which is what a container wants and what
# zou dev deliberately does not do.
EXPOSE 54321 5432 6543

# The postmaster is found next to the binary, so there is nothing to
# configure in here and no ZOU_PG_BIN to set.
ENTRYPOINT ["zou"]
CMD ["serve", "/data"]
