# syntax=docker/dockerfile:1
#
# Build:  docker build -t tg-searcher .
# Run:    docker run --rm -it -v $PWD/config:/app/config tg-searcher
#
# OpenCV and Clang are installed from the distribution packages, so the same
# Dockerfile works for any Linux architecture supported by Debian's OpenCV.

FROM rust:1.99-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        libopencv-dev \
        pkg-config \
        cmake \
    && rm -rf /var/lib/apt/lists/*

# Debian bookworm ships LLVM/Clang 14 in this location. clang-sys needs the
# directory containing libclang.so for binding generation.
ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libopencv-dev \
        redis-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/tg-searcher /usr/local/bin/tg-searcher
COPY searcher.sample.yaml /app/searcher.sample.yaml

VOLUME ["/app/config", "/app/data"]
ENTRYPOINT ["tg-searcher"]
CMD ["-f", "/app/config/searcher.yaml"]
