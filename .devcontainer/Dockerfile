# syntax=docker/dockerfile:1

# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.245.2/containers/debian/.devcontainer/base.Dockerfile
FROM mcr.microsoft.com/vscode/devcontainers/base:0-bullseye

# As we are using buildkit, we could mount cache directories. So there is no need to
# clean up the package. See https://github.com/moby/buildkit/blob/master/frontend/dockerfile/docs/reference.md#example-cache-apt-packages
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

# Install Clang
# Clang is very large and we do not change its version frequently. Place it at the beginning
# to better utilize the docker cache.
ARG CLANG_VERSION
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    curl -L https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && echo "deb http://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-${CLANG_VERSION} main" | tee /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
                clang-${CLANG_VERSION} lldb-${CLANG_VERSION} lld-${CLANG_VERSION} clangd-${CLANG_VERSION} \
                clang-tidy-${CLANG_VERSION} clang-format-${CLANG_VERSION} clang-tools-${CLANG_VERSION} \
                llvm-${CLANG_VERSION}-dev llvm-${CLANG_VERSION}-tools libomp-${CLANG_VERSION}-dev \
                libc++-${CLANG_VERSION}-dev libc++abi-${CLANG_VERSION}-dev \
                libclang-common-${CLANG_VERSION}-dev libclang-${CLANG_VERSION}-dev \
                libclang-cpp${CLANG_VERSION}-dev libunwind-${CLANG_VERSION}-dev \
    && rm -rf /etc/apt/sources.list.d/llvm.list

# Install Cmake
ARG CMAKE_VERSION
RUN wget -q https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-$(uname -m).tar.gz -O - | \
    tar -xzf - --directory /usr/local --strip-components=1

# Install other build dependencies
# make is needed when building TiFlash proxy.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
                ninja-build zlib1g-dev libcurl4-openssl-dev ccache \
                make \
# Also install some handy dev tools, so that you will be happy when `docker exec -it` into the container.
                vim screen


USER vscode

# Install Rust. No need to install a specific toolchain, because we will specify
# it in rust-toolchain file.
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain none
# Override default linkers. Cargo does not respect CC env variables at all.
RUN mkdir -p /home/vscode/.cargo/
COPY --chown=vscode:vscode misc/cargo-config.toml /home/vscode/.cargo/config

# Create directories to mount as volumes. These directories will be persisted
# after the container is recreated.
RUN mkdir -p /home/vscode/.vscode-server/extensions \
    && mkdir -p /home/vscode/.cargo/registry \
    && mkdir -p /home/vscode/.cargo/git \
    && mkdir -p /home/vscode/.rustup/toolchains \
    && mkdir -p /home/vscode/.cache/ccache
