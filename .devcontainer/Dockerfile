# syntax=docker/dockerfile:1

# The source of this image is available at https://github.com/breezewish/tiflash-devcontainer
FROM breezewish/tiflash-dev:0.0.4

# Create directories to mount as volumes. These directories will be persisted
# after the container is recreated.
RUN mkdir -p /home/dev/.vscode-server/extensions \
    && mkdir -p /home/dev/.cargo/registry \
    && mkdir -p /home/dev/.cargo/git \
    && mkdir -p /home/dev/.rustup/toolchains \
    && mkdir -p /home/dev/.cache/ccache \
    && mkdir -p /home/dev/.config/gh
