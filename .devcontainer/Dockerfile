# syntax=docker/dockerfile:1

# The source of this image is available at https://github.com/pingcap/tiflash/blob/master/release-linux-llvm/dockerfiles/Dockerfile-tiflash-llvm-base
FROM hub.pingcap.net/tiflash/tiflash-llvm-base:rocky8-llvm-17.0.6

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y yum-utils

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y git zsh python3 gh sudo screen

# Use dump-init as the default entry point.
RUN if [[ "$(uname -m)" = @(x86_64|x64) ]]; then \
    PLATFORM=x86_64 ; \
  elif [[ "$(uname -m)" = @(arm64|aarch64|armv8b|armv8l) ]]; then \
    PLATFORM=aarch64 ; \
  fi \
  && wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_${PLATFORM}
RUN chmod +x /usr/local/bin/dumb-init

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

ENV HOME=""
USER $USERNAME
WORKDIR /home/$USERNAME

RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
COPY --chown=$USERNAME misc/codespaces.zsh-theme /home/$USERNAME/.oh-my-zsh/custom/themes/
RUN { echo "zstyle ':omz:update' mode disabled" | tee -a /home/$USERNAME/.zshrc ; } \
    && sed -i -e 's/ZSH_THEME=.*/ZSH_THEME="codespaces"/g' /home/$USERNAME/.zshrc

# Install Rust for current user
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain none
# Override default Rust linkers. Cargo does not respect CC env variables at all.
RUN mkdir -p /home/$USERNAME/.cargo/
RUN sudo cp /root/.cargo/config /home/$USERNAME/.cargo/config
RUN sudo chown $USERNAME:$USERNAME /home/$USERNAME/.cargo/config

# Create directories to mount as volumes. These directories will be persisted
# after the container is recreated.
RUN mkdir -p /home/$USERNAME/.vscode-server/extensions \
    && mkdir -p /home/$USERNAME/.cargo/registry \
    && mkdir -p /home/$USERNAME/.cargo/git \
    && mkdir -p /home/$USERNAME/.rustup/toolchains \
    && mkdir -p /home/$USERNAME/.cache/ccache \
    && mkdir -p /home/$USERNAME/.config/gh

COPY --chmod=777 misc/docker-entrypoint.sh /
ENTRYPOINT [ "/usr/local/bin/dumb-init", "--", "/usr/bin/zsh", "/docker-entrypoint.sh" ]
CMD [ "/usr/bin/zsh" ]

# TODO: Add other handy tools like rg fd fzf
