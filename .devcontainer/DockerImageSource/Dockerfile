# syntax=docker/dockerfile:1

# Note: if you are building this image slowly, see https://github.com/docker/buildx/issues/379

####################
# A base image with mirrors. We should keep this image small.
# Note: Mirrors are currently available only for x86_64 arch.
####################

FROM centos:7 AS centos-base

ARG USE_CN_MIRROR

RUN if [ "$USE_CN_MIRROR" = "1" ] ; then \
      sed -e 's|^mirrorlist=|#mirrorlist=|g' \
        -e 's|^#[[:space:]]*baseurl=http://mirror.centos.org|baseurl=https://mirrors.tuna.tsinghua.edu.cn|g' \
        -i.bak \
        /etc/yum.repos.d/CentOS-*.repo; \
    fi

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y epel-release centos-release-scl

RUN if [ "$USE_CN_MIRROR" = "1" ] ; then \
      sed -e 's|^mirrorlist=|#mirrorlist=|g' \
        -e 's|^#[[:space:]]*baseurl=http://mirror.centos.org|baseurl=https://mirrors.tuna.tsinghua.edu.cn|g' \
        -i.bak \
        /etc/yum.repos.d/CentOS-SCLo*.repo \
      && sed -e 's!^metalink=!#metalink=!g' \
        -e 's!^#baseurl=!baseurl=!g' \
        -e 's!//download\.fedoraproject\.org/pub!//mirrors.tuna.tsinghua.edu.cn!g' \
        -e 's!//download\.example/pub!//mirrors.tuna.tsinghua.edu.cn!g' \
        -e 's!http://mirrors!https://mirrors!g' \
        -i /etc/yum.repos.d/epel*.repo; \
    fi

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y curl wget \
    && yum update -y ca-certificates

####################
# A base image with basic compiler toolchains
####################

FROM centos-base AS centos-base-compilers

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y cmake3 ninja-build devtoolset-10
RUN ln -s /usr/bin/cmake3 /usr/bin/cmake

# Some other missing build tools in devtoolset
RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y autoconf automake

SHELL [ "/usr/bin/scl", "enable", "devtoolset-10" ]

####################
# Build a recent ccache
####################

FROM centos-base-compilers AS builder-ccache

WORKDIR /

ARG CCACHE_VERSION=4.6.3
RUN wget https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz \
    && tar xf ccache-${CCACHE_VERSION}.tar.gz \
    && mv ccache-${CCACHE_VERSION} ccache-src

RUN mkdir -p /ccache-src/build

WORKDIR /ccache-src/build

RUN cmake .. -DCMAKE_BUILD_TYPE=Release \
      -DZSTD_FROM_INTERNET=ON \
      -DHIREDIS_FROM_INTERNET=ON \
      -DENABLE_TESTING=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -GNinja \
    && ninja-build \
    && DESTDIR=/ccache-artifacts ninja-build install

####################
# Build a recent LLVM
####################

FROM centos-base-compilers AS builder-llvm

ARG USE_CN_MIRROR
ARG CLANG_VERSION=15.0.7

WORKDIR /

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y git

# Use ccache to accelerate the rebuild of this stage
COPY --from=builder-ccache /ccache-artifacts /

RUN if [ "$USE_CN_MIRROR" = "1" ] ; \
      then LLVM_GIT_REPO=https://mirrors.tuna.tsinghua.edu.cn/git/llvm-project.git; \
      else LLVM_GIT_REPO=https://github.com/llvm/llvm-project.git; \
    fi \
    && git clone ${LLVM_GIT_REPO} --depth=1 -b llvmorg-${CLANG_VERSION}

WORKDIR /llvm-project

# Stage 1

RUN --mount=type=cache,target=/root/.cache/ccache,sharing=locked \
    rm -rf build-stage1 \
    && mkdir build-stage1 \
    && cd build-stage1 \
    && cmake -DCMAKE_BUILD_TYPE=Release \
      -GNinja \
      -DLLVM_CCACHE_BUILD=ON \
      -DLLVM_ENABLE_PROJECTS="clang;lld" \
      -DLLVM_ENABLE_RUNTIMES="libcxx;libcxxabi" \
      -DLLVM_TARGETS_TO_BUILD=Native \
      ../llvm \
    && ninja-build \
    && ninja-build install

# Stage 2
# See https://salsa.debian.org/pkg-llvm-team/llvm-toolchain/-/blob/11/debian/rules

ENV LD_LIBRARY_PATH="/usr/local/lib/x86_64-unknown-linux-gnu:/usr/local/lib/aarch64-unknown-linux-gnu:${LD_LIBRARY_PATH}"

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y zlib-devel

WORKDIR /llvm-project

RUN --mount=type=cache,target=/root/.cache/ccache,sharing=locked \
    rm -rf build-stage2 \
    && mkdir build-stage2 \
    && cd build-stage2 \
    && cmake -DCMAKE_BUILD_TYPE=Release \
      -GNinja \
      -DLLVM_CCACHE_BUILD=ON \
      -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_C_COMPILER=clang \
      -DCMAKE_INSTALL_PREFIX=/llvm-artifacts \
      -DCLANG_DEFAULT_LINKER=lld \
      -DCLANG_DEFAULT_CXX_STDLIB=libc++ \
      -DCLANG_LINK_CLANG_DYLIB=ON \
      -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
      -DLLVM_TARGETS_TO_BUILD=Native \
      -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld;lldb;polly" \
      -DLLVM_ENABLE_RUNTIMES="compiler-rt;libcxx;libcxxabi" \
      -DLLVM_ENABLE_ZLIB=FORCE_ON \
      -DLLVM_BUILD_LLVM_DYLIB=ON \
      -DLLVM_LINK_LLVM_DYLIB=ON \
      -DLLVM_INSTALL_TOOLCHAIN_ONLY=ON \
      -DLIBCXX_USE_COMPILER_RT=YES \
      -DLIBCXXABI_USE_COMPILER_RT=YES \
      ../llvm \
    && ninja-build \
    && ninja-build install

####################
# Build a recent git
####################

FROM centos-base-compilers AS builder-git

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y curl-devel expat-devel gettext-devel openssl-devel zlib-devel perl-CPAN perl-devel

WORKDIR /

ARG GIT_VERSION=2.38.0
RUN wget https://mirrors.edge.kernel.org/pub/software/scm/git/git-${GIT_VERSION}.tar.xz \
    && tar xf git-${GIT_VERSION}.tar.xz \
    && mv git-${GIT_VERSION} git-src

WORKDIR /git-src

RUN NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)} \
    && make configure \
    && ./configure --prefix=/usr/local \
    && make all -j ${NPROC} \
    && make strip \
    && make install DESTDIR=/git-artifacts

####################
# Build a recent zsh
####################

FROM centos-base-compilers AS builder-zsh

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y ncurses-devel

WORKDIR /

ARG ZSH_VERSION=5.8.1
RUN wget https://www.zsh.org/pub/old/zsh-${ZSH_VERSION}.tar.xz \
    && tar xf zsh-${ZSH_VERSION}.tar.xz \
    && mv zsh-${ZSH_VERSION} zsh-src

WORKDIR /zsh-src

RUN NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)} \
    && ./configure --with-tcsetpgrp --prefix=/usr/local \
    && make -j ${NPROC} \
    && make install DESTDIR=/zsh-artifacts

RUN ls -la /zsh-artifacts

####################
# Build a recent Python
# Refer to https://github.com/docker-library/python/blob/master/3.10/slim-bullseye/Dockerfile
####################

FROM centos-base-compilers AS builder-python

WORKDIR /

ARG PYTHON_VERSION=3.10.7
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
    && tar xf Python-${PYTHON_VERSION}.tar.xz \
    && mv Python-${PYTHON_VERSION} python-src

WORKDIR /python-src

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y zlib-devel bzip2 bzip2-devel readline-devel openssl-devel tk-devel \
                   libffi-devel xz-devel gdbm-devel ncurses-devel db4-devel

RUN NPROC=${NPROC:-$(nproc || grep -c ^processor /proc/cpuinfo)} \
    && ./configure --prefix=/usr/local \
                   --disable-test-modules \
                   --with-ensurepip=no \
    && make -s -j ${NPROC} LDFLAGS="-Wl,--strip-all" \
    && make install DESTDIR=/python-artifacts

RUN find /python-artifacts -depth \
		\( \
			\( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
			-o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name 'libpython*.a' \) \) \
		\) -exec rm -rf '{}' + \
	;

####################
# Build a recent cmake
####################

FROM centos-base AS builder-cmake

WORKDIR /

ARG CMAKE_VERSION=3.22.6
RUN wget https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-$(uname -m).tar.gz \
    && tar xf cmake-${CMAKE_VERSION}-linux-$(uname -m).tar.gz \
    && mv cmake-${CMAKE_VERSION}-linux-$(uname -m) cmake-artifacts

# Remove unused things
RUN rm -rf /cmake-artifacts/doc \
           /cmake-artifacts/man \
           /cmake-artifacts/bin/cmake-gui \
           /cmake-artifacts/bin/ccmake

####################
# Build GitHub CLI
####################

FROM centos-base AS builder-gh

WORKDIR /

ARG GH_VERSION=2.17.0

RUN if [[ "$(uname -m)" = @(x86_64|x64) ]]; then \
      PLATFORM=amd64 ; \
    elif [[ "$(uname -m)" = @(arm64|aarch64|armv8b|armv8l) ]]; then \
      PLATFORM=arm64 ; \
    fi \
    && mkdir gh-artifacts \
    && wget -qO - https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${PLATFORM}.tar.gz \
       | tar xz -C gh-artifacts --strip-components=1

####################
# Build the DevContainer
####################

FROM centos-base

COPY --from=builder-git /git-artifacts /

COPY --from=builder-zsh /zsh-artifacts /
RUN { echo "/usr/local/bin/zsh" | tee -a /etc/shells ; } \
    && sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" \
    && chsh -s /usr/local/bin/zsh \
    && mkdir -p /root/.oh-my-zsh/custom/themes
COPY misc/codespaces.zsh-theme /root/.oh-my-zsh/custom/themes/
RUN { echo "DISABLE_AUTO_UPDATE=true" | tee -a /root/.zshrc ; } \
    && { echo "DISABLE_UPDATE_PROMPT=true" | tee -a /root/.zshrc ; } \
    && sed -i -e 's/ZSH_THEME=.*/ZSH_THEME="codespaces"/g' /root/.zshrc

# Install Rust: No need to install a specific toolchain, because we will specify
# it in rust-toolchain file.
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain none
# Override default Rust linkers. Cargo does not respect CC env variables at all.
RUN mkdir -p /root/.cargo/
COPY misc/cargo-config.toml /root/.cargo/config

RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    yum install -y vim screen less \
                   make ninja-build gcc

# Use dump-init as the default entry point.
RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /usr/local/bin/dumb-init

COPY --from=builder-cmake /cmake-artifacts /usr/local/

COPY --from=builder-llvm /llvm-artifacts /usr/local/
RUN { echo "/usr/local/lib/$(uname -m)-unknown-linux-gnu" | tee /etc/ld.so.conf.d/llvm.conf ; } \
    && ldconfig

COPY --from=builder-ccache /ccache-artifacts /

COPY --from=builder-python /python-artifacts /

COPY --from=builder-gh /gh-artifacts/bin/gh /usr/local/bin/gh

# TODO: Add other handy tools like rg fd fzf

COPY misc/docker-entrypoint.sh /
ENTRYPOINT [ "/usr/local/bin/dumb-init", "--", "/usr/local/bin/zsh", "/docker-entrypoint.sh" ]
CMD [ "/usr/local/bin/zsh" ]
