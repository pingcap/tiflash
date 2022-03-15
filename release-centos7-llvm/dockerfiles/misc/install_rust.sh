#!/usr/bin/env bash

# Install Rust for CI/CD.
# Require: curl

function install_rust() {
    curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain nightly
    SCRIPTPATH=$(cd $(dirname "$0"); pwd -P)
    mkdir -p $HOME/.cargo/
    cp $SCRIPTPATH/cargo-config $HOME/.cargo/config
}
