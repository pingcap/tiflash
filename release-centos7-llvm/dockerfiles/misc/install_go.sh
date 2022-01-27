#!/usr/bin/env bash
# Copyright (C) 2021 PingCAP, Inc.

# Install Go for CI/CD.
# Require: wget, tar

function install_go() {
    # $1: go_version
    # $2: go_arch
    wget https://dl.google.com/go/go$1.linux-$2.tar.gz 
    tar -C /usr/local -xzvf go$1.linux-$2.tar.gz
    rm -rf go$1.linux-$2.tar.gz
}