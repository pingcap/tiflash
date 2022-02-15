#!/bin/bash

function check_arm_arch() {
    local ARCH=$(uname -i)
    if [[ "$ARCH" =~ ^(aarch64.*|AARCH64.*) || "$ARCH" == arm* ]]; then
        echo 1
    else
        echo 0
    fi
}

export -f check_arm_arch
