#!/usr/bin/env bash

function vendor_dependency() {
    lib=$(ldd "$1" | grep '=>' | grep "$2" | awk '{print $3}')
    cp -f $lib $3
}
