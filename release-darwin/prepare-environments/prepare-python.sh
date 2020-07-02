#!/bin/bash

set -ueox pipefail

brew install python
# you might need to run with sudo
pip3 install \
    pybind11 \
    pyinstaller \
    dnspython \
    uri \
    requests \
    urllib3 \
    toml \
    setuptools \
    etcd3
