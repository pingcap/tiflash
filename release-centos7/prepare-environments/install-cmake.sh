#!/usr/bin/env bash

set -euo pipefail

VERSION="3.20"
COMPLETE_VERSION="3.20.3"

cd ~
wget "https://download.pingcap.org/cmake-${COMPLETE_VERSION}-linux-x86_64.tar.gz"
tar zxvf cmake-${COMPLETE_VERSION}-linux-x86_64.tar.gz

rm cmake-${COMPLETE_VERSION}-linux-x86_64.tar.gz
yum remove cmake -y

ln -sf ~/cmake-${COMPLETE_VERSION}-linux-x86_64/bin/ccmake /usr/bin/ccmake
ln -sf ~/cmake-${COMPLETE_VERSION}-linux-x86_64/bin/cmake /usr/bin/cmake
ln -sf ~/cmake-${COMPLETE_VERSION}-linux-x86_64/bin/cmake-gui /usr/bin/cmake-gui
ln -sf ~/cmake-${COMPLETE_VERSION}-linux-x86_64/bin/cpack /usr/bin/cpack
ln -sf ~/cmake-${COMPLETE_VERSION}-linux-x86_64/bin/ctest /usr/bin/ctest

cmake --version
