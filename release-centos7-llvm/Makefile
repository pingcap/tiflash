# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DOCKER ?= docker
SUFFIX ?= ""

# Base docker files with prerequisites build tools
image_tiflash_llvm_base_amd64:
	$(DOCKER) build dockerfiles -f dockerfiles/tiflash-llvm-base-amd64 -t hub.pingcap.net/tiflash/tiflash-llvm-base:amd64$(SUFFIX)

image_tiflash_llvm_base_aarch64:
	$(DOCKER) build dockerfiles -f dockerfiles/tiflash-llvm-base-aarch64 -t hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64$(SUFFIX)

# Build tiflash binaries under directory "tiflash"
build_tiflash_release_amd64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64$(SUFFIX) /build/tics/release-centos7-llvm/scripts/build-release.sh

build_tiflash_release_aarch64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64$(SUFFIX) /build/tics/release-centos7-llvm/scripts/build-release.sh

# Package tiflash docker image using the binaries under directory "tiflash"
image_tiflash_release:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-server-centos7 .

# Add build_tiflash_debug_amd64 target to enable FailPoints on x86. Since outputs are the same as release version, no new package targets added. 
build_tiflash_debug_amd64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-debug.sh

build_tiflash_tsan_amd64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-tsan.sh

build_tiflash_asan_amd64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-asan.sh

build_tiflash_ci_amd64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-tiflash-ci.sh

build_tiflash_ci_aarch64:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64 /build/tics/release-centos7-llvm/scripts/build-tiflash-ci.sh

# Base docker with prerequisites running CI
image_tiflash_ci_base:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-ci-base -t hub.pingcap.net/tiflash/tiflash-ci-base .

# Package tiflash CI docker image using the binaries under directory "tiflash"
image_tiflash_ci:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-ci -t hub.pingcap.net/tiflash/tiflash-ci-centos7 .
