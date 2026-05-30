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
ARCH = $(shell uname -m)

TIFLASH_BUILDER_IMAGE := ghcr.io/pingcap-qe/cd/builders/tiflash:v2025.4.15-rocky8$(SUFFIX)

# Mount host SSH into the build container for private git deps (e.g. cloud-storage-engine).
BUILD_TIFLASH_SSH_VOLUME := -v $(HOME)/.ssh:/root/.ssh:ro
BUILD_TIFLASH_SSH_ENV := -e GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"
ifneq ($(SSH_AUTH_SOCK),)
BUILD_TIFLASH_SSH_VOLUME += -v $(SSH_AUTH_SOCK):/ssh-agent
BUILD_TIFLASH_SSH_ENV += -e SSH_AUTH_SOCK=/ssh-agent
endif

# Base docker files with prerequisites build tools
image_tiflash_llvm_base:
	$(DOCKER) buildx build dockerfiles -f dockerfiles/Dockerfile-tiflash-llvm-base --progress plain --build-arg arch="$(ARCH)" -t $(TIFLASH_BUILDER_IMAGE)

# Build tiflash binaries under directory "tiflash"
build_tiflash_release:
	$(DOCKER) run --rm \
		-v $(realpath ..):/build/tics \
		$(TIFLASH_BUILDER_IMAGE) \
		/build/tics/release-linux-llvm/scripts/build-release.sh

# Package tiflash docker image using the binaries under directory "tiflash"
image_tiflash_release:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-rocky8 -t ghcr.io/pingcap-qe/cd/builders/tiflash/tiflash-server-rocky8 .

# Build tiflash binaries with next-gen features under directory "tiflash" and "tiflash-columnar"
build_tiflash_next_gen_release:
	@test -d "$(HOME)/.ssh" || (echo "error: $(HOME)/.ssh is required for private git access" && exit 1)
	$(DOCKER) run --rm \
		-e ENABLE_NEXT_GEN=true \
		$(BUILD_TIFLASH_SSH_ENV) \
		$(BUILD_TIFLASH_SSH_VOLUME) \
		-v $(realpath ..):/build/tics \
		$(TIFLASH_BUILDER_IMAGE) \
		/build/tics/release-linux-llvm/scripts/build-release.sh

# Interactive shell with the same settings as build_tiflash_next_gen_release.
.PHONY: shell_tiflash_next_gen_release
shell_tiflash_next_gen_release:
	@test -d "$(HOME)/.ssh" || (echo "error: $(HOME)/.ssh is required for private git access" && exit 1)
	$(DOCKER) run -it --rm \
		-e ENABLE_NEXT_GEN=true \
		$(BUILD_TIFLASH_SSH_ENV) \
		$(BUILD_TIFLASH_SSH_VOLUME) \
		-v $(realpath ..):/build/tics \
		$(TIFLASH_BUILDER_IMAGE) \
		/bin/bash

# Package tiflash docker image using the binaries under directory "tiflash" && "tiflash-columnar"
image_tiflash_next_gen_release:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-next-gen-rocky8 -t ghcr.io/pingcap-qe/cd/builders/tiflash/tiflash-server-rocky8-next-gen .

# Add build_tiflash_debug target to enable FailPoints. Since outputs are the same as release version, no new package targets added.
build_tiflash_debug:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics $(TIFLASH_BUILDER_IMAGE) /build/tics/release-linux-llvm/scripts/build-debug.sh

build_tiflash_tsan:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics $(TIFLASH_BUILDER_IMAGE) /build/tics/release-linux-llvm/scripts/build-tsan.sh

build_tiflash_asan:
	$(DOCKER) run --rm -v $(realpath ..):/build/tics $(TIFLASH_BUILDER_IMAGE) /build/tics/release-linux-llvm/scripts/build-asan.sh

# Base docker with prerequisites running integration tests
image_tiflash_ci_base:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-ci-base -t ghcr.io/pingcap-qe/cd/builders/tiflash/tiflash-ci-base:rocky9-20250529 .

# Package tiflash CI docker image using the binaries under directory "tiflash"
image_tiflash_ci:
	$(DOCKER) build -f dockerfiles/Dockerfile-tiflash-ci -t ghcr.io/pingcap-qe/cd/builders/tiflash/tiflash-ci-rocky8 .
