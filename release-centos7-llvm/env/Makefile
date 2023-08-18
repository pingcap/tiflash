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
ARCH=$(shell uname -m)
MOUNT=$(shell realpath tiflash-env)
DOCKER_TAG=$(shell [[ "$(ARCH)" == "aarch64" ]] && echo "aarch64" || echo "amd64" )

tiflash-env:
	mkdir -p tiflash-env

tiflash-env/prepare-sysroot.sh: tiflash-env
	cp prepare-sysroot.sh tiflash-env

tiflash-env/loader: tiflash-env
	cp loader tiflash-env

tiflash-env/tiflash-linker: tiflash-env
	cp tiflash-linker tiflash-env

tiflash-env-$(ARCH).tar.xz: tiflash-env/prepare-sysroot.sh tiflash-env/loader tiflash-env/tiflash-linker
	# Add the z to modify the selinux label of the host file or directory being mounted into the container.
	# Reference: https://docs.docker.com/storage/bind-mounts/#configure-the-selinux-label
	$(DOCKER) run --rm -v $(MOUNT):/tiflash-env:z hub.pingcap.net/tiflash/tiflash-llvm-base:$(DOCKER_TAG) /tiflash-env/prepare-sysroot.sh
	tar -cvaf tiflash-env-$(ARCH).tar.xz tiflash-env

clean:
	rm -rf tiflash-env tiflash-env-$(ARCH).tar.xz
