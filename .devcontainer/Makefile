# Copyright 2023 PingCAP, Inc.
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

CONTAINER_SHELL := devcontainer exec --workspace-folder=.. /usr/bin/zsh

.PHONY: dev
dev: SHELL := $(CONTAINER_SHELL)
dev:
	mkdir -p cmake-build-Debug \
		&& cd cmake-build-Debug \
		&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG \
				-DCMAKE_C_COMPILER=clang \
				-DCMAKE_CXX_COMPILER=clang++ \
		&& cmake --build . --target tiflash \
		&& cmake --install . --component=tiflash-release --prefix=artifacts

.PHONY: release
release: SHELL := $(CONTAINER_SHELL)
release:
	mkdir -p cmake-build-Release \
	&& cd cmake-build-Release \
	&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=RELWITHDEBINFO \
			-DCMAKE_C_COMPILER=clang \
			-DCMAKE_CXX_COMPILER=clang++ \
	&& cmake --build . --target tiflash \
	&& cmake --install . --component=tiflash-release --prefix=artifacts

.PHONY: test
test: SHELL := $(CONTAINER_SHELL)
test:
	mkdir -p cmake-build-Debug \
		&& cd cmake-build-Debug \
		&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG \
				-DCMAKE_C_COMPILER=clang \
				-DCMAKE_CXX_COMPILER=clang++ \
		&& cmake --build . --target gtests_dbms \
		&& dbms/gtests_dbms

.PHONY: clean-all
clean-all:
	rm -rf ../cmake-build-Debug
	rm -rf ../cmake-build-Release

.PHONY: clean-cmake
clean-cmake:
	rm -rf ../cmake-build-Debug/CMakeCache.txt
	rm -rf ../cmake-build-Release/CMakeCache.txt

.PHONY: shell
shell:
	$(CONTAINER_SHELL)
