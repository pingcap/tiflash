# Copyright 2022 PingCAP, Ltd.
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

# Must keep sync with the project name in repository_root/.env
export COMPOSE_PROJECT_NAME = tiflash-devcontainer

CONTAINER_SHELL := docker compose run --rm -it --entrypoint /usr/local/bin/dumb-init shell -- /usr/local/bin/zsh

.PHONY: rebuild-docker
rebuild-docker:
	docker compose build

.PHONY: dev-in-container
dev-in-container: SHELL := $(CONTAINER_SHELL)
dev-in-container:
	mkdir -p cmake-build-Debug \
		&& cd cmake-build-Debug \
		&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG \
				-DCMAKE_C_COMPILER=clang \
				-DCMAKE_CXX_COMPILER=clang++ \
		&& cmake --build . \
		&& cmake --install . --prefix=artifacts

.PHONY: dev
dev: rebuild-docker dev-in-container

.PHONY: release-in-container
release-in-container: SHELL := $(CONTAINER_SHELL)
release-in-container:
	mkdir -p cmake-build-Release \
	&& cd cmake-build-Release \
	&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=RELWITHDEBINFO \
			-DCMAKE_C_COMPILER=clang \
			-DCMAKE_CXX_COMPILER=clang++ \
	&& cmake --build . \
	&& cmake --install . --prefix=artifacts

.PHONY: release
release: rebuild-docker release-in-container

.PHONY: test-in-container
test-in-container: SHELL := $(CONTAINER_SHELL)
test-in-container:
	mkdir -p cmake-build-Debug \
		&& cd cmake-build-Debug \
		&& cmake .. -GNinja -DCMAKE_BUILD_TYPE=DEBUG \
				-DCMAKE_C_COMPILER=clang \
				-DCMAKE_CXX_COMPILER=clang++ \
		&& cmake --build . --target gtests_dbms \
		&& dbms/gtests_dbms

.PHONY: test
test: rebuild-docker test-in-container

.PHONY: clean-all
clean-all:
	rm -rf ../cmake-build-Debug
	rm -rf ../cmake-build-Release

.PHONY: clean-cmake
clean-cmake:
	rm -rf ../cmake-build-Debug/CMakeCache.txt
	rm -rf ../cmake-build-Release/CMakeCache.txt
