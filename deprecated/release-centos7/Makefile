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

image_builder_release:
	docker build -f Dockerfile-builder -t hub.pingcap.net/tiflash/tiflash-builder .

image_builder_ci:
	docker tag hub.pingcap.net/tiflash/tiflash-builder hub.pingcap.net/tiflash/tiflash-builder-ci

build_tiflash_release:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder /tics/release-centos7/build/build-release.sh

build_tiflash_ci:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder-ci /tics/release-centos7/build/build-tiflash-ci.sh

image_tiflash_release:
	docker build -f Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-server-centos7 .

image_tiflash_ci:
	docker build -f Dockerfile-tiflash-ci -t hub.pingcap.net/tiflash/tiflash-ci-centos7 .

image_tiflash_ci_base:
	docker build -f Dockerfile-tiflash-ci-base -t hub.pingcap.net/tiflash/tiflash-ci-base .

image_builder_release_arm64:
	docker build -f Dockerfile-builder-arm64 -t hub.pingcap.net/tiflash/tiflash-builder:arm64 .

build_tiflash_release_arm64:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder:arm64 /tics/release-centos7/build/build-release.sh
