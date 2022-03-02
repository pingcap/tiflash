# base docker files
image_tiflash_llvm_base_amd64:
	docker build dockerfiles -f dockerfiles/tiflash-llvm-base-amd64 -t hub.pingcap.net/tiflash/tiflash-llvm-base:amd64

image_tiflash_llvm_base_aarch64:
	docker build dockerfiles -f dockerfiles/tiflash-llvm-base-aarch64 -t hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64

# build tiflash
build_tiflash_release_amd64:
	docker run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-release.sh

build_tiflash_ci_amd64:
	docker run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64 /build/tics/release-centos7-llvm/scripts/build-tiflash-ci.sh

build_tiflash_release_aarch64:
	docker run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64 /build/tics/release-centos7-llvm/scripts/build-release.sh

build_tiflash_ci_aarch64:
	docker run --rm -v $(realpath ..):/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64 /build/tics/release-centos7-llvm/scripts/build-tiflash-ci.sh

# package tiflash
image_tiflash_release:
	docker build -f dockerfiles/Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-server-centos7 .

image_tiflash_ci:
	docker build -f dockerfiles/Dockerfile-tiflash-ci -t hub.pingcap.net/tiflash/tiflash-ci-centos7 .

image_tiflash_ci_base:
	docker build -f dockerfiles/Dockerfile-tiflash-ci-base -t hub.pingcap.net/tiflash/tiflash-ci-base .

