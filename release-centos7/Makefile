image_builder_release:
	docker build -f Dockerfile-builder -t hub.pingcap.net/tiflash/tiflash-builder .

image_builder_ci:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder /tics/release-centos7/build/build-ci.sh
	docker build -f Dockerfile-builder-ci -t hub.pingcap.net/tiflash/tiflash-builder-ci .

build_tiflash_release:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder /tics/release-centos7/build/build-release.sh

build_tiflash_ci:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder-ci /tics/release-centos7/build/build-tiflash-ci.sh

image_tiflash_release:
	docker build -f Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-server-centos7 .

image_tiflash_ci:
	docker build -f Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-ci-centos7 .