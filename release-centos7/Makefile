build_release:
	docker run --rm -v $(realpath ..):/tics hub.pingcap.net/tiflash/tiflash-builder /tics/release-centos7/build/build-release.sh

image_builder:
	docker build -f Dockerfile-builder -t hub.pingcap.net/tiflash/tiflash-builder .

push_image_builder:
	docker push hub.pingcap.net/tiflash/tiflash-builder

image_tiflash:
	docker build -f Dockerfile-tiflash-centos7 -t hub.pingcap.net/tiflash/tiflash-server-centos7 .
	docker build -f Dockerfile-tiflash-ubuntu -t hub.pingcap.net/tiflash/tiflash-server-ubuntu .
