build:
	docker run --rm -v $(realpath ../..):/tics hub.pingcap.net/tiflash/tics-builder /tics/docker/builder/build.sh

pull:
	docker pull hub.pingcap.net/tiflash/tics-builder

push:
	docker push hub.pingcap.net/tiflash/tics-builder

image:
	docker build -f Dockerfile -t hub.pingcap.net/tiflash/tics-builder .

tics_base_image:
	docker build -f Dockerfile-tics-base -t hub.pingcap.net/tiflash/tics-base .

tics_image:
	docker build -f Dockerfile-tics -t hub.pingcap.net/tiflash/tics .
