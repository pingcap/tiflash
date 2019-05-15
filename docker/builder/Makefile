build:
	docker run --rm -v $(realpath ../..):/tics hub.pingcap.net/tiflash/tics-builder-base /tics/docker/builder/build.sh

pull:
	docker pull hub.pingcap.net/tiflash/tics-builder-base

push:
	docker push hub.pingcap.net/tiflash/tics-builder-base

image:
	docker build -f Dockerfile-base -t hub.pingcap.net/tiflash/tics-builder-base .

tics_base_image:
	docker build -f Dockerfile-tics-base -t hub.pingcap.net/tiflash/tics-base .

tics_image:
	docker build -f Dockerfile-tics -t hub.pingcap.net/tiflash/tics .
