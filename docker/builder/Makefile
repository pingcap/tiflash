build:
	docker run --rm --workdir /server -v $(realpath ../..):/server -it hub.pingcap.net/tiflash/tics-builder

pull:
	docker pull hub.pingcap.net/tiflash/tics-builder

image:
	docker build -t hub.pingcap.net/tiflash/tics-builder .

image_push:
	docker push hub.pingcap.net/tiflash/tics-builder

tics_image:
	docker build -f Dockerfile-tics -t hub.pingcap.net/tiflash/tics .
