build:
	docker run --rm --workdir /server -v $(realpath ../..):/server -it hub.pingcap.net/tiflash/tiflash-builder

pull:
	docker pull hub.pingcap.net/tiflash/tiflash-builder

image:
	docker build -t hub.pingcap.net/tiflash/tiflash-builder

image_push:
	docker push hub.pingcap.net/tiflash/tiflash-builder
