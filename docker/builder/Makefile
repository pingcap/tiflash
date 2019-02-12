build:
	docker run --rm --workdir /server -v $(realpath ../..):/server -it pingcap/tiflash-builder

pull:
	docker pull pingcap/tiflash-builder

image:
	docker build -t pingcap/tiflash-builder .

image_push:
	docker push pingcap/tiflash-builder
