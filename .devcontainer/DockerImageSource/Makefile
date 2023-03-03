BUILD_TAG := breezewish/tiflash-dev:0.0.3

.PHONY: build
build:
	docker buildx build --build-arg -t ${BUILD_TAG} .

.PHONY: build-cn
build-cn:
	docker buildx build --build-arg USE_CN_MIRROR=1 -t ${BUILD_TAG} .
