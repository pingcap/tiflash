# How to build tiflash-release-builder-image

```shell
# build tiflash-release-builder-image
make image_builder_release

# push tiflash-release-builder-image to hub
docker push hub.pingcap.net/tiflash/tiflash-builder
```

# How to build tiflash-ci-builder-image
```shell
# build tiflash-ci-builder-image
make image_builder_ci

# push tiflash-ci-builder-image to hub
docker push hub.pingcap.net/tiflash/tiflash-builder-ci
```

# How to build tiflash-release-binary
```shell
# build tilfash-release-binary
make build_tiflash_release
```

The executable files are located in `tiflash` dir.

# Deploy Enviroument Requirements

Following OS are tested OK

* CentOS 7.6
* Ubuntu 16.04 and 18.04

**NOTE** CentOS 8 has newer `libnsl.so.2`. If we want to run on CentOS 8 , maybe we should copy old libnsl manually.

Your system needs to install

* GLibC 2.27+ (musl LibC is not OK)
* Libgcc 4.8+
