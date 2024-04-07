# Docker Builder for centos 7

Scripts to build TiFlash binaries under centos 7

Usage: 

```bash
> make build_tiflash_release_amd64
# Or aarch64
#> make build_tiflash_release_aarch64
# The binaries is built under directory `tiflash`, check its information
> ./tiflash/tiflash version
```

## Upgrading toolchain for releasing image

Update the script

- `dockerfiles/misc/bake_llvm_base_amd64.sh`
- `dockerfiles/misc/bake_llvm_base_aarch64.sh`

```bash
# Build the base docker with suffix.
> SUFFIX=-llvm-17.0.6 make image_tiflash_llvm_base_amd64
# Use the new base docker to build TiFlash binary
> SUFFIX=-llvm-17.0.6 make build_tiflash_release_amd64
# Or aarch64
#> SUFFIX=-llvm-17.0.6 make image_tiflash_llvm_base_aarch64
#> SUFFIX=-llvm-17.0.6 make build_tiflash_release_aarch64
# The binaries is built under directory `tiflash`, check its information
> ./tiflash/tiflash version
TiFlash
Release Version: ...
...
Profile:         RELWITHDEBINFO
Compiler:        clang++ 17.0.6

Raft Proxy
...

# Push the new base docker to the dockerhub, usually it is "hub.pingcap.net"
> docker login -u <your-username> -p <your-password> <dockerhub-url>
> docker image push <dockerhub-url>/tiflash/tiflash-llvm-base:amd64-llvm-17.0.6
```

At last, change the suffix in `${REPO_DIR}/.toolchain.yml` and check the built target in the CI.


How to get into a build container:

```
SUFFIX=-llvm-17.0.6
# x86_64
docker run -it --rm -v /your/path/to/tiflash:/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:amd64${SUFFIX} /bin/bash
# arm
docker run -it --rm -v /your/path/to/tiflash:/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:aarch64${SUFFIX} /bin/bash
```

## Local toolchain prerequisites

Check `env/README.md`
