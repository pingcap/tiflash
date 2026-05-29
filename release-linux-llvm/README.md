# Docker Builder for Rocky Linux 8

Scripts to build TiFlash binaries under Rocky Linux 8

Usage:

```bash
> make build_tiflash_release
# The binaries is built under directory `tiflash`, check its information
> ./tiflash/tiflash version

# build release image
> make image_tiflash_release
```

To build TiFlash binaries && docker image for TiDB X:

```bash
> ENABLE_NEXT_GEN=true ENABLE_NEXT_GEN_COLUMNAR=true make build_tiflash_release
# The binary for tiflash-write is built under directory `tiflash`
# The binary for tiflash-compute is built under directory `tiflash-columnar`
> ./tiflash/tiflash version
> ./tiflash-columnar/tiflash version

# build release image for TiDB X
> make image_tiflash_next_gen_release
```


## Upgrading toolchain for releasing image

Update the script

- `dockerfiles/misc/bake_llvm_base.sh`

```bash
# Build the base docker with suffix.
> SUFFIX=-llvm-17.0.6-v2 make image_tiflash_llvm_base
# Use the new base docker to build TiFlash binary
> SUFFIX=-llvm-17.0.6-v2 make build_tiflash_release
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
> docker image push <dockerhub-url>/tiflash/tiflash-llvm-base:rocky8${SUFFIX}
```

At last, change the suffix in `${REPO_DIR}/.toolchain.yml` and check the built target in the CI.


How to get into a build container:

```
SUFFIX=-llvm-17.0.6-v2
docker run -it --rm -v /your/path/to/tiflash:/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:rocky8${SUFFIX} /bin/bash
```

## Local toolchain prerequisites

Check `env/README.md`
