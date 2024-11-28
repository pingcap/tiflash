# Docker Builder for Rocky Linux 8

Scripts to build TiFlash binaries under Rocky Linux 8

Usage: 

```bash
> make build_tiflash_release
# The binaries is built under directory `tiflash`, check its information
> ./tiflash/tiflash version
```

## Upgrading toolchain for releasing image

Update the script

- `dockerfiles/misc/bake_llvm_base.sh`

```bash
# Build the base docker with suffix.
> SUFFIX=-llvm-17.0.6 make image_tiflash_llvm_base
# Use the new base docker to build TiFlash binary
> SUFFIX=-llvm-17.0.6 make build_tiflash_release
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
> docker image push <dockerhub-url>/tiflash/tiflash-llvm-base:rocky8-llvm-17.0.6
```

At last, change the suffix in `${REPO_DIR}/.toolchain.yml` and check the built target in the CI.


How to get into a build container:

```
SUFFIX=-llvm-17.0.6
docker run -it --rm -v /your/path/to/tiflash:/build/tics hub.pingcap.net/tiflash/tiflash-llvm-base:rocky8${SUFFIX} /bin/bash
```

## Local toolchain prerequisites

Check `env/README.md`
