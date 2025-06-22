# next-gen binaries download script

```bash
# copy the binaries from tikv/tidb/pd/tiflash images to local
make download
# always pull the latest image before copy the binaries from images to local
make download PULL=1
# use podman instead of docker
make download DOCKER=podman
```
