# next-gen binaries download script

Note that the images under `us-docker.pkg.dev` are private. You need to acquire the access token and login before using this script.

```bash
docker login -u oauth2accesstoken --password-stdin us-docker.pkg.dev
# or using podman instead of docker
# podman login -u oauth2accesstoken --password-stdin us-docker.pkg.dev
```

Download the binaries and start a tiup cluster.

```bash
# copy the binaries from tikv/tidb/pd/tiflash images to local
make download
# always pull the latest image before copy the binaries from images to local
make download PULL=1
# use podman instead of docker
make download DOCKER=podman

# prepare the patch package
make package

# deploy a next-gen cluster
tiup cluster deploy j8-ng v8.5.2 topo/topo80.ng.yaml --ignore-config-check
# patch the binaries before starting the cluster
tiup cluster patch j8-ng -R tidb ./binaries/package/tidb.tar.gz --overwrite --offline -y
tiup cluster patch j8-ng -R pd ./binaries/package/pd.tar.gz --overwrite --offline -y
tiup cluster patch j8-ng -R tikv ./binaries/package/tikv.tar.gz --overwrite --offline -y
tiup cluster patch j8-ng -R tiflash ./binaries/package/tiflash.tar.gz --overwrite --offline -y
# start the cluster
tiup cluster start j8-ng
```
