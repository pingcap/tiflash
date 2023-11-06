# TiFlash devcontainer

```

# Check whether you can run the docker successfully. If not, add yourself to the group of docker
> docker ps
# add yourself to the group of docker if the previous command fail
# > usermod -a -G docker ${your_user_name}

# Start a docker for devcontainer
> devcontainer up --workspace-folder=..
...
{"outcome":"success","containerId":"c71ec93d97edd3c61f96d35b3e491c265ed868192e44fd33c17b5f9802da57eb","remoteUser":"dev","remoteWorkspaceFolder":"/home/dev/workspace"}

# Then you can use the toolchain in the devcontainer to
# build the TiFlash binary
> make dev
> make test
# or
> make release

# (optional) stop the container if you don't need it anymore
# The container id is in the output of `devcontainer up`. Or
# you can find the id using `docker ps`.
> docker stop c71ec93d97edd3c61f96d35b3e491c265ed868192e44fd33c17b5f9802da57eb
> docker rm c71ec93d97edd3c61f96d35b3e491c265ed868192e44fd33c17b5f9802da57eb
```
