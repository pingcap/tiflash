# tiflash-env Builder

Scripts to build a toolchain sysroot (with cmake, clang, ccache, go) for building TiFlash.

Usage: 

```
make tiflash-env-x86_64.tar.xz
tar xvf tiflash-env-x86_64.tar.xz

# Now you can add the env variables to your init script to
# make the toolchain avaiable
./tiflash-env/loader-env-dump > ~/.tiflash_env_rc
source ~/.tiflash_env_rc

```
