# How to build

```shell
#prepare build enviroment 
make image_builer
```

```shell
#compile
make build_release
```

The executable files are located in `tiflash` dir.

# Deploy Enviroument Requirements

Following OS are tested OK

* CentOS 7.6 or higher version
* Ubuntu 16.04 or higher version

**NOTE** CentOS 8 has newer `libnsl.so.2`. If we want to run on CentOS 8 , maybe we should copy old libnsl manually.

Your system needs to install

* GLibC 2.27+ (musl LibC is not OK)
* Libgcc 4.8+
