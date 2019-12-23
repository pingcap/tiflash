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

Your system needs to install

* GLibC 2.27+ (musl LibC is not OK)
* Libgcc 4.8+
