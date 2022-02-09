ARCH=$(shell uname -m)
MOUNT=$(shell realpath tiflash-env)
DOCKER_TAG=$(shell [[ "$(ARCH)" == "aarch64" ]] && echo "aarch64" || echo "amd64" )
tiflash-env:
	mkdir -p tiflash-env

tiflash-env/prepare-sysroot.sh: tiflash-env
	cp prepare-sysroot.sh tiflash-env

tiflash-env/loader: tiflash-env
	cp loader tiflash-env

tiflash-env/tiflash-linker: tiflash-env
	cp tiflash-linker tiflash-env

tiflash-env-$(ARCH).tar.xz: tiflash-env/prepare-sysroot.sh tiflash-env/loader tiflash-env/tiflash-linker
	docker run --rm -v $(MOUNT):/tiflash-env hub.pingcap.net/tiflash/tiflash-llvm-base:$(DOCKER_TAG) /tiflash-env/prepare-sysroot.sh
	tar -cvaf tiflash-env-$(ARCH).tar.xz tiflash-env

clean:
	rm -rf tiflash-env tiflash-env-$(ARCH).tar.xz