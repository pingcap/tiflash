#!/bin/bash

workspace="$(cd $(dirname $0);pwd)"

set -x

sys=$(uname -s)
if [ $sys != "Linux" ]; then
    echo "Can not run on kernel: $sys"
    exit 0
fi

# https://stackoverflow.com/questions/16044204/testing-out-of-disk-space-in-linux

function clean()
{
    # umount && free limited filesystem
    umount ${mount_point}
    lb_device=$(losetup -a | grep "${limited_file}" | awk -F: '{print $1}')
    losetup -d ${lb_device}
    if [ -f "${limited_file}" ]; then
        rm -f ${limited_file}
    fi
}

function main()
{
    num_lb_device=$(losetup -a | wc -l)
    if [ ${num_lb_device} -gt 0 ]; then
        echo "Please cleanup before running test"
        exit 1
    fi

    # create limited file
    dd if=/dev/zero of=${limited_file} bs=500M count=1

    # make a loopback device
    losetup -f ${limited_file}
    lb_device=$(losetup -a | grep "${limited_file}" | awk -F: '{print $1}')

    # format with ext3 filesystem
    mkfs.ext4 ${lb_device}

    # mount as a directory
    mkdir -p ${mount_point}
    mount ${lb_device} ${mount_point}

    # run test on limited filesystem
    build_dir="${workspace}/../../../../../cmake-build-release"
    cd $build_dir && make test_page_storage_write_disk_full
    test_prog="$(find ${build_dir} -name test_page_storage_write_disk_full)"
    echo $(echo $test_prog | wc -l)
    ${test_prog} ${mount_point}/test
    if [ $? -eq 0 ]; then
        echo -e "\033[42;37m Test passed! \033[0m"
    else
        echo -e "\033[41;37m Test failed! \033[0m"
    fi

    clean
}

limited_file="$workspace/tmp.fs"
mount_point="/data/tmpfs"
if [ "x$1" == "xclean" ]; then
    set -ue
    clean
    exit 0
else
    set -u
    main
fi

