#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
SRCPATH=${1:-$(cd $SCRIPTPATH/../..; pwd -P)}
source ${SCRIPTPATH}/env.sh
TAR_BIN_URI="${TIFLASH_CI_BUILD_URI_PREFIX}/${PULL_ID}/tiflash.tar.gz"
COMMIT_HASH_FILE_URI="${TIFLASH_CI_BUILD_URI_PREFIX}/${PULL_ID}/commit-hash"

build_dir="$SRCPATH/release-centos7"
cd ${build_dir}
tar -zcf tiflash.tar.gz ./tiflash
MD5_SUM=$(md5sum ./tiflash.tar.gz | awk '{ print $1 }')
echo "${CI_BUILD_INFO_PRE_FIX}" > ./commit-hash
echo "${MD5_SUM}" >> ./commit-hash
echo "${COMMIT_HASH}" >> ./commit-hash

curl -F ${TAR_BIN_URI}=@tiflash.tar.gz http://fileserver.pingcap.net/upload
curl -F ${COMMIT_HASH_FILE_URI}=@commit-hash http://fileserver.pingcap.net/upload
