#!/bin/bash

set -ueox pipefail

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPTPATH}/env.sh

DOWNLOAD_TAR=${DOWNLOAD_TAR:-false}

rm -rf "${TAR_PATH}"
mkdir -p "${TAR_PATH}"
cd "${TAR_PATH}"
TAR_BIN_URL="${TIFLASH_CI_BUILD_PRE_FIX}/${PULL_ID}/tiflash.tar.gz"
COMMIT_HASH_FILE_URL="${TIFLASH_CI_BUILD_PRE_FIX}/${PULL_ID}/commit-hash"
while [[ true ]]; do
  curl -o ./commit-hash ${COMMIT_HASH_FILE_URL}
  if [[ `grep -c "${CI_BUILD_INFO_PRE_FIX}" ./commit-hash` -ne '0' ]];then
    COMMIT_HASH_VAL=$(tail -n -1 ./commit-hash)
    if [[ ${COMMIT_HASH_VAL} == ${COMMIT_HASH} ]]; then
      if [[ ${DOWNLOAD_TAR} == "true" ]]; then
        curl -o ./tiflash.tar.gz ${TAR_BIN_URL}
        MD5_SUM=$(md5sum ./tiflash.tar.gz | awk '{ print $1 }')
        EXPECT_MD5_SUM=$(tail -n +2 ./commit-hash | head -n 1)
        if [[ ${MD5_SUM} != ${EXPECT_MD5_SUM} ]]; then
          echo "wrong md5sum got ${MD5_SUM} but expect ${EXPECT_MD5_SUM}"
          exit -1
        fi
        tar -zxf tiflash.tar.gz
      fi
      break
    fi
  fi
  echo "fail to get ci build tiflash binary, sleep 60s"
  sleep 60
done
