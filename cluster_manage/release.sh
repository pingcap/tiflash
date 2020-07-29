#!/bin/bash

set -ue

pushd tikv_util
./build.sh

popd

python3 check_lib.py

if [ $? != 0 ]; then
  echo "check lib fail"
  exit -1
else
  echo "check lib success"
fi

commit_ts=`git log -1 --format="%ct"`

if [ "`uname`" == "Darwin" ]; then
  commit_time=`date -r $commit_ts +"%Y-%m-%d %H:%M:%S"`
else
  commit_time=`date -d @$commit_ts +"%Y-%m-%d %H:%M:%S"`
fi

git_hash=`git log -1 --format="%H"`
git_branch=`git symbolic-ref -q --short HEAD || git describe --tags --exact-match || echo "HEAD"`
version_file='version.py'
git_hash_info="git_hash = '$git_hash'"
overwrite="true"

if [ -f ${version_file} ]; then
  tmp_hash=`head -n 1 version.py`
  if [ "$tmp_hash" == "$git_hash_info" ]; then
    overwrite="false"
  fi
fi

if [ $overwrite == "true" ]; then
  echo "start to overwrite $version_file"
  echo "$git_hash_info" > $version_file
  echo "commit_time = '$commit_time'" >> $version_file
  echo "git_branch = '$git_branch'" >> $version_file
fi

echo ""
echo "Cluster Manager Version Info"
cat $version_file
echo ""

pyinstaller flash_cluster_manager.py -y --hidden-import pkg_resources.py2_warn
