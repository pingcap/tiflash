#!/bin/bash

set -ex

SCRIPTPATH="$(
    cd "$(dirname "$0")"
    pwd -P
)"

SRCPATH=${1:-$(
    cd ${SCRIPTPATH}/../..
    pwd -P
)}

BUILD_BRANCH=${BUILD_BRANCH:-master}

python3 ${SRCPATH}/format-diff.py --repo_path "${SRCPATH}" --check_formatted --diff_from $(git merge-base origin/${BUILD_BRANCH} HEAD) --dump_diff_files_to "/tmp/tiflash-diff-files.json"
