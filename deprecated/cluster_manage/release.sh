#!/bin/bash

set -ue

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

echo "cluster_manager is deprecated"
mkdir -p ${SCRIPTPATH}/dist/flash_cluster_manager
