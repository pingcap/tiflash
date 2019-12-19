#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

$SCRIPTPATH/build-tiflash-proxy.sh
$SCRIPTPATH/build-cluster-manager.sh
$SCRIPTPATH/build-tiflash.sh
