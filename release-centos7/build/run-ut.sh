#!/bin/bash

set -x

INSTALL_DIR=${INSTALL_DIR:-"/build/release-centos7/tiflash"}
SRCPATH=/build/tics

ln -s ${SRCPATH}/tests /tests
ln -s ${INSTALL_DIR} /tiflash

source /tests/docker/util.sh

show_env

ENV_VARS_PATH=/tests/docker/_env.sh OUTPUT_XML=false /tests/run-gtest.sh
