#!/bin/bash

function show_env() {
  set +e
  pwd
  df -h
  grep ^ /sys/block/*/queue/rotational

  cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c
  cat /proc/meminfo
  uname -a
  hostname
  lsmod
  dmidecode | grep 'Product Name'
  free -mh
  cat /proc/loadavg

  set -e
}

function wait_env() {
  local timeout='200'
  local failed='true'

  echo "=> wait for env available"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if [[ -n $(cat ./log/tidb0/tidb.log | grep "server is running MySQL protocol") && \
          -n $(cat ./log/tiflash/server.log | grep "Ready for connections") ]]; then
        local failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

function set_branch() {
  # XYZ_BRANCH: pd/tikv/tidb hash, default to `master`
  # BRANCH:     hash short cut, default to `master`
  if [ -n "$BRANCH" ]; then
    [ -z "$PD_BRANCH" ] && export PD_BRANCH="$BRANCH"
    [ -z "$TIKV_BRANCH" ] && export TIKV_BRANCH="$BRANCH"
    [ -z "$TIDB_BRANCH" ] && export TIDB_BRANCH="$BRANCH"
  fi
  echo "use branch \`${BRANCH-master}\` for ci test"
}

function clean_data_log() {
  rm -rf ./data ./log
}

export -f show_env
export -f wait_env
export -f set_branch
export -f clean_data_log
