#!/bin/bash

function get_host_ip()
{
    if [ `uname` == "Darwin" ]; then
        local host="192.168.65.2"
    else
        local host=`ip a | grep -A 5 "docker0" | grep "inet " | awk '{print $2}' | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}'`
    fi
    if [ -z "host" ]; then
        echo "can't get host ip" >&2
        return 1
    fi
    echo "$host"
}

export get_host_ip
