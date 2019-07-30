#!/bin/bash

function get_host_ip()
{
    local host=`ip a | grep -A 5 "docker0" | grep "inet " | awk '{print $2}' | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}'`
    if [ -z "host" ]; then
        echo "can't get host ip" >&2
        return 1
    fi
    echo "$host"
}

export get_host_ip