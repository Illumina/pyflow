#!/usr/bin/env bash

if [ $# != 1 ]; then
    echo "usage $0 arg"
    exit 1
fi
arg=$1

pid=$$
echo pid: $pid arg: $arg starting sleep
sleep $arg
echo pid: $pid arg: $arg ending sleep

