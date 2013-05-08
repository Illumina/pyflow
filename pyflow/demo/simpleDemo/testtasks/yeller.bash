#!/usr/bin/env bash

if [ $# != 1 ]; then
    echo "usage $0 arg"
    exit 1
fi
arg=$1

pid=$$
echo pid: $pid arg: $arg starting yell
for i in {1..100}; do
    echo "Yeller $pid yellin $i stdout"
    echo "Yeller $pid yellin $i stderr" 1>&2
done
echo pid: $pid arg: $arg ending sleep

