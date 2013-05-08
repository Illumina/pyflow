#!/usr/bin/env bash

thisdir=$(dirname $0)

cd $thisdir

if ! [ -e ./runner ]; then
    # turning on -O2 is too variable accross different platforms, so leave off:
    gcc ./runner.c -lm -o runner.tmp && mv runner.tmp runner
fi

./runner $1


