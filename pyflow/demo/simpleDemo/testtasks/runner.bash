#!/usr/bin/env bash

thisdir=$(dirname $0)

cd $thisdir

if ! [ -e ./runner ]; then
    # turning on -O2 is too variable accross different platforms, so leave off:
    #
    # the move and sleep steps here help to make sure that we don't get a "text file busy"
    # error on the ./runner call below:
    #
    gcc ./runner.c -lm -o runner.tmp && mv runner.tmp runner && sleep 1
fi

./runner $1

