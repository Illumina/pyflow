#!/usr/bin/env bash

#
# clean trailing w/s from pyflow source
#
# pretty hacky script... probably best to avoid running this if you have a lot of uncommitted changes
#

set -o nounset

scriptdir=$(cd $(dirname $0); pwd -P)


get_source() {
    find $scriptdir/../pyflow -type f \
        -name "*.bash" -or \
        -name "*.py"
}

tempfile=$(mktemp)

for f in $(get_source); do
    echo "checking: $f"
    cat $f |\
    sed 's/[ 	]*$//' >|\
    $tempfile    

    if ! diff $tempfile $f > /dev/null; then 
        mv -f $tempfile $f
    else 
        rm -f $tempfile 
    fi
done
