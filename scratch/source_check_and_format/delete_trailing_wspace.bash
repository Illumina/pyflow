#!/usr/bin/env bash

set -o nounset
set -o pipefail

reltoabs() {
    (cd $1; pwd -P)
}

scriptdir=$(dirname $0)
basedir=$(reltoabs $scriptdir/../..)


if [ $# != 1 ] || [ "$1" != "-imeanit" ]; then
    cat <<EOF

usage: $0 -imeanit

Cleanup whitespace in all project code and ensure all files end in a newline.
This is a slightly dangerous script, make sure your work is committed before running it

EOF
    exit 2
fi


find_script_source() {
    base_dir=$1
    find $base_dir -type f \
        -name "*.bash" -or \
        -name "*.py"
}

find_doc_source() {
    base_dir=$1
    find $base_dir -type f \
        -name "*.md" -or \
        -name "*.txt" -or \
        -name "*.tex"
}


get_source() {
    for f in $basedir/src/*; do
        dir=$(basename $f)
        find_script_source $f
    done
    find_doc_source $basedir
}

for f in $(get_source); do
    echo "checking: $f"
    sed -i 's/[ 	]*$//' $f
    python $scriptdir/ensureFileEndsInNewline.py $f
done
