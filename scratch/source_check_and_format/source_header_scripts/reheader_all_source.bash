#!/usr/bin/env bash

#
# this script is used for changing license headers in the project, it attempts to cover all files
#

set -o nounset
set -o pipefail


rel2abs() {
    (cd $1; pwd -P)
}


thisdir=$(rel2abs $(dirname $0))


find_python_source() {
    base_dir=$1
    find $base_dir -type f \
        -name "*.py"
}


reheader_file() {
    script="$1"
    file=$2

    if [ ! -f $file ]; then return; fi
    echo $file

    is_exe=false
    if [ -x $file ]; then
        is_exe=true
    fi
    tmpfile=$(mktemp)
    $script $thisdir/new_header < $file >| $tmpfile
    if [ $? != 0 ]; then echo "error on file $file"; exit 1; fi
    mv $tmpfile $file
    if [ $? != 0 ]; then echo "error on file $file"; exit 1; fi
    if $is_exe; then
        chmod +x $file
    fi
}


project_base_dir=$(rel2abs $thisdir/../../..)
pyflow_dir=${project_base_dir}/pyflow


get_script_files() {
    echo $(find_python_source $pyflow_dir)
}

for file in $(get_script_files); do
    reheader_file "python $thisdir/reheader_script_file.py" $file
done

