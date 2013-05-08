#!/usr/bin/env bash

#
# this script makes the pyflow release tarball assuming it's being called in the git repo
# already checked out to the targeted version
#
# the tarball is written to the callers cwd
#

set -o nounset
set -o xtrace
set -o errexit

pname_root=""
if [ $# -gt 1 ]; then
    echo "usage: $0 [rootname]"
    exit 2
elif [ $# == 1 ]; then
    pname_root=$1
fi


get_abs_path() {
    (cd $1; pwd -P)
}


script_dir=$(get_abs_path $(dirname $0))
outdir=$(pwd)
echo $outdir

cd $script_dir
echo $script_dir
gitversion=$(git describe | sed s/^v//)

if [ "$pname_root" == "" ]; then
    pname_root=pyflow-$gitversion
fi

pname=$outdir/$pname_root

cd ..

# use archive instead of copy so that we clean up any tmp files in the working directory:
git archive --prefix=$pname_root/ HEAD:pyflow/ | tar -x -C $outdir

# make version number substitutions:
cat pyflow/src/pyflow.py |\
sed "s/pyflowAutoVersion = None/pyflowAutoVersion = \"$gitversion\"/" >|\
$pname/src/pyflow.py

cat pyflow/README.txt |\
sed "s/\${VERSION}/$gitversion/" >|\
$pname/README.txt

chmod +x $pname/src/pyflow.py

cd $outdir
tar -cz $pname_root -f $pname.tar.gz
rm -rf $pname 

