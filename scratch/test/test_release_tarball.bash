#!/usr/bin/env bash
#
# this script brings everything together for an automated build/test system
#

set -o errexit
set -o nounset
set -o xtrace

if [ $# -gt 1 ]; then
    echo "usage: $0 [ -nosge ]" 2>&1
    exit 2
fi

is_sge=1
if [ $# -ge 1 ] && [ "$1" == "-nosge" ]; then
    is_sge=0
fi



thisdir=$(dirname $0)

cd $thisdir/..
testname=TESTBALL
make_release_tarball.bash $testname
tar -xzf $testname.tar.gz

testdir=$(pwd)/$testname

# run through tests:
PYTHONPATH=$testdir/src test/pyflow_unit_tests.py -v

# run this a few times just in case we can russle out any subtle/rare race conditions:
for f in $(seq 5); do
    PYTHONPATH=$testdir/src test/pyflow_basic_feature_runner.py --mode local
done

if [ $is_sge == 1 ]; then
    PYTHONPATH=$testdir/src test/pyflow_basic_feature_runner.py --mode sge
fi

# run through demos:
for f in cwdDemo envDemo helloWorld makeDemo memoryDemo mutexDemo simpleDemo subWorkflow; do
    cd $testdir/demo/$f
    python $f.py
    python pyflow.data/state/make_pyflow_task_graph.py >| test.dot
done


