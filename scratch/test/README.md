
## pyflow test scripts

### Global test scripts

The new global test script maintained for *nix and windows is:

    test_pyflow.py


The previous global test script written for *nix only is:

    test_release_tarball.bash


...this currently contains more tests, and will still be the test target for
travis until windows support is complete.


### Component test scripts

* pyflow_unit_tests.py - all pyflow unit tests 

* pyflow_basic_feature_runner.py - runs a number of pyflow operations for
  local or sge modes

* demos - Running through the various small demo scripts and making sure they
  complete without error is used to round out the full test process. Most demo
  scripts are linux-only at this point.

