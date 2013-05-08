#!/usr/bin/env python
#
# Copyright (c) 2012-2013 Illumina, Inc.
#
# This software is provided under the terms and conditions of the
# Illumina Open Source Software License 1.
#
# You should have received a copy of the Illumina Open Source
# Software License 1 along with this program. If not, see
# <https://github.com/downloads/sequencing/licenses/>.
#


#
# demonstrate/test addTask() env option
#

import os.path
import sys

# add module path by hand
#
scriptDir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir + "/../../src")

from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class EnvWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :
        # run a task with the parent env:
        #
        home = os.environ["HOME"]
        self.addTask("testDefEnv", "[ $HOME == '%s' ]; exit $?" % (home))

        # create a minimal test environment
        #
        new_path = "/bin"
        min_env = { "PATH" : new_path }
        self.addTask("testMinEnv", "[ $PATH == '%s' ]; exit $?" % (new_path), env=min_env)

        # augment parent env with additional settings:
        #
        augmented_env = os.environ.copy()
        augmented_env["FOO"] = "BAZ"
        self.addTask("testAugmentedEnv", "[ $FOO == 'BAZ' ]; exit $?", env=augmented_env)

        # test funny characters that have shown to cause trouble on some sge installations
        funky_env = {}
        funky_env["PATH"] = "/bin"
        funky_env["_"] = "| %s %F \n"
        # in this case we just want the job to run at all:
        self.addTask("testFunkyEnv", "echo 'foo'; exit $?", env=funky_env)

        assert("FOO" not in os.environ)



# Instantiate the workflow
#
wflow = EnvWorkflow()

# Run the worklow:
#
retval = wflow.run(mode="local")

sys.exit(retval)

