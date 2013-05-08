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


import os.path
import sys

# add module paths
#
filePath = os.path.dirname(__file__)
pyflowPath = os.path.abspath(os.path.join(filePath, "../../src"))
sys.path.append(pyflowPath)

from pyflow import WorkflowRunner
from getDemoRunOptions import getDemoRunOptions


#
# very simple task scripts called by the demo:
#
testJobDir = os.path.join(filePath, "testtasks")

sleepjob = os.path.join(testJobDir, "sleeper.bash")  # sleeps
yelljob = os.path.join(testJobDir, "yeller.bash")  # generates some i/o
runjob = os.path.join(testJobDir, "runner.bash")  # runs at 100% cpu



# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class TestWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # A simple command task with no dependencies, labeled 'task1'.
        #
        cmd = "%s 1" % (yelljob)
        self.addTask("task1", cmd)

        # Another task which runs the same command, this time the
        # command is provided as an argument list. An argument list
        # can be useful when a command has many arguments or
        # complicated quoting issues:
        #
        cmd = [yelljob, "1"]
        self.addTask("task2", cmd)

        # This task will always run on the local machine, no matter
        # what the run mode is. The force local option is useful for
        # non-cpu intensive jobs which are taking care of minor
        # workflow overhead (moving/touching files, etc)
        #
        self.addTask("task3a", sleepjob + " 10", isForceLocal=True)


# get runtime options
#
runOptions = getDemoRunOptions()

# Instantiate the workflow
#
wflow = TestWorkflow()

# Run the worklow with runtime options specified on the command-line:
#
retval = wflow.run(mode=runOptions.mode,
                 nCores=runOptions.jobs,
                 memMb=runOptions.memMb,
                 mailTo=runOptions.mailTo,
                 isContinue=(runOptions.isResume and "Auto" or False),
                 isForceContinue=True,
                 schedulerArgList=runOptions.schedulerArgList)

sys.exit(retval)

