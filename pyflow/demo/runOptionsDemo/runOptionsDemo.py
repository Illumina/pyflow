#!/usr/bin/env python
#
# pyFlow - a lightweight parallel task engine
#
# Copyright (c) 2012-2015 Illumina, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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

