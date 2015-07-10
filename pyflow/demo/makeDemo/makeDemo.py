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

# add module path by hand
#
scriptDir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir + "/../../src")

from pyflow import WorkflowRunner


# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class MakeWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # This command 'configures' a makefile
        #
        self.addTask("task1", "cd %s; cp .hidden Makefile" % scriptDir)

        # Sometimes you get to deal with make. The task below
        # demonstates a make command which starts when the above task
        # completes. Make tasks are specified as directories which
        # contain a makefile. This task points to the direcotry of
        # this demo script, which contains has a Makefile at the
        # completion of task1.
        # pyflow will switch the task command between make and qmake
        # depending on run type.
        #
        self.addTask("make_task", scriptDir, isCommandMakePath=True, nCores=2, dependencies="task1")

        # This command 'unconfigures' the makefile
        #
        self.addTask("task2", "rm -f %s/Makefile" % scriptDir, dependencies="make_task")


# Instantiate the workflow
#
# parameters are passed into the workflow via its constructor:
#
wflow = MakeWorkflow()

# Run the worklow:
#
retval = wflow.run(mode="local", nCores=8)

sys.exit(retval)

