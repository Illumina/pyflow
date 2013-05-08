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

