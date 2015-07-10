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
scriptDir=os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir+"/../../src")

from pyflow import WorkflowRunner


#
# very simple task scripts called by the demo:
#
testJobDir=os.path.join(scriptDir,"testtasks")

sleepjob=os.path.join(testJobDir,"sleeper.bash")  # sleeps
yelljob=os.path.join(testJobDir,"yeller.bash")    # generates some i/o
runjob=os.path.join(testJobDir,"runner.bash")     # runs at 100% cpu



# all pyflow workflows are written into classes derived from pyflow.WorkflowRunner:
#
# this workflow  is a simple example of a workflow we can either run directly,
# or run as a task within another workflow:
#
class SubWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :
        # this workflow executes a simple dependency diamond:
        self.addTask("task1",yelljob+" 1")
        self.addTask("task2a",yelljob+" 1",dependencies="task1")
        self.addTask("task2b",yelljob+" 1",dependencies="task1")
        self.addTask("task3",yelljob+" 1",dependencies=("task2a","task2b"))


#
# This workflow will use SubWorkflow as a task:
#
class SimpleWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # it's fine to repeat task names in two workflows, even if you're sub-tasking one from the other
        self.addTask("task1",yelljob+" 1")
        self.addTask("task2",runjob+" 3")

        # instantiate a new workflow and run it as soon as task1 and task2 complete
        wflow=SubWorkflow()
        self.addWorkflowTask("subwf_task3",wflow,dependencies=("task1","task2"))

        # this job will not run until the workflow-task completes. This means that all of the
        # tasks that SubWorkflow launches will need to complete successfully beforehand:
        #
        self.addTask("task4",sleepjob+" 1",dependencies="subwf_task3")


# Instantiate our workflow
#
wflow = SimpleWorkflow()

# Run the worklow:
#
retval=wflow.run(mode="local",nCores=8)


# If we want to run the SubWorkflow as a regular workflow, that can be done as well:
#

#wflow2 = SubWorkflow()
#retval2=wflow2.run()


sys.exit(retval)


