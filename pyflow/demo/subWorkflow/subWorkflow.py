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


