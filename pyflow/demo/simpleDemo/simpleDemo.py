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



# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class SimpleWorkflow(WorkflowRunner) :

    # WorkflowRunner objects can create regular constructors to hold
    # run parameters or other state information:
    #
    def __init__(self,params) :
        self.params=params


    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # A simple command task with no dependencies, labeled 'task1'.
        #
        cmd="%s 1" % (yelljob)
        self.addTask("task1",cmd)

        # Another task which runs the same command, this time the
        # command is provided as an argument list. An argument list
        # can be useful when a command has many arguments or
        # complicated quoting issues:
        #
        cmd=[yelljob,"1"]
        self.addTask("task2",cmd)

        # This task will always run on the local machine, no matter
        # what the run mode is. The force local option is useful for
        # non-cpu intensive jobs which are taking care of minor
        # workflow overhead (moving/touching files, etc)
        #
        self.addTask("task3a",sleepjob+" 10",isForceLocal=True)

        # This job is requesting 2 threads:
        #
        self.addTask("task3b",runjob+" 10",nCores=2)

        # This job is requesting 2 threads and 3 gigs of ram:
        #
        self.addTask("task3c",runjob+" 10",nCores=2,memMb=3*1024)


        # addTask and addWorkflowTask always return their task labels
        # as a simple convenience. taskName is set to "task4" now.
        #
        taskName=self.addTask("task4",sleepjob+" 1")

        # an example task dependency:
        #
        # pyflow stores dependencies in set() objects, but you can
        # provide a list,tuple,set or single string as the argument to
        # dependencies:
        #
        # all the task5* tasks below specify "task4" as their
        # dependency:
        #
        self.addTask("task5a",yelljob+" 2",dependencies=taskName)
        self.addTask("task5b",yelljob+" 2",dependencies="task4")
        self.addTask("task5c",yelljob+" 2",dependencies=["task4"])
        self.addTask("task5d",yelljob+" 2",dependencies=[taskName])

        # this time we launch a number of sleep tasks based on the
        # workflow parameters:
        #
        # we store all tasks in sleepTasks -- which we use to make
        # other tasks wait for this entire set of jobs to complete:
        #
        sleepTasks=set()
        for i in range(self.params["numSleepTasks"]) :
            taskName="sleep_task%i" % (i)
            sleepTasks.add(taskName)
            self.addTask(taskName,sleepjob+" 1",dependencies="task5a")

            ## note the three lines above could have been written in a
            ## more compact single-line format:
            ##
            #sleepTasks.add(self.addTask("sleep_task%i" % (i),sleepjob+" 1",dependencies="task5a"))

        # this job cannot start until all tasks in the above loop complete:
        self.addTask("task6",runjob+" 2",nCores=3,dependencies=sleepTasks)

        # This task is supposed to fail, uncomment to see error reporting:
        #
        #self.addTask("task7",sleepjob)

        # Note that no command is provided to this task. It will not
        # be distributed locally or to sge, but does provide a
        # convenient label for a set of tasks that other processes
        # depend on. There is no special "checkpoint-task" type in
        # pyflow -- but any task can function like one per this
        # example:
        #
        self.addTask("checkpoint_task",dependencies=["task1","task6","task5a"])

        # The final task depends on the above checkpoint:
        #
        self.addTask("task8",yelljob+" 2",dependencies="checkpoint_task")



# simulated workflow parameters
#
myRunParams={"numSleepTasks" : 15}


# Instantiate the workflow
#
# parameters are passed into the workflow via its constructor:
#
wflow = SimpleWorkflow(myRunParams)

# Run the worklow:
#
retval=wflow.run(mode="local",nCores=8)

sys.exit(retval)

