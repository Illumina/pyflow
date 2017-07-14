#!/usr/bin/env python
#
# pyFlow - a lightweight parallel task engine
#
# Copyright (c) 2012-2017 Illumina, Inc.
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
testJobDir=os.path.join(scriptDir)

workerJob=os.path.join(testJobDir,"testWorker.py")


class DeepCopyProtector(object) :
    """
    Any data attached to this object will remain aliased through a deepcopy operation

    Overloading __copy__ is provided here just to ensure that deep/shallow copy semantics are identical.
    """
    def __copy__(self) :
        return self

    def __deepcopy__(self, dict) :
        return self


def lockMethod(f):
    """
    method decorator acquires/releases object's lock
    """

    def wrapped(self, *args, **kw):
        import threading

        if not hasattr(self,"lock") :
            self.lock = threading.RLock()

        self.lock.acquire()
        try:
            return f(self, *args, **kw)
        finally:
            self.lock.release()
    return wrapped



class SyncronizedAccumulator(object) :

    def __init__(self) :
        self._values = []

    @lockMethod
    def addOrderedValue(self, index, value):
        while index+1 > len(self._values) :
            self._values.append(None)
        self._values[index] = value

    @lockMethod
    def totalValue(self):
        count = 0
        sum = 0
        for v in self._values :
            if v is None : continue
            count += 1
            sum += v
        return (count, sum)

    @lockMethod
    def totalContinuousValue(self):
        count = 0
        sum = 0
        for v in self._values :
            if v is None : break
            count += 1
            sum += v
        return (count, sum)

    @lockMethod
    def continuousTasksRequiredToReachTarget(self, targetVal):
        count = 0
        sum = 0
        if sum >= targetVal : return count
        for v in self._values :
            if v is None : break
            count += 1
            sum += v
            if sum >= targetVal : return count
        return None



class SumWorkflow(WorkflowRunner) :

    def __init__(self, taskIndex, inputFile, totalWorkCompleted) :
        self.taskIndex=taskIndex
        self.inputFile=inputFile
        self.nocopy = DeepCopyProtector()
        self.nocopy.totalWorkCompleted = totalWorkCompleted

    def workflow(self) :
        import os
        infp = open(self.inputFile, "rb")
        value = int(infp.read().strip())
        infp.close()
        self.flowLog("File: %s Value: %i" % (self.inputFile, value))
        os.remove(self.inputFile)
        self.nocopy.totalWorkCompleted.addOrderedValue(self.taskIndex, value)


class LaunchUntilWorkflow(WorkflowRunner) :

    def __init__(self):
        self.totalContinuousWorkTarget = 100

    def workflow(self):

        taskByIndex = []
        allTasks = set()
        completedTasks = set()
        totalWorkCompleted = SyncronizedAccumulator()

        def launchNextTask() :
            taskIndex = len(allTasks)
            workerTaskLabel = "workerTask_%05i" % (taskIndex)
            workerTaskFile = "outputFile_%05i" % (taskIndex)
            workerTaskCmd=[sys.executable, workerJob, workerTaskFile]
            self.addTask(workerTaskLabel, workerTaskCmd)

            allTasks.add(workerTaskLabel)
            taskByIndex.append(workerTaskLabel)

            sumTaskLabel="sumTask_%05i" % (taskIndex)
            self.addWorkflowTask(sumTaskLabel, SumWorkflow(taskIndex, workerTaskFile, totalWorkCompleted), dependencies=workerTaskLabel)

        def updateCompletedTasks() :
            for task in allTasks :
                if task in completedTasks : continue
                (isDone, isError) = self.isTaskDone(task)
                if not isDone : continue
                if isError :
                    raise Exception("Task %s failed." % (task))
                completedTasks.add(task)

        def stopRunningExtraTasks(nTargetTasks) :
            for task in taskByIndex[nTargetTasks:] :
                self.cancelTaskTree(task)

        maxTaskCount = self.getNCores()
        assert(maxTaskCount > 0)

        import time
        while True :
            count, completedWork = totalWorkCompleted.totalValue()
            self.flowLog("TotalWorkCompleted: %i (%i)" % (completedWork, count))
            count, completedContinuousWork = totalWorkCompleted.totalContinuousValue()
            self.flowLog("TotalContinuousWorkCompleted: %i (%i)" % (completedContinuousWork, count))
            nTargetTasks = totalWorkCompleted.continuousTasksRequiredToReachTarget(self.totalContinuousWorkTarget)
            if nTargetTasks is not None :
                self.flowLog("Work target completed in first %i tasks" % (nTargetTasks))
                stopRunningExtraTasks(nTargetTasks)
                break

            updateCompletedTasks()
            runningTaskCount = len(allTasks)-len(completedTasks)
            self.flowLog("Completed/Running tasks: %i %i" % (len(completedTasks), runningTaskCount))
            assert(runningTaskCount >= 0)

            # launch new tasks until it is clear the total threshold will be met
            if completedWork < self.totalContinuousWorkTarget :
                numberOfTasksToLaunch = max(maxTaskCount-runningTaskCount,0)
                for _ in range(numberOfTasksToLaunch) : launchNextTask()

            time.sleep(5)



wflow = LaunchUntilWorkflow()

# Run the worklow:
#
retval=wflow.run(mode="local",nCores=8)

sys.exit(retval)
