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


#
# very simple task scripts called by the demo:
#
testJobDir = os.path.join(scriptDir, "testtasks")

sleepjob = os.path.join(testJobDir, "sleeper.bash")  # sleeps



# all pyflow workflows are written into classes derived from
# pyflow.WorkflowRunner:
#
class MutexWorkflow(WorkflowRunner) :

    # a workflow is defined by overloading the
    # WorkflowRunner.workflow() method:
    #
    def workflow(self) :

        # create an array of mutex restricted tasks which can only run
        # once at a time:
        for i in range(8) :
            self.addTask("mutex_task_" + str(i), sleepjob + " 1", mutex="test")

        # and add an array of 'normal' tasks for comparison:
        for i in range(16) :
            self.addTask("normal_task_" + str(i), sleepjob + " 1")




def main() :
    # Instantiate the workflow
    wflow = MutexWorkflow()

    # Run the worklow:
    retval = wflow.run(mode="local", nCores=6)

    sys.exit(retval)



if __name__ == "__main__" :
    main()
